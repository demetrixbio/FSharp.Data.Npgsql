namespace FSharp.Data.Npgsql

open System
open System.Data
open System.Data.Common
open System.Runtime.CompilerServices
open System.Collections.Concurrent
open System.ComponentModel
open Npgsql
open NpgsqlTypes

#nowarn "0025"

[<Extension>]
[<AbstractClass; Sealed>]
[<EditorBrowsable(EditorBrowsableState.Never)>]
type Utils private() =
    static let optionCtorCache =
        ConcurrentDictionary<Type, obj -> obj> ()

    static let statementIndexGetter =
        typeof<NpgsqlDataReader>.GetProperty("StatementIndex", Reflection.BindingFlags.Instance ||| Reflection.BindingFlags.NonPublic).GetMethod

    static member ResizeArrayToList ra =
        let rec inner (ra: ResizeArray<'a>, index, acc) = 
            if index = 0 then
                acc
            else
                inner (ra, index - 1, ra.[index - 1] :: acc)

        inner (ra, ra.Count, [])

    static member ResizeArrayToOption (source: ResizeArray<'a>) =  
        match source.Count with
        | 0 -> None
        | 1 -> Some source.[0]
        | _ -> invalidOp "The output sequence contains more than one element."

    [<Extension>]
    static member GetStatementIndex(cursor: DbDataReader) =
        statementIndexGetter.Invoke(cursor, null) :?> int

    static member ToSqlParam (name, dbType: NpgsqlTypes.NpgsqlDbType, size, scale, precision) = 
        NpgsqlParameter (name, dbType, size, Scale = scale, Precision = precision)

    static member ToDataColumn (stringValues: string, isEnum, autoIncrement, allowDbNull, readonly, maxLength, partOfPk: bool, nullable: bool) =
        let [| columnName; typeName; pgTypeName; baseSchemaName; baseTableName |] = stringValues.Split '|'
        let x = new DataColumn (columnName, Type.GetType (typeName, throwOnError = true))

        x.AutoIncrement <- autoIncrement
        x.AllowDBNull <- allowDbNull
        x.ReadOnly <- readonly
        x.MaxLength <- maxLength
        
        if pgTypeName = "timestamptz" then
            //https://github.com/npgsql/npgsql/issues/1076#issuecomment-355400785
            x.DateTimeMode <- DataSetDateTime.Local
            //https://www.npgsql.org/doc/types/datetime.html#detailed-behavior-sending-values-to-the-database
            x.ExtendedProperties.Add (SchemaTableColumn.ProviderType, NpgsqlDbType.TimestampTz)
        elif pgTypeName = "timestamp" then
            //https://www.npgsql.org/doc/types/datetime.html#detailed-behavior-sending-values-to-the-database
            x.ExtendedProperties.Add (SchemaTableColumn.ProviderType, NpgsqlDbType.Timestamp)
        elif isEnum then
            // value is an enum and should be sent to npgsql as unknown (auto conversion from string to appropriate enum type)
            x.ExtendedProperties.Add (SchemaTableColumn.ProviderType, NpgsqlDbType.Unknown)
        elif pgTypeName = "json" then
            x.ExtendedProperties.Add (SchemaTableColumn.ProviderType, NpgsqlDbType.Json)
        elif pgTypeName = "jsonb" then
            x.ExtendedProperties.Add (SchemaTableColumn.ProviderType, NpgsqlDbType.Jsonb)
        
        x.ExtendedProperties.Add (SchemaTableColumn.IsKey, partOfPk)
        x.ExtendedProperties.Add (SchemaTableColumn.AllowDBNull, nullable)
        x.ExtendedProperties.Add (SchemaTableColumn.BaseSchemaName, baseSchemaName)
        x.ExtendedProperties.Add (SchemaTableColumn.BaseTableName, baseTableName)
        x

    static member ToDataColumnSlim (stringValues: string, nullable: bool) =
        let [| columnName; typeName |] = stringValues.Split '|'
        let x = new DataColumn (columnName, Type.GetType (typeName, true))
        x.ExtendedProperties.Add (SchemaTableColumn.AllowDBNull, nullable)
        x

    static member private MakeOptionValue (typeParam: Type) v =
        match optionCtorCache.TryGetValue typeParam with
        | true, ctor ->
            ctor v
        | _ ->
            let cases =  typedefof<_ option>.MakeGenericType typeParam |> Reflection.FSharpType.GetUnionCases |> Array.partition (fun x -> x.Name = "Some")
            let someCtor = fst cases |> Array.exactlyOne |> Reflection.FSharpValue.PreComputeUnionConstructor
            let noneInfo = snd cases |> Array.exactlyOne
            let noneValue = Reflection.FSharpValue.MakeUnion (noneInfo, [||])

            optionCtorCache.GetOrAdd (typeParam, fun v -> if Convert.IsDBNull v then noneValue else someCtor [| v |]) v
    
    [<Extension>]
    static member MapRowValues<'TItem> (cursor: DbDataReader, resultType, resultSet, isTypeReuseEnabled) =
        let rowMapping =
            if resultSet.ExpectedColumns.Length = 1 then
                Array.item 0
            elif resultType = ResultType.Tuples then
                Reflection.FSharpValue.PreComputeTupleConstructor (Type.GetType (resultSet.SeqItemTypeName, true))
            else
                box
        
        let results = ResizeArray<'TItem> ()

        let values = Array.zeroCreate cursor.FieldCount

        // If type type reuse of records is enabled, columns need to be sorted alphabetically, because records are erased to arrays and thus the insert order
        // of elements matters
        let columns =
            if isTypeReuseEnabled && resultType = ResultType.Records then
                resultSet.ExpectedColumns |> Array.indexed |> Array.sortBy (fun (_, col) -> col.ColumnName)
            else
                resultSet.ExpectedColumns |> Array.indexed

        while cursor.Read () do
            cursor.GetValues values |> ignore

            columns
            |> Array.map (fun (i, column) ->
                let obj = values.[i]

                if column.ExtendedProperties.[SchemaTableColumn.AllowDBNull] :?> bool then
                    Utils.MakeOptionValue column.DataType obj
                else
                    obj)
            |> rowMapping
            |> unbox<'TItem>
            |> results.Add

        results
    
    static member DbNull = box DBNull.Value

    static member OptionToObj<'a> (value: obj) =
        match value :?> 'a option with
        | Some x -> box x
        | _ -> box DBNull.Value

    static member GetNullableValueFromDataRow<'a> (row: DataRow, name: string) =
        if row.IsNull name then None else Some (row.[name] :?> 'a)

    static member SetNullableValueInDataRow<'a> (row: DataRow, name: string, value: obj) =
        row.[name] <- Utils.OptionToObj<'a> value

    static member GetMapperWithNullsToOptions(nullsToOptions, mapper: obj[] -> obj) = 
        fun values -> 
            nullsToOptions values
            mapper values

    static member UpdateDataTable(table: DataTable<DataRow>, connection, transaction, batchSize, continueUpdateOnError, conflictOption, batchTimeout) = 

        if batchSize <= 0 then invalidArg "batchSize" "Batch size has to be larger than 0."
        if batchTimeout <= 0 then invalidArg "batchTimeout" "Batch timeout has to be larger than 0."

        use selectCommand = table.SelectCommand.Clone()

        selectCommand.Connection <- connection
        if transaction <> null then selectCommand.Transaction <- transaction

        use dataAdapter = new BatchDataAdapter(selectCommand, batchTimeout, UpdateBatchSize = batchSize, ContinueUpdateOnError = continueUpdateOnError)
        use commandBuilder = new CommandBuilder(table, DataAdapter = dataAdapter, ConflictOption = conflictOption)

        use __ = dataAdapter.RowUpdating.Subscribe(fun args ->

            if  args.Errors = null 
                && args.StatementType = Data.StatementType.Insert 
                && dataAdapter.UpdateBatchSize = 1
            then 
                let columnsToRefresh = ResizeArray()
                for c in table.Columns do
                    if c.AutoIncrement  
                        || (c.AllowDBNull && args.Row.IsNull c.Ordinal)
                    then 
                        columnsToRefresh.Add( commandBuilder.QuoteIdentifier c.ColumnName)

                if columnsToRefresh.Count > 0
                then                        
                    let returningClause = columnsToRefresh |> String.concat "," |> sprintf " RETURNING %s"
                    let cmd = args.Command
                    cmd.CommandText <- cmd.CommandText + returningClause
                    cmd.UpdatedRowSource <- UpdateRowSource.FirstReturnedRecord
        )

        dataAdapter.Update(table)   

    static member BinaryImport(table: DataTable<DataRow>, connection: NpgsqlConnection) = 
        let copyFromCommand = 
            [ for c in table.Columns -> c.ColumnName ]
            |> String.concat ", "
            |> sprintf "COPY %s (%s) FROM STDIN (FORMAT BINARY)" table.TableName

        use writer = connection.BeginBinaryImport(copyFromCommand)

        for row in table.Rows do
            writer.WriteRow(row.ItemArray)

        writer.Complete()
