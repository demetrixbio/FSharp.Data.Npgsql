namespace FSharp.Data.Npgsql

open System
open System.Data
open System.Data.Common
open System.Collections.Concurrent
open System.ComponentModel
open Npgsql
open NpgsqlTypes
open FSharp.Control.Tasks.NonAffine
open System.Linq.Expressions

#nowarn "0025"

[<EditorBrowsable(EditorBrowsableState.Never)>]
type Utils () =
    static let makeOptionValue =
        let cache = ConcurrentDictionary<Type, obj -> obj> ()
        let factory = Func<_, _>(fun (typeParam: Type) ->
            // PreComputeUnionConstructor returns obj[] -> obj, which would require an extra array allocation when creating option values
            // Since we're only interested in Some (one argument) here, the array can be left out
            let param = Expression.Parameter typeof<obj>
            let expr =
                Expression.Lambda<Func<obj, obj>> (
                    Expression.New (
                        typedefof<_ option>.MakeGenericType(typeParam).GetConstructor [| typeParam |],
                        Expression.Convert (param, typeParam)
                    ),
                    param)

            let someCtor = expr.Compile ()

            fun (v: obj) -> if Convert.IsDBNull v then null else someCtor.Invoke v)

        Func<_, _>(fun typeParam -> cache.GetOrAdd (typeParam, factory))

    static let getColumnMapping =
        let cache = ConcurrentDictionary<Type, obj -> obj> ()
        fun (x: DataColumn) -> if x.AllowDBNull then cache.GetOrAdd (x.DataType, makeOptionValue) else id

    static let getRowToTupleReader =
        let cache = ConcurrentDictionary<int, Func<DbDataReader, obj>> ()
        let rec constituentTuple (t: Type) (columns: (int * DataColumn)[]) param startIndex: Expression =
            let genericArgs = t.GetGenericArguments ()
            Expression.New (
                t.GetConstructor genericArgs,
                [ 
                    for paramIndex in 0 .. genericArgs.Length - 1 do
                        let genericArg = genericArgs.[paramIndex]

                        if paramIndex = 7 then
                            constituentTuple genericArg columns param (startIndex + 7)
                        else
                            let i, c = columns.[paramIndex + startIndex]
                            let v = Expression.Call (param, typeof<DbDataReader>.GetMethod("GetFieldValue").MakeGenericMethod c.DataType, Expression.Constant i)

                            if c.AllowDBNull then
                                Expression.Condition (
                                    Expression.Call (param, typeof<DbDataReader>.GetMethod "IsDBNull", Expression.Constant i),
                                    Expression.Constant (null, typedefof<_ option>.MakeGenericType c.DataType),
                                    Expression.New (typedefof<_ option>.MakeGenericType(c.DataType).GetConstructor [| c.DataType |], v))
                            else
                                v
                ]) :> Expression

        fun resultSet sortColumns ->
            let mutable func = Unchecked.defaultof<_>
            if cache.TryGetValue (resultSet.ExpectedColumns.GetHashCode (), &func) then
                func
            else
                let func =
                    let param = Expression.Parameter typeof<DbDataReader>
                    let expr =
                        Expression.Lambda<Func<DbDataReader, obj>> (
                            constituentTuple resultSet.ErasedRowType (resultSet.ExpectedColumns |> Array.indexed |> (if sortColumns then Array.sortBy (fun (_, c) -> c.ColumnName) else id)) param 0,
                            param)

                    expr.Compile ()

                cache.[resultSet.ExpectedColumns.GetHashCode ()] <- func
                func

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

    static member val GetStatementIndex =
        let mi = typeof<NpgsqlDataReader>.GetProperty("StatementIndex", Reflection.BindingFlags.Instance ||| Reflection.BindingFlags.NonPublic).GetMethod
        Delegate.CreateDelegate (typeof<Func<NpgsqlDataReader, int>>, mi) :?> Func<NpgsqlDataReader, int>

    static member ToSqlParam (name, dbType: NpgsqlTypes.NpgsqlDbType, size, scale, precision) = 
        NpgsqlParameter (name, dbType, size, Scale = scale, Precision = precision)

    static member CloneDataColumn (column: DataColumn) =
        let c = new DataColumn (column.ColumnName, column.DataType)
        c.AutoIncrement <- column.AutoIncrement
        c.AllowDBNull <- column.AllowDBNull
        c.ReadOnly <- column.ReadOnly
        c.MaxLength <- column.MaxLength
        c.DateTimeMode <- column.DateTimeMode

        for p in column.ExtendedProperties do
            let p = p :?> System.Collections.DictionaryEntry
            c.ExtendedProperties.Add (p.Key, p.Value)

        c

    static member CreateResultSetDefinition (columns: DataColumn[], resultType) =
        let t =
            match columns with
            | [||] -> typeof<int>
            | [| c |] -> if c.AllowDBNull then typedefof<_ option>.MakeGenericType c.DataType else c.DataType
            | _ ->
                match resultType with
                | ResultType.Records -> Utils.ToTupleType (columns |> Array.sortBy (fun c -> c.ColumnName))
                | ResultType.Tuples -> Utils.ToTupleType columns
                | _ -> null

        { ErasedRowType = t; ExpectedColumns = columns }

    static member GetType typeName = 
        if isNull typeName then
            null
        else
            let t = Type.GetType typeName

            if isNull t then
                let t = Type.GetType (typeName + ", FSharp.Core")

                if isNull t then
                    let t = Type.GetType (typeName + ", Npgsql")

                    if isNull t then
                        Type.GetType (typeName + ", NetTopologySuite", true)
                    else
                        t
                else
                    t
            else
                t

    static member ToDataColumn (stringValues: string, isEnum, autoIncrement, allowDbNull, readonly, maxLength, partOfPk: bool, nullable: bool) =
        let [| columnName; typeName; pgTypeName; baseSchemaName; baseTableName |] = stringValues.Split '|'
        let x = new DataColumn (columnName, Utils.GetType typeName)

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

    static member ToTupleType (columns: DataColumn[]) =
        Reflection.FSharpType.MakeTupleType (columns |> Array.map (fun c -> if c.AllowDBNull then typedefof<_ option>.MakeGenericType c.DataType else c.DataType))

    static member ToDataColumnSlim (stringValues: string) =
        let [| columnName; typeName; nullable |] = stringValues.Split '|'
        new DataColumn (columnName, Utils.GetType typeName, AllowDBNull = (nullable = "1"))

    static member NoBoxingMapRowValues<'TItem> (cursor: DbDataReader, resultType, resultSet) = Unsafe.uply {
        let results = ResizeArray<'TItem> ()
        let rowReader = getRowToTupleReader resultSet (resultType = ResultType.Records)
        
        let! go = cursor.ReadAsync ()
        let mutable go = go

        while go do
            rowReader.Invoke cursor
            |> unbox
            |> results.Add

            let! cont = cursor.ReadAsync ()
            go <- cont

        return results }

    static member NoBoxingMapRowValuesLazy<'TItem> (cursor: DbDataReader, resultType, resultSet) =
        seq {
            let rowReader = getRowToTupleReader resultSet (resultType = ResultType.Records)

            while cursor.Read () do
                rowReader.Invoke cursor |> unbox<'TItem>
        }

    static member MapRowValues<'TItem> (cursor: DbDataReader, resultType, resultSet: ResultSetDefinition) =
        if resultSet.ExpectedColumns.Length > 1 then
            Utils.NoBoxingMapRowValues<'TItem> (cursor, resultType, resultSet)
        else Unsafe.uply {
            let columnMapping = getColumnMapping resultSet.ExpectedColumns.[0]
            let results = ResizeArray<'TItem> ()
            
            let! go = cursor.ReadAsync ()
            let mutable go = go

            while go do
                cursor.GetValue 0
                |> columnMapping
                |> unbox
                |> results.Add

                let! cont = cursor.ReadAsync ()
                go <- cont

            return results }

    static member MapRowValuesLazy<'TItem> (cursor: DbDataReader, resultSet) =
        seq {
            let columnMapping = getColumnMapping resultSet.ExpectedColumns.[0]

            while cursor.Read () do
                cursor.GetValue 0
                |> columnMapping
                |> unbox<'TItem>
        }
    
    static member OptionToObj<'a> (value: obj) =
        match value :?> 'a option with
        | Some x -> box x
        | _ -> box DBNull.Value

    static member GetNullableValueFromDataRow<'a> (row: DataRow, name: string) =
        if row.IsNull name then None else Some (row.[name] :?> 'a)

    static member SetNullableValueInDataRow<'a> (row: DataRow, name: string, value: obj) =
        row.[name] <- Utils.OptionToObj<'a> value

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

    static member BinaryImport (table: DataTable<DataRow>, connection: NpgsqlConnection, ignoreIdentityColumns) =
        let columnsToInsert =
            Seq.cast<DataColumn> table.Columns
            |> Seq.indexed
            |> Seq.filter (fun (_, x) -> not ignoreIdentityColumns || not x.AutoIncrement)
            |> Seq.toArray

        let copyFromCommand = 
            columnsToInsert
            |> Array.map (fun (_, x) -> x.ColumnName)
            |> String.concat ", "
            |> sprintf "COPY %s (%s) FROM STDIN (FORMAT BINARY)" table.TableName

        use writer = connection.BeginBinaryImport copyFromCommand

        for row in table.Rows do
            writer.StartRow ()
            for i, _ in columnsToInsert do
                writer.Write row.[i]

        writer.Complete ()

    #if DEBUG
    static member private Udp =
        let c = new System.Net.Sockets.UdpClient ()
        c.Connect ("localhost", 2180)
        c
    static member Log what =
        let b = System.Text.Encoding.UTF8.GetBytes (sprintf "%s - %s" (DateTime.Now.TimeOfDay.ToString "hh':'mm':'ss'.'ff") what)
        Utils.Udp.Send (b, b.Length) |> ignore
    #endif
