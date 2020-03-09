namespace FSharp.Data.Npgsql

open System
open System.Data
open System.Data.Common
open System.Runtime.CompilerServices
open System.ComponentModel
open Npgsql

[<Extension>]
[<AbstractClass; Sealed>]
[<EditorBrowsable(EditorBrowsableState.Never)>]
type Utils private() =
    static member private StatementIndexGetter =
        typeof<NpgsqlDataReader>.GetProperty("StatementIndex", Reflection.BindingFlags.Instance ||| Reflection.BindingFlags.NonPublic).GetMethod
    
    [<Extension>]
    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member GetStatementIndex(cursor: DbDataReader) =
        Utils.StatementIndexGetter.Invoke(cursor, null) :?> int

    static member private CreateOptionType typeParam =
        typeof<unit option>.GetGenericTypeDefinition().MakeGenericType([| typeParam |])
    
    static member GetOptionCaseInfos optionType =
        let cases = FSharp.Reflection.FSharpType.GetUnionCases(optionType)
        let cases = cases |> Array.partition (fun x -> x.Name = "Some")
        let someCase = fst cases |> Array.exactlyOne
        let noneCase = snd cases |> Array.exactlyOne
        someCase, noneCase

    static member private MakeOptionValue typeParam v isSome =
        let optionType = Utils.CreateOptionType typeParam
        let someCase, noneCase = Utils.GetOptionCaseInfos optionType
        let relevantCase, args =
            match isSome with
            | true -> someCase, [| v |]
            | false -> noneCase, [| |]
        FSharp.Reflection.FSharpValue.MakeUnion(relevantCase, args)
    
    [<Extension>]
    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member MapRowValues<'TItem>(cursor: DbDataReader, resultType : ResultType, resultSet : ResultSetDefinition, isTypeReuseEnabled) =
        let rowMapping =
            if resultSet.ExpectedColumns.Length = 1 then
                Array.item 0
            elif resultType = ResultType.Tuples then
                let clrTypeName = resultSet.SeqItemTypeName
                Reflection.FSharpValue.PreComputeTupleConstructor(Type.GetType(clrTypeName, throwOnError = true))
            else
                box
        
        seq {
            let values = Array.zeroCreate cursor.FieldCount

            // If type type reuse of records is enabled, columns need to be sorted alphabetically, because records are erased to arrays and thus the insert order
            // of elements matters
            let columns =
                if isTypeReuseEnabled && resultType = ResultType.Records then
                    resultSet.ExpectedColumns |> Array.indexed |> Array.sortBy (fun (_, col) -> col.ColumnName)
                else
                    resultSet.ExpectedColumns |> Array.indexed

            while cursor.Read() do
                cursor.GetValues(values) |> ignore

                columns
                |> Array.map (fun (i, column) ->
                    let obj = values.[i]
                    let isNullable = column.ExtendedProperties.[SchemaTableColumn.AllowDBNull] |> unbox<bool>
                    if isNullable then
                        let dataTypeName = column.ExtendedProperties.["ClrType.PartiallyQualifiedName"] |> unbox<string>
                        let dataType = Type.GetType(dataTypeName, throwOnError = true)
                        let isSome = Convert.IsDBNull(obj) |> not
                        Utils.MakeOptionValue dataType obj isSome
                    else
                        obj)
                |> rowMapping
                |> unbox<'TItem>
        }
    
    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member DbNull = box DBNull.Value

    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member GetMapperWithNullsToOptions(nullsToOptions, mapper: obj[] -> obj) = 
        fun values -> 
            nullsToOptions values
            mapper values

    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member SetRef<'t>(r : byref<'t>, arr: (string * obj)[], i) = r <- arr.[i] |> snd |> unbox

    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member UpdateDataTable(table: DataTable<DataRow>, connection, transaction, batchSize, continueUpdateOnError, conflictOption, batchTimeout) = 

        if batchSize <= 0 then invalidArg "batchSize" "Batch size has to be larger than 0."
        if batchTimeout <= 0 then invalidArg "batchTimeout" "Batch timeout has to be larger than 0."

        use selectCommand = table.SelectCommand.Clone()

        selectCommand.Connection <- connection
        if transaction <> null then selectCommand.Transaction <- transaction

        for column in table.Columns do column.ExtendedProperties.Remove("ClrType.PartiallyQualifiedName")
        
        use dataAdapter = new BatchDataAdapter(selectCommand, batchTimeout, UpdateBatchSize = int batchSize, ContinueUpdateOnError = continueUpdateOnError)
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

    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member BinaryImport(table: DataTable<DataRow>, connection: NpgsqlConnection) = 
        let copyFromCommand = 
            [ for c in table.Columns -> c.ColumnName ]
            |> String.concat ", "
            |> sprintf "COPY %s (%s) FROM STDIN (FORMAT BINARY)" table.TableName

        use writer = connection.BeginBinaryImport(copyFromCommand)

        for row in table.Rows do
            writer.WriteRow(row.ItemArray)

        writer.Complete()
