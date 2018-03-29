namespace FSharp.Data.Npgsql

open System
open System.Data
open System.Data.Common
open System.Runtime.CompilerServices
open System.ComponentModel
open Npgsql

[<Extension>]
[<EditorBrowsable(EditorBrowsableState.Never)>]
type Utils private() =

    [<Extension>]
    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member MapRowValues<'TItem>(cursor: DbDataReader ,rowMapping) = 
        seq {
            use _ = cursor
            let values = Array.zeroCreate cursor.FieldCount
            while cursor.Read() do
                cursor.GetValues(values) |> ignore
                yield values |> rowMapping |> unbox<'TItem>
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
    static member UpdateDataTable(table: DataTable<DataRow>, connection, transaction, continueUpdateOnError, conflictOption) = 

        let selectCommand = table.SelectCommand

        if connection <> null then selectCommand.Connection <- connection
        if transaction <> null then selectCommand.Transaction <- transaction

        use dataAdapter = new NpgsqlDataAdapter(selectCommand, ContinueUpdateOnError = continueUpdateOnError)

        use commandBuilder = new CommandBuilder(table, DataAdapter = dataAdapter, ConflictOption = conflictOption)

        dataAdapter.InsertCommand <- downcast commandBuilder.GetInsertCommand()
        dataAdapter.DeleteCommand <- downcast commandBuilder.GetDeleteCommand()
        dataAdapter.UpdateCommand <- downcast commandBuilder.GetUpdateCommand()

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
