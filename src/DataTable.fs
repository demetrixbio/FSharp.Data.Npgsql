namespace FSharp.Data

open System
open System.Data
open System.Collections.Generic
open Npgsql

[<Sealed>]
[<CompilerMessageAttribute("This API supports the FSharp.Data.Npgsql infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
type DataTable<'T when 'T :> DataRow>(selectCommand: NpgsqlCommand, ?enumTypeColumns: string[]) as this = 
    inherit DataTable() 

    let update (tx, conn, batchSize, continueUpdateOnError, conflictOption)  = 
        selectCommand.Transaction <- tx
        selectCommand.Connection <- conn
        
        use dataAdapter = new NpgsqlDataAdapter(selectCommand)

        use commandBuilder = new NpgsqlCommandBuilder(dataAdapter)
        commandBuilder.ConflictOption <- defaultArg conflictOption ConflictOption.OverwriteChanges

        use __ = dataAdapter.RowUpdating.Subscribe(fun args ->

            //enumTypeColumns |> Option.iter (fun enumColumns -> 
            //    let ps: NpgsqlParameterCollection = downcast args.Command.Parameters 
            //    for p in ps do 
            //        if Array.contains p.SourceColumn enumColumns
            //        then 
            //            p.NpgsqlDbType <- NpgsqlTypes.NpgsqlDbType.Unknown
            //)
                    
            if  args.Errors = null 
                && args.StatementType = Data.StatementType.Insert 
                && defaultArg batchSize dataAdapter.UpdateBatchSize = 1
            then 
                let columnsToRefresh = ResizeArray()
                for c in this.Columns do
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

        batchSize |> Option.iter dataAdapter.set_UpdateBatchSize
        continueUpdateOnError |> Option.iter dataAdapter.set_ContinueUpdateOnError

        dataAdapter.Update(this)        

    let rows = base.Rows

    member __.Rows : IList<'T> = {
        new IList<'T> with
            member __.GetEnumerator() = rows.GetEnumerator()
            member __.GetEnumerator() : IEnumerator<'T> = (Seq.cast<'T> rows).GetEnumerator() 

            member __.Count = rows.Count
            member __.IsReadOnly = rows.IsReadOnly
            member __.Item 
                with get index = downcast rows.[index]
                and set index row = 
                    rows.RemoveAt(index)
                    rows.InsertAt(row, index)

            member __.Add row = rows.Add row
            member __.Clear() = rows.Clear()
            member __.Contains row = rows.Contains row
            member __.CopyTo(dest, index) = rows.CopyTo(dest, index)
            member __.IndexOf row = rows.IndexOf row
            member __.Insert(index, row) = rows.InsertAt(row, index)
            member __.Remove row = rows.Remove(row); true
            member __.RemoveAt index = rows.RemoveAt(index)
    }

    //member __.NewRow(): 'T = downcast base.NewRow()

    member this.Update(transaction, ?batchSize, ?continueUpdateOnError, ?conflictOption) = 
        update(transaction, transaction.Connection, batchSize, continueUpdateOnError, conflictOption)

    member this.Update(connectionString, ?batchSize, ?continueUpdateOnError, ?conflictOption) = 
        use conn = new NpgsqlConnection(connectionString)
        update(null, conn, batchSize, continueUpdateOnError, conflictOption)

