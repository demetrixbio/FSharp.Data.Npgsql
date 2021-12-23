namespace FSharp.Data.Npgsql

open System.Data.Common
open Npgsql
open System

type internal BatchDataAdapter(selectCommand: NpgsqlCommand, batchTimeout) = 
    inherit DbDataAdapter(SelectCommand = selectCommand) 

    let rowUpdating = Event<_>()
    let rowUpdated = Event<_>()

    let batch =
        new NpgsqlBatch(
            Connection = selectCommand.Connection, 
            Transaction = selectCommand.Transaction
        )

    do batch.Timeout <- batchTimeout

    let mutable cmdIndex = -1

    [<CLIEvent>] member _.RowUpdating = rowUpdating.Publish
    [<CLIEvent>] member _.RowUpdated = rowUpdated.Publish
    override _.OnRowUpdating( value) = rowUpdating.Trigger(value)
    override _.OnRowUpdated( value) = rowUpdated.Trigger(value)

    override val UpdateBatchSize = 1 with get, set
    override _.InitializeBatching() = ()
    
    override _.AddToBatch( command) =
        let cmd = NpgsqlBatchCommand()
        cmd.CommandText <- command.CommandText
        cmd.Parameters.AddRange [| 
            for p in (command :?> NpgsqlCommand).Parameters do
                let copy = p.Clone() 
                copy.Value <- 
                    match p.Value with 
                    | :? array<char> as value -> value |> Array.copy |> box
                    | :? array<byte> as value -> value |> Array.copy |> box
                    | :? ICloneable as value -> value.Clone() 
                    | asIs -> asIs

                yield copy
        |]

        batch.BatchCommands.Add(cmd)
        cmdIndex <- cmdIndex + 1
        cmdIndex

    override _.ExecuteBatch() = 
        let res = batch.ExecuteNonQuery()
        res

    override _.GetBatchedRecordsAffected(commandIdentifier, recordsAffected, error) = 
        recordsAffected <- int batch.BatchCommands.[commandIdentifier].Rows
        error <- null
        true

    override _.ClearBatch() = 
        batch.BatchCommands.Clear()
        cmdIndex <- -1

    override _.TerminateBatching() = batch.Dispose()

    interface IDisposable with
        member _.Dispose() = 
            batch.Dispose()
            base.Dispose()
