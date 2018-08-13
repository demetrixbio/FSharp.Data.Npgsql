namespace FSharp.Data.Npgsql

open System.Data.Common
open Npgsql

type internal BatchDataAdapter(selectCommand: NpgsqlCommand, batchTimeout) = 
    inherit DbDataAdapter(SelectCommand = selectCommand) 

    let rowUpdating = Event<_>()
    let rowUpdated = Event<_>()

    let batch = new NpgsqlCommand(Connection = selectCommand.Connection, Transaction = selectCommand.Transaction, CommandTimeout = batchTimeout)
    let mutable count = -1

    [<CLIEvent>] member __.RowUpdating = rowUpdating.Publish
    [<CLIEvent>] member __.RowUpdated = rowUpdated.Publish
    override __.OnRowUpdating( value) = rowUpdating.Trigger(value)
    override __.OnRowUpdated( value) = rowUpdated.Trigger(value)

    override val UpdateBatchSize = 1 with get, set

    override __.InitializeBatching() = ()

    override __.AddToBatch( command) = 
        batch.CommandText <- sprintf "%s\n%s;" batch.CommandText command.CommandText
        batch.Parameters.AddRange( [| for p in (command :?> NpgsqlCommand).Parameters -> p.Clone() |])
        count <- count + 1
        count

    override __.ExecuteBatch() = batch.ExecuteNonQuery()

    override __.GetBatchedRecordsAffected(commandIdentifier, recordsAffected, error) = 
        recordsAffected <- int batch.Statements.[commandIdentifier].Rows
        error <- null
        true

    override __.ClearBatch() = 
        batch.CommandText <- ""
        batch.Parameters.Clear()
        count <- -1

    override __.TerminateBatching() = batch.Dispose()
