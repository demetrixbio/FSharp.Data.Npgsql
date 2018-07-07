namespace FSharp.Data.Npgsql

open System.Data.Common
open Npgsql

type internal BatchDataAdapter(selectCommand: NpgsqlCommand) = 
    inherit DbDataAdapter(SelectCommand = selectCommand) 

    let rowUpdating = Event<_>()
    let rowUpdated = Event<_>()

    let batch = new NpgsqlCommand(Connection = selectCommand.Connection, Transaction = selectCommand.Transaction, CommandTimeout = selectCommand.CommandTimeout)
    let mutable count = -1

    [<CLIEvent>] member this.RowUpdating = rowUpdating.Publish
    [<CLIEvent>] member this.RowUpdated = rowUpdated.Publish
    override __.OnRowUpdating( value) = rowUpdating.Trigger(value)
    override __.OnRowUpdated( value) = rowUpdated.Trigger(value)

    override val UpdateBatchSize = 1 with get, set

    override __.InitializeBatching() = ()

    override __.AddToBatch( command) = 
        count <- count + 1
        let mutable commandText = command.CommandText
        for p in (command :?> NpgsqlCommand).Parameters do 
            let clone = batch.Parameters.Add(p.Clone())
            clone.ParameterName <- sprintf "%s_%i" clone.ParameterName count
            commandText <- commandText.Replace(p.ParameterName, clone.ParameterName)

        batch.CommandText <- sprintf "%s\n%s;" batch.CommandText commandText
        count

    override __.ExecuteBatch() = batch.ExecuteNonQuery()

    override __.GetBatchedRecordsAffected(commandIdentifier, recordsAffected, error) = 
        recordsAffected <- int batch.Statements.[commandIdentifier].Rows
        error <- null
        true

    override __.ClearBatch() = batch.Parameters.Clear()
    override __.TerminateBatching() = batch.Dispose()    
