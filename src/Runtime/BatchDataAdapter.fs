namespace FSharp.Data.Npgsql

open System.Data.Common
open Npgsql

type internal BatchDataAdapter(selectCommand: NpgsqlCommand) = 
    inherit DbDataAdapter(SelectCommand = selectCommand) 

    let rowUpdating = Event<_>()
    let rowUpdated = Event<_>()

    let mutable count = -1

    let commands = ResizeArray()

    [<CLIEvent>] member this.RowUpdating = rowUpdating.Publish
    [<CLIEvent>] member this.RowUpdated = rowUpdated.Publish
    override __.OnRowUpdating( value) = rowUpdating.Trigger(value)
    override __.OnRowUpdated( value) = rowUpdated.Trigger(value)

    override val UpdateBatchSize = 1 with get, set

    override __.InitializeBatching() = ()

    override __.AddToBatch( command) = 
        commands.Add(ref command.CommandText, [ for p in (command :?> NpgsqlCommand).Parameters -> p.Clone() ], ref 0)
        commands.Count - 1

    override __.ExecuteBatch() = 
        use batch = new NpgsqlCommand(Connection = selectCommand.Connection, Transaction = selectCommand.Transaction, CommandTimeout = selectCommand.CommandTimeout)
        for commandIdentifier = 0 to commands.Count - 1 do 
            let cmdText, ps, _ = commands.[commandIdentifier]
            ps |> List.iteri( fun pIndex p -> 
                let clone = batch.Parameters.Add(p.Clone())
                clone.ParameterName <- sprintf "%s_%i" clone.ParameterName commandIdentifier
                let trailingChar = if pIndex = ps.Length - 1 then ")" else ","
                cmdText.Value <- cmdText.Value.Replace(p.ParameterName + trailingChar, clone.ParameterName + trailingChar)
            )
            batch.CommandText <- sprintf "%s\n%s;" batch.CommandText cmdText.Value

        let totalRecordsAffectedPerBatch = batch.ExecuteNonQuery()
        assert(batch.Statements.Count = commands.Count)

        for i = 0 to batch.Statements.Count - 1 do 
            let _, _, recordsAffected = commands.[i]
            recordsAffected.Value <- int batch.Statements.[i].Rows

        totalRecordsAffectedPerBatch

    override __.GetBatchedRecordsAffected(commandIdentifier, recordsAffected, error) = 
        let _, _, x = commands.[commandIdentifier]
        recordsAffected <- x.Value
        error <- null
        true

    override __.ClearBatch() = commands.Clear()
    override __.TerminateBatching() = ()
