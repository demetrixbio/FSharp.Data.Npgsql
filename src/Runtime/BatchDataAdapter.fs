﻿namespace FSharp.Data.Npgsql

open System.Data.Common
open Npgsql
open System
open System.Data

type internal BatchDataAdapter(selectCommand: NpgsqlCommand, batchTimeout) = 
    inherit DbDataAdapter(SelectCommand = selectCommand) 

    let rowUpdating = Event<_>()
    let rowUpdated = Event<_>()

    let batch = 
        new NpgsqlCommand(
            Connection = selectCommand.Connection, 
            Transaction = selectCommand.Transaction, 
            CommandTimeout = batchTimeout,
            UpdatedRowSource = UpdateRowSource.None
        )

    let mutable cmdIndex = -1

    [<CLIEvent>] member __.RowUpdating = rowUpdating.Publish
    [<CLIEvent>] member __.RowUpdated = rowUpdated.Publish
    override __.OnRowUpdating( value) = rowUpdating.Trigger(value)
    override __.OnRowUpdated( value) = rowUpdated.Trigger(value)

    override val UpdateBatchSize = 1 with get, set
    override __.InitializeBatching() = ()

    override __.AddToBatch( command) = 
        batch.CommandText <- sprintf "%s\n%s;" batch.CommandText command.CommandText
        batch.Parameters.AddRange [| 
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
        cmdIndex <- cmdIndex + 1
        cmdIndex

    override __.ExecuteBatch() = 
        let res = batch.ExecuteNonQuery()
        res

    override __.GetBatchedRecordsAffected(commandIdentifier, recordsAffected, error) = 
        recordsAffected <- int batch.Statements.[commandIdentifier].Rows
        error <- null
        true

    override __.ClearBatch() = 
        batch.CommandText <- ""
        batch.Parameters.Clear()
        cmdIndex <- -1

    override __.TerminateBatching() = batch.Dispose()

    interface System.IDisposable with
        member __.Dispose() = 
            batch.Dispose()
            base.Dispose()
