namespace FSharp.Data.Npgsql

open System
open System.Data
open Npgsql
open System.ComponentModel
open System.Reflection
open System.Collections.Concurrent
open FSharp.Control.Tasks.NonAffine
open type Utils

type internal ExecutionType =
    | Sync
    | Async
    | TaskAsync

[<EditorBrowsable(EditorBrowsableState.Never)>]
type ISqlCommand = 
    abstract Execute: parameters: (string * obj)[] -> obj
    abstract AsyncExecute: parameters: (string * obj)[] -> obj
    abstract TaskAsyncExecute: parameters: (string * obj)[] -> obj

[<EditorBrowsable(EditorBrowsableState.Never)>]
type DesignTimeConfig = {
    SqlStatement: string
    Parameters: NpgsqlParameter[]
    ResultType: ResultType
    CollectionType: CollectionType
    SingleRow: bool
    ResultSets: ResultSetDefinition[]
    UseNetTopologySuite: bool
    Prepare: bool
}

[<EditorBrowsable(EditorBrowsableState.Never)>]
type ISqlCommandImplementation (commandNameHash: int, cfgBuilder: unit -> DesignTimeConfig, connection, commandTimeout) =
    static let cfgCache = ConcurrentDictionary<int, DesignTimeConfig> ()

    let cfg =
        let mutable cfg = Unchecked.defaultof<_>
        if cfgCache.TryGetValue (commandNameHash, &cfg) then
            cfg
        else
            let cfg = cfgBuilder ()
            cfgCache.[commandNameHash] <- cfg
            cfg

    let cmd =
        let cmd = new NpgsqlCommand (cfg.SqlStatement, CommandTimeout = commandTimeout)
        for p in cfg.Parameters do
            p.Clone () |> cmd.Parameters.Add |> ignore
        cmd

    let readerBehavior = 
        if cfg.SingleRow then CommandBehavior.SingleRow else CommandBehavior.Default
        ||| if cfg.ResultType = ResultType.DataTable then CommandBehavior.KeyInfo else CommandBehavior.Default
        ||| match connection with Choice1Of2 _ -> CommandBehavior.CloseConnection | _ -> CommandBehavior.Default

    let setupConnection () =
        match connection with
        | Choice2Of2 (conn, tx) ->
            cmd.Connection <- conn
            cmd.Transaction <- tx
            Unsafe.uply.Zero ()
        | Choice1Of2 connectionString -> Unsafe.uply {
            cmd.Connection <- new NpgsqlConnection (connectionString)
            do! cmd.Connection.OpenAsync ()
            if cfg.UseNetTopologySuite then cmd.Connection.TypeMapper.UseNetTopologySuite () |> ignore }

    static let mapTask (t: Ply.Ply<_>, executionType) =
        let t = task { return! t }

        match executionType with
        | Sync -> box t.Result
        | Async -> Async.AwaitTask t |> box
        | TaskAsync -> box t

    let execute =
        match cfg.ResultType with
        | ResultType.DataReader ->
            ISqlCommandImplementation.AsyncExecuteReader
        | ResultType.DataTable ->
            if cfg.ResultSets.Length = 1 then
                ISqlCommandImplementation.AsyncExecuteDataTable
            else
                ISqlCommandImplementation.AsyncExecuteDataTables
        | ResultType.Records | ResultType.Tuples ->
            match cfg.ResultSets with
            | [| resultSet |] ->
                if isNull resultSet.SeqItemType then
                    ISqlCommandImplementation.AsyncExecuteNonQuery
                else
                    typeof<ISqlCommandImplementation>
                        .GetMethod(nameof ISqlCommandImplementation.AsyncExecuteList, BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod(resultSet.SeqItemType)
                        .Invoke(null, [||]) |> unbox
            | _ ->
                ISqlCommandImplementation.AsyncExecuteMulti
        | unexpected -> failwithf "Unexpected ResultType value: %O" unexpected

    interface ISqlCommand with
        member __.Execute parameters = execute (cfg, cmd, setupConnection, readerBehavior, parameters, Sync)
        member __.AsyncExecute parameters = execute (cfg, cmd, setupConnection, readerBehavior, parameters, Async)
        member __.TaskAsyncExecute parameters = execute (cfg, cmd, setupConnection, readerBehavior, parameters, TaskAsync)

    interface IDisposable with
        member __.Dispose () =
            if cfg.CollectionType <> CollectionType.LazySeq then
                cmd.Dispose ()

    static member internal SetParameters (cmd: NpgsqlCommand, parameters: (string * obj)[]) =
        for name, value in parameters do

            let p = cmd.Parameters.[name]

            if p.Direction.HasFlag(ParameterDirection.Input) then
                if value = null then
                    p.Value <- DBNull.Value
                else
                    p.Value <- value
            elif p.Direction.HasFlag(ParameterDirection.Output) && value :? Array
            then
                p.Size <- (value :?> Array).Length

    static member internal VerifyOutputColumns(cursor: Common.DbDataReader, expectedColumns: DataColumn[]) =
        if cursor.FieldCount < expectedColumns.Length then
            let message = sprintf "Expected at least %i columns in result set but received only %i." expectedColumns.Length cursor.FieldCount
            cursor.Close()
            invalidOp message

        for i = 0 to expectedColumns.Length - 1 do
            let expectedName, expectedType = expectedColumns.[i].ColumnName, expectedColumns.[i].DataType
            let actualName, actualType = cursor.GetName( i), cursor.GetFieldType( i)
                
            //TO DO: add extended property on column to mark enums
            let maybeEnum = expectedType = typeof<string> && actualType = typeof<obj>
            let maybeArray = (expectedType = typeof<Array> || expectedType.IsArray) && (actualType = typeof<Array> || actualType.IsArray)
            let typeless = expectedType = typeof<obj> && actualType = typeof<string>
            let isGeometry = actualType = typeof<NetTopologySuite.Geometries.Geometry>
            if (expectedName <> "" && actualName <> expectedName)
                || (actualType <> expectedType && not (maybeArray || maybeEnum) && not typeless && not isGeometry)
            then 
                let message = sprintf """Expected column "%s" of type "%A" at position %i (0-based indexing) but received column "%s" of type "%A".""" expectedName expectedType i actualName actualType
                cursor.Close()
                invalidOp message

    static member internal AsyncExecuteDataReaderTask (cfg, cmd, setupConnection: unit -> Ply.Ply<unit>, readerBehavior: CommandBehavior, parameters) = Unsafe.uply {
        ISqlCommandImplementation.SetParameters (cmd, parameters)
        do! setupConnection ()

        if cfg.Prepare then
            do! cmd.PrepareAsync ()

        let! cursor = cmd.ExecuteReaderAsync readerBehavior
        return cursor :?> NpgsqlDataReader }

    static member internal AsyncExecuteReader (cfg, cmd, setupConnection, readerBehavior, parameters, executionType) =
        mapTask (ISqlCommandImplementation.AsyncExecuteDataReaderTask(cfg, cmd, setupConnection, readerBehavior, parameters), executionType)

    static member internal LoadDataTable (cursor: Common.DbDataReader) cmd (columns: DataColumn[]) =
        let result = new FSharp.Data.Npgsql.DataTable<DataRow>(selectCommand = cmd)

        for c in columns do
            CloneDataColumn c |> result.Columns.Add

        result.Load cursor
        result

    static member internal AsyncExecuteDataTables (cfg, cmd, setupConnection, readerBehavior, parameters, executionType) =
        let t = Unsafe.uply {
            use! cursor = ISqlCommandImplementation.AsyncExecuteDataReaderTask (cfg, cmd, setupConnection, readerBehavior, parameters)

            // No explicit NextResult calls, Load takes care of it
            let results =
                cfg.ResultSets
                |> Array.map (fun resultSet ->
                    if Array.isEmpty resultSet.ExpectedColumns then
                        null
                    else
                        ISqlCommandImplementation.VerifyOutputColumns(cursor, resultSet.ExpectedColumns)
                        ISqlCommandImplementation.LoadDataTable cursor (cmd.Clone()) resultSet.ExpectedColumns |> box)

            ISqlCommandImplementation.SetNumberOfAffectedRows results cmd.Statements
            return results }

        mapTask (t, executionType)

    static member internal AsyncExecuteDataTable (cfg, cmd, setupConnection, readerBehavior, parameters, executionType) =
        let t = Unsafe.uply {
            use! reader = ISqlCommandImplementation.AsyncExecuteDataReaderTask (cfg, cmd, setupConnection, readerBehavior, parameters) 
            return ISqlCommandImplementation.LoadDataTable reader (cmd.Clone()) cfg.ResultSets.[0].ExpectedColumns }

        mapTask (t, executionType)

    // TODO output params
    static member internal ExecuteSingle<'TItem> (reader: Common.DbDataReader, readerBehavior: CommandBehavior, resultSetDefinition, cfg) = Unsafe.uply {
        let! xs = MapRowValues<'TItem> (reader, cfg.ResultType, resultSetDefinition)

        return
            if readerBehavior.HasFlag CommandBehavior.SingleRow then
                ResizeArrayToOption xs |> box
            elif cfg.CollectionType = CollectionType.Array then
                xs.ToArray () |> box
            elif cfg.CollectionType = CollectionType.List then
                ResizeArrayToList xs |> box
            else
                box xs }
            
    static member internal AsyncExecuteList<'TItem> () = fun (cfg, cmd, setupConnection, readerBehavior: CommandBehavior, parameters, executionType) ->
        if cfg.CollectionType = CollectionType.LazySeq && readerBehavior.HasFlag CommandBehavior.SingleRow |> not then
            let t = Unsafe.uply {
                let! reader = ISqlCommandImplementation.AsyncExecuteDataReaderTask (cfg, cmd, setupConnection, readerBehavior, parameters)
                let xs = MapRowValuesLazy<'TItem> (reader, cfg.ResultType, cfg.ResultSets.[0])
                return new LazySeq<'TItem> (xs, reader, cmd) }

            mapTask (t, executionType)
        else
            let xs = Unsafe.uply {
                use! reader = ISqlCommandImplementation.AsyncExecuteDataReaderTask (cfg, cmd, setupConnection, readerBehavior, parameters)
                return! MapRowValues<'TItem> (reader, cfg.ResultType, cfg.ResultSets.[0]) }

            if readerBehavior.HasFlag CommandBehavior.SingleRow then
                let t = Unsafe.uply {
                    let! xs = xs
                    return ResizeArrayToOption xs
                }
                mapTask (t, executionType)
            elif cfg.CollectionType = CollectionType.Array then
                let t = Unsafe.uply {
                    let! xs = xs
                    return xs.ToArray ()
                }
                mapTask (t, executionType)
            elif cfg.CollectionType = CollectionType.List then
                let t = Unsafe.uply {
                    let! xs = xs
                    return ResizeArrayToList xs
                }
                mapTask (t, executionType)
            else
                mapTask (xs, executionType)

    static member private ReadResultSet (cursor: Common.DbDataReader) readerBehavior resultSetDefinition cfg =
        ISqlCommandImplementation.VerifyOutputColumns(cursor, resultSetDefinition.ExpectedColumns)

        let executeHandle = 
            typeof<ISqlCommandImplementation>
                .GetMethod(nameof ISqlCommandImplementation.ExecuteSingle, BindingFlags.NonPublic ||| BindingFlags.Static)
                .MakeGenericMethod resultSetDefinition.SeqItemType

        executeHandle.Invoke (null, [| cursor; readerBehavior; resultSetDefinition; cfg |]) :?> Ply.Ply<obj>

    static member internal AsyncExecuteMulti (cfg, cmd, setupConnection, readerBehavior, parameters, executionType) =
        let t = Unsafe.uply {
            // Don't pass CommandBehavior.SingleRow to Npgsql, because it only applies to the first row of the first result set and all other result sets are completely ignored
            use! cursor = ISqlCommandImplementation.AsyncExecuteDataReaderTask (cfg, cmd, setupConnection, readerBehavior &&& ~~~CommandBehavior.SingleRow, parameters)
            let results = Array.zeroCreate cmd.Statements.Count

            // Command contains at least one query
            if cfg.ResultSets |> Array.exists (fun x -> Array.isEmpty x.ExpectedColumns |> not) then
                let mutable go = true

                while go do
                    let currentStatement = GetStatementIndex cursor
                    let! res = ISqlCommandImplementation.ReadResultSet cursor readerBehavior cfg.ResultSets.[currentStatement] cfg
                    results.[currentStatement] <- res
                    let! more = cursor.NextResultAsync ()
                    go <- more

            ISqlCommandImplementation.SetNumberOfAffectedRows results cmd.Statements
            return results }

        mapTask (t, executionType)

    static member internal AsyncExecuteNonQuery (cfg, cmd, setupConnection, readerBehavior, parameters, executionType) = 
        let t = Unsafe.uply {
            ISqlCommandImplementation.SetParameters (cmd, parameters)
            do! setupConnection ()
            use _ = if readerBehavior.HasFlag CommandBehavior.CloseConnection then cmd.Connection else null

            if cfg.Prepare then
                do! cmd.PrepareAsync ()

            return! cmd.ExecuteNonQueryAsync () }

        mapTask (t, executionType)

    static member internal SetNumberOfAffectedRows (results: obj[]) (statements: System.Collections.Generic.IReadOnlyList<NpgsqlStatement>) =
        for i in 0 .. statements.Count - 1 do
            if isNull results.[i] then
                results.[i] <- int statements.[i].Rows |> box
