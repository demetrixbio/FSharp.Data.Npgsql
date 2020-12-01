namespace FSharp.Data.Npgsql

open System
open System.Data
open Npgsql
open System.ComponentModel
open System.Reflection
open System.Collections.Concurrent

[<EditorBrowsable(EditorBrowsableState.Never)>]
type ISqlCommand = 
    abstract Execute: parameters: (string * obj)[] -> obj
    abstract AsyncExecute: parameters: (string * obj)[] -> obj

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

[<Sealed>]
[<EditorBrowsable(EditorBrowsableState.Never)>]
type ISqlCommandImplementation (commandNameHash: int, cfgBuilder: Func<int, DesignTimeConfig>, connection, commandTimeout) =
    static let cfgCache = ConcurrentDictionary<int, DesignTimeConfig> ()

    let cfg = cfgCache.GetOrAdd (commandNameHash, cfgBuilder)

    let cmd =
        let cmd = new NpgsqlCommand (cfg.SqlStatement, CommandTimeout = commandTimeout)
        for p in cfg.Parameters do
            p.Clone () |> cmd.Parameters.Add |> ignore
        cmd
    
    let readerBehavior = 
        if cfg.SingleRow then CommandBehavior.SingleRow else CommandBehavior.Default
        ||| if cfg.ResultType = ResultType.DataTable then CommandBehavior.KeyInfo else CommandBehavior.Default
        ||| match connection with Choice1Of2 _ -> CommandBehavior.CloseConnection | _ ->  CommandBehavior.Default
        
    let setupConnection () =
        match connection with
        | Choice2Of2 (conn, tx) ->
            cmd.Connection <- conn
            cmd.Transaction <- tx
        | Choice1Of2 connectionString ->
            cmd.Connection <- new NpgsqlConnection (connectionString)
            cmd.Connection.Open ()
            if cfg.UseNetTopologySuite then cmd.Connection.TypeMapper.UseNetTopologySuite () |> ignore
        
    let asyncSetupConnection () = async {
        match connection with
        | Choice2Of2 (conn, tx) ->
            cmd.Connection <- conn
            cmd.Transaction <- tx
        | Choice1Of2 connectionString ->
            cmd.Connection <- new NpgsqlConnection (connectionString)
            do! cmd.Connection.OpenAsync () |> Async.AwaitTask
            if cfg.UseNetTopologySuite then cmd.Connection.TypeMapper.UseNetTopologySuite () |> ignore }

    let execute, asyncExecute = 
        match cfg.ResultType with
        | ResultType.DataReader ->
            ISqlCommandImplementation.ExecuteReader >> box,
            ISqlCommandImplementation.AsyncExecuteReader >> box
        | ResultType.DataTable ->
            if cfg.ResultSets.Length = 1 then
                ISqlCommandImplementation.ExecuteDataTable >> box,
                ISqlCommandImplementation.AsyncExecuteDataTable >> box
            else
                ISqlCommandImplementation.ExecuteDataTables >> box,
                ISqlCommandImplementation.AsyncExecuteDataTables >> box
        | ResultType.Records | ResultType.Tuples ->
            match cfg.ResultSets with
            | [| resultSet |] ->
                if isNull resultSet.SeqItemType then
                    ISqlCommandImplementation.ExecuteNonQuery >> box,
                    ISqlCommandImplementation.AsyncExecuteNonQuery >> box
                else
                    let executeHandle = 
                        typeof<ISqlCommandImplementation>
                            .GetMethod(nameof ISqlCommandImplementation.ExecuteList, BindingFlags.NonPublic ||| BindingFlags.Static)
                            .MakeGenericMethod resultSet.SeqItemType
                    
                    let asyncExecuteHandle = 
                        typeof<ISqlCommandImplementation>
                            .GetMethod(nameof ISqlCommandImplementation.AsyncExecuteList, BindingFlags.NonPublic ||| BindingFlags.Static)
                            .MakeGenericMethod resultSet.SeqItemType

                    executeHandle.Invoke (null, [||]) |> unbox >> box,
                    asyncExecuteHandle.Invoke (null, [||]) |> unbox >> box
            | _ ->
                ISqlCommandImplementation.ExecuteMulti >> box,
                ISqlCommandImplementation.AsyncExecuteMulti >> box
        | unexpected -> failwithf "Unexpected ResultType value: %O" unexpected

    interface ISqlCommand with
        member __.Execute parameters = execute(cfg, cmd, setupConnection, readerBehavior, parameters)
        member __.AsyncExecute parameters = asyncExecute(cfg, cmd, asyncSetupConnection, readerBehavior, parameters)

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

    //Execute/AsyncExecute versions
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

    static member internal ExecuteReader (cfg, cmd, setupConnection, readerBehavior, parameters) =
        ISqlCommandImplementation.SetParameters(cmd, parameters)
        setupConnection()

        if cfg.Prepare then
            cmd.Prepare()

        cmd.ExecuteReader(readerBehavior)

    static member internal AsyncExecuteReader (cfg, cmd, setupConnection, readerBehavior: CommandBehavior, parameters) = async {
        ISqlCommandImplementation.SetParameters(cmd, parameters)
        do! setupConnection()

        if cfg.Prepare then
            do! cmd.PrepareAsync() |> Async.AwaitTask

        let! cursor = cmd.ExecuteReaderAsync( readerBehavior) |> Async.AwaitTask
        return cursor :?> NpgsqlDataReader }
    
    static member internal LoadDataTable (cursor: Common.DbDataReader) cmd columns =
        let result = new FSharp.Data.Npgsql.DataTable<DataRow>(selectCommand = cmd)
        result.Columns.AddRange(columns)
        result.Load(cursor)
        result

    static member internal AsyncExecuteDataTables (cfg, cmd, setupConnection, readerBehavior, parameters) = async {
        ISqlCommandImplementation.SetParameters(cmd, parameters)
        do! setupConnection()

        if cfg.Prepare then
            do! cmd.PrepareAsync() |> Async.AwaitTask

        use! cursor = cmd.ExecuteReaderAsync(readerBehavior) |> Async.AwaitTask

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
        return box results }

    // Reads data tables from multiple result sets
    static member internal ExecuteDataTables (cfg, cmd, setupConnection, readerBehavior, parameters) =
        ISqlCommandImplementation.SetParameters(cmd, parameters)
        setupConnection()

        if cfg.Prepare then
            cmd.Prepare()

        use cursor = cmd.ExecuteReader(readerBehavior)        

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
        box results

    static member internal ExecuteDataTable (cfg, cmd, setupConnection, readerBehavior, parameters) = 
        use cursor = ISqlCommandImplementation.ExecuteReader (cfg, cmd, setupConnection, readerBehavior, parameters) 
        ISqlCommandImplementation.LoadDataTable cursor (cmd.Clone()) cfg.ResultSets.[0].ExpectedColumns

    static member internal AsyncExecuteDataTable (cfg, cmd, setupConnection, readerBehavior, parameters) = 
        async {
            use! reader = ISqlCommandImplementation.AsyncExecuteReader (cfg, cmd, setupConnection, readerBehavior, parameters) 
            return ISqlCommandImplementation.LoadDataTable reader (cmd.Clone()) cfg.ResultSets.[0].ExpectedColumns
        }

    static member internal ExecuteList<'TItem> () = fun (cfg, cmd: NpgsqlCommand, setupConnection, readerBehavior, parameters) ->
        let hasOutputParameters = cmd.Parameters |> Seq.cast<NpgsqlParameter> |> Seq.exists (fun x -> x.Direction.HasFlag ParameterDirection.Output)

        let reader = ISqlCommandImplementation.ExecuteReader (cfg, cmd, setupConnection, readerBehavior, parameters)

        if cfg.CollectionType = CollectionType.LazySeq && readerBehavior.HasFlag CommandBehavior.SingleRow |> not then
            let xs = reader.MapRowValuesLazy<'TItem> (cfg.ResultType, cfg.ResultSets.[0])
            new LazySeq<'TItem> (xs, reader, cmd) |> box
        else
            use reader = reader
            let xs = reader.MapRowValues<'TItem> (cfg.ResultType, cfg.ResultSets.[0])

            let out =
                if readerBehavior.HasFlag CommandBehavior.SingleRow then
                    Utils.ResizeArrayToOption xs |> box
                elif cfg.CollectionType = CollectionType.Array then
                    xs.ToArray () |> box
                elif cfg.CollectionType = CollectionType.List then
                    Utils.ResizeArrayToList xs |> box
                else
                    box xs 

            if hasOutputParameters then
                for i = 0 to parameters.Length - 1 do
                    let name, _ = parameters.[i]
                    let p = cmd.Parameters.[name]
                    if p.Direction.HasFlag ParameterDirection.Output then
                        parameters.[i] <- name, p.Value

            out

    // TODO output params
    static member internal ExecuteSingle<'TItem> (reader: Common.DbDataReader, readerBehavior: CommandBehavior, resultSetDefinition, cfg) = 
        let xs = reader.MapRowValues<'TItem> (cfg.ResultType, resultSetDefinition)

        if readerBehavior.HasFlag CommandBehavior.SingleRow then
            Utils.ResizeArrayToOption xs |> box
        elif cfg.CollectionType = CollectionType.Array then
            xs.ToArray () |> box
        elif cfg.CollectionType = CollectionType.List then
            Utils.ResizeArrayToList xs |> box
        else
            box xs 
            
    static member internal AsyncExecuteList<'TItem> () = fun (cfg, cmd, setupConnection, readerBehavior: CommandBehavior, parameters) ->
        if cfg.CollectionType = CollectionType.LazySeq && readerBehavior.HasFlag CommandBehavior.SingleRow |> not then
            async {
                let! reader = ISqlCommandImplementation.AsyncExecuteReader (cfg, cmd, setupConnection, readerBehavior, parameters)
                let xs = reader.MapRowValuesLazy<'TItem> (cfg.ResultType, cfg.ResultSets.[0])
                return new LazySeq<'TItem> (xs, reader, cmd)
            }
            |> box
        else
            let xs = 
                async {
                    use! reader = ISqlCommandImplementation.AsyncExecuteReader (cfg, cmd, setupConnection, readerBehavior, parameters)
                    return reader.MapRowValues<'TItem> (cfg.ResultType, cfg.ResultSets.[0])
                }

            if readerBehavior.HasFlag CommandBehavior.SingleRow then
                async {
                    let! xs = xs 
                    return Utils.ResizeArrayToOption xs
                }
                |> box
            elif cfg.CollectionType = CollectionType.Array then
                async {
                    let! xs = xs 
                    return xs.ToArray ()
                }
                |> box
            elif cfg.CollectionType = CollectionType.List then
                async {
                    let! xs = xs 
                    return Utils.ResizeArrayToList xs
                }
                |> box
            else
                box xs 

    static member private ReadResultSet (cursor: Common.DbDataReader) readerBehavior resultSetDefinition cfg =
        ISqlCommandImplementation.VerifyOutputColumns(cursor, resultSetDefinition.ExpectedColumns)
        
        let executeHandle = 
            typeof<ISqlCommandImplementation>
                .GetMethod(nameof ISqlCommandImplementation.ExecuteSingle, BindingFlags.NonPublic ||| BindingFlags.Static)
                .MakeGenericMethod resultSetDefinition.SeqItemType
                
        executeHandle.Invoke(null, [| cursor; readerBehavior; resultSetDefinition; cfg |])

    static member internal ExecuteMulti (cfg, cmd, setupConnection, readerBehavior, parameters) =
        ISqlCommandImplementation.SetParameters(cmd, parameters)
        setupConnection()

        if cfg.Prepare then
            cmd.Prepare()

        use cursor = cmd.ExecuteReader(readerBehavior)

        let results = Array.zeroCreate cursor.Statements.Count

        // Command contains at least one query
        if cfg.ResultSets |> Array.exists (fun x -> Array.isEmpty x.ExpectedColumns |> not) then
            let mutable go = true

            while go do
                let currentStatement = Utils.GetStatementIndex cursor
                results.[currentStatement] <- ISqlCommandImplementation.ReadResultSet cursor readerBehavior cfg.ResultSets.[currentStatement] cfg
                go <- cursor.NextResult()

        ISqlCommandImplementation.SetNumberOfAffectedRows results cmd.Statements
        box results

    static member internal AsyncExecuteMulti (cfg, cmd, setupConnection, readerBehavior: CommandBehavior, parameters) = async {
        ISqlCommandImplementation.SetParameters(cmd, parameters)
        do! setupConnection()

        if cfg.Prepare then
            do! cmd.PrepareAsync() |> Async.AwaitTask

        use! cursor = cmd.ExecuteReaderAsync(readerBehavior) |> Async.AwaitTask

        let results = Array.zeroCreate cmd.Statements.Count

        // Command contains at least one query
        if cfg.ResultSets |> Array.exists (fun x -> Array.isEmpty x.ExpectedColumns |> not) then
            let mutable go = true

            while go do
                let currentStatement = Utils.GetStatementIndex (cursor :?> _)
                results.[currentStatement] <- ISqlCommandImplementation.ReadResultSet cursor readerBehavior cfg.ResultSets.[currentStatement] cfg
                let! more = cursor.NextResultAsync() |> Async.AwaitTask
                go <- more

        ISqlCommandImplementation.SetNumberOfAffectedRows results cmd.Statements
        return box results }

    static member internal ExecuteNonQuery (cfg, cmd, setupConnection, _, parameters) = 
        ISqlCommandImplementation.SetParameters(cmd, parameters)  
        setupConnection()

        if cfg.Prepare then
            cmd.Prepare()

        let rowsAffected = cmd.ExecuteNonQuery() 
        for i = 0 to parameters.Length - 1 do
            let name, _ = parameters.[i]
            let p = cmd.Parameters.[name]
            if p.Direction.HasFlag ParameterDirection.Output then
                parameters.[i] <- name, p.Value

        rowsAffected

    static member internal AsyncExecuteNonQuery (cfg, cmd, setupConnection, _, parameters) = 
        ISqlCommandImplementation.SetParameters(cmd, parameters)  
        async {         
            do! setupConnection()

            if cfg.Prepare then
                do! cmd.PrepareAsync() |> Async.AwaitTask

            return! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask
        }

    static member internal SetNumberOfAffectedRows (results: obj[]) (statements: System.Collections.Generic.IReadOnlyList<NpgsqlStatement>) =
        for i in 0 .. statements.Count - 1 do
            if isNull results.[i] then
                results.[i] <- int statements.[i].Rows |> box
