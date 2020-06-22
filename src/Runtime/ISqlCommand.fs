namespace FSharp.Data.Npgsql

open System
open System.Data
open Npgsql
open System.ComponentModel
open System.Reflection

[<EditorBrowsable(EditorBrowsableState.Never)>]
type ISqlCommand = 
    abstract Execute: parameters: (string * obj)[] -> obj
    abstract AsyncExecute: parameters: (string * obj)[] -> obj

[<EditorBrowsable(EditorBrowsableState.Never)>]
type DesignTimeConfig = {
    SqlStatement: string
    Parameters: NpgsqlParameter[]
    ResultType: ResultType
    SingleRow: bool
    ResultSets: ResultSetDefinition[]
    UseNetTopologySuite: bool
    Prepare: bool
    IsTypeReuseEnabled: bool
}

[<Sealed>]
[<EditorBrowsable(EditorBrowsableState.Never)>]
type ``ISqlCommand Implementation``(cfg: DesignTimeConfig, connection, commandTimeout) = 

    let cmd = new NpgsqlCommand(cfg.SqlStatement, CommandTimeout = commandTimeout)
    do
        cmd.Parameters.AddRange( cfg.Parameters)
    
    let readerBehavior = 
        if cfg.SingleRow then CommandBehavior.SingleRow else CommandBehavior.Default
        ||| if cfg.ResultType = ResultType.DataTable then CommandBehavior.KeyInfo else CommandBehavior.Default
        ||| match connection with Choice1Of2 _ -> CommandBehavior.CloseConnection | _ ->  CommandBehavior.Default
        
    let setupConnection() = 
        match connection with
        | Choice2Of2(conn, tx) -> 
            cmd.Connection <- conn
            cmd.Transaction <- tx 
            { new IDisposable with member __.Dispose() = () }            
        | Choice1Of2 connectionString -> 
            cmd.Connection <- new NpgsqlConnection(connectionString)
            cmd.Connection.Open()
            if cfg.UseNetTopologySuite then cmd.Connection.TypeMapper.UseNetTopologySuite() |> ignore
            upcast cmd.Connection
        
    let asyncSetupConnection() = 
        async {
            match connection with
            | Choice2Of2 _ -> 
                return setupConnection()
            | Choice1Of2 connectionString -> 
                cmd.Connection <- new NpgsqlConnection(connectionString)
                do! cmd.Connection.OpenAsync() |> Async.AwaitTask
                if cfg.UseNetTopologySuite then cmd.Connection.TypeMapper.UseNetTopologySuite() |> ignore
                return upcast cmd.Connection
        }

    static let listToOption source =  
        match source |> List.truncate 2 with
        | [] -> None
        | [ x ] -> Some x
        | _ -> invalidOp "The output sequence contains more than one element."

    let execute, asyncExecute = 
        match cfg.ResultType with
        | ResultType.DataReader -> 
            ``ISqlCommand Implementation``.ExecuteReader >> box, 
            ``ISqlCommand Implementation``.AsyncExecuteReader >> box
        | ResultType.DataTable ->
            if cfg.ResultSets.Length = 1 then
                ``ISqlCommand Implementation``.ExecuteDataTable >> box, 
                ``ISqlCommand Implementation``.AsyncExecuteDataTable >> box
            else
                ``ISqlCommand Implementation``.ExecuteDataTables >> box, 
                ``ISqlCommand Implementation``.AsyncExecuteDataTables >> box
        | ResultType.Records | ResultType.Tuples ->
            match cfg.ResultSets with
            | [| resultSet |] ->
                if isNull resultSet.SeqItemTypeName (*|| isNull (box resultSet.Row2ItemMapping)*) then
                    ``ISqlCommand Implementation``.ExecuteNonQuery >> box, 
                    ``ISqlCommand Implementation``.AsyncExecuteNonQuery >> box
                else
                    let itemType = Type.GetType( resultSet.SeqItemTypeName, throwOnError = true)
                    
                    let executeHandle = 
                        typeof<``ISqlCommand Implementation``>
                            .GetMethod("ExecuteList", BindingFlags.NonPublic ||| BindingFlags.Static)
                            .MakeGenericMethod(itemType)
                    
                    let asyncExecuteHandle = 
                        typeof<``ISqlCommand Implementation``>
                            .GetMethod("AsyncExecuteList", BindingFlags.NonPublic ||| BindingFlags.Static)
                            .MakeGenericMethod(itemType)
                            
                    executeHandle.Invoke(null, [| cfg.ResultType; cfg.IsTypeReuseEnabled |]) |> unbox >> box, 
                    asyncExecuteHandle.Invoke(null, [| cfg.ResultType; cfg.IsTypeReuseEnabled |]) |> unbox >> box
            | _ ->
                ``ISqlCommand Implementation``.ExecuteMulti (cfg.ResultType, cfg.IsTypeReuseEnabled) >> box,
                ``ISqlCommand Implementation``.AsyncExecuteMulti (cfg.ResultType, cfg.IsTypeReuseEnabled) >> box
        | unexpected -> failwithf "Unexpected ResultType value: %O" unexpected

    member __.CommandTimeout = cmd.CommandTimeout

    interface ISqlCommand with
        member __.Execute parameters = execute(cmd, setupConnection, readerBehavior, parameters, cfg.ResultSets, cfg.Prepare)
        member __.AsyncExecute parameters = asyncExecute(cmd, asyncSetupConnection, readerBehavior, parameters, cfg.ResultSets, cfg.Prepare)

    interface IDisposable with
        member __.Dispose() =
            cmd.Dispose()

    static member internal SetParameters(cmd: NpgsqlCommand, parameters: (string * obj)[]) = 
        for name, value in parameters do
            
            let p = cmd.Parameters.[name]            

            if p.Direction.HasFlag(ParameterDirection.Input)
            then 
                if value = null 
                then 
                    p.Value <- DBNull.Value 
                else
                    p.Value <- value
            elif p.Direction.HasFlag(ParameterDirection.Output) && value :? Array
            then
                p.Size <- (value :?> Array).Length


    //Execute/AsyncExecute versions
    static member internal VerifyOutputColumns(cursor: Common.DbDataReader, expectedColumns: DataColumn[]) = 
        if  cursor.FieldCount < expectedColumns.Length
        then 
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
            if (expectedName <> "" && actualName <> expectedName) 
                || (actualType <> expectedType && not (maybeArray || maybeEnum) && not typeless)
            then 
                let message = sprintf """Expected column "%s" of type "%A" at position %i (0-based indexing) but received column "%s" of type "%A".""" expectedName expectedType i actualName actualType
                cursor.Close()
                invalidOp message

    static member internal ExecuteReader(cmd, setupConnection, readerBehavior, parameters, resultSetDefinitions: ResultSetDefinition[], prepare) = 
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
        setupConnection() |> ignore

        if prepare then
            cmd.Prepare()

        let cursor = cmd.ExecuteReader(readerBehavior)
        // Can't verify output columns of all result sets without calling NextResult
        ``ISqlCommand Implementation``.VerifyOutputColumns(cursor, resultSetDefinitions.[0].ExpectedColumns)
        cursor

    static member internal AsyncExecuteReader(cmd, setupConnection, readerBehavior: CommandBehavior, parameters, resultSetDefinitions: ResultSetDefinition[], prepare) = 
        async {
            ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
            let! _ = setupConnection()

            if prepare then
                do! cmd.PrepareAsync() |> Async.AwaitTask

            let! cursor = cmd.ExecuteReaderAsync( readerBehavior) |> Async.AwaitTask
            // Can't verify output columns of all result sets without calling NextResult
            ``ISqlCommand Implementation``.VerifyOutputColumns(downcast cursor, resultSetDefinitions.[0].ExpectedColumns)
            return cursor :?> NpgsqlDataReader
        }
    
    static member internal LoadDataTable (cursor: Common.DbDataReader) cmd columns =
        let result = new FSharp.Data.Npgsql.DataTable<DataRow>(selectCommand = cmd)
        result.Columns.AddRange(columns)
        result.Load(cursor)
        result

    static member internal AsyncExecuteDataTables(cmd, setupConnection, readerBehavior, parameters, resultSets: ResultSetDefinition[], prepare) = async {
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
        do! setupConnection() |> Async.Ignore

        if prepare then
            do! cmd.PrepareAsync() |> Async.AwaitTask

        use! cursor = cmd.ExecuteReaderAsync(readerBehavior) |> Async.AwaitTask

        // No explicit NextResult calls, Load takes care of it
        let results =
            resultSets
            |> Array.map (fun resultSet ->
                if Array.isEmpty resultSet.ExpectedColumns then
                    null
                else
                    ``ISqlCommand Implementation``.VerifyOutputColumns(cursor, resultSet.ExpectedColumns)
                    ``ISqlCommand Implementation``.LoadDataTable cursor (cmd.Clone()) resultSet.ExpectedColumns |> box)

        ``ISqlCommand Implementation``.SetNumberOfAffectedRecords results cmd.Statements
        return box results }

    // Reads data tables from multiple result sets
    static member internal ExecuteDataTables(cmd, setupConnection, readerBehavior, parameters, resultSets: ResultSetDefinition[], prepare) = 
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
        setupConnection() |> ignore

        if prepare then
            cmd.Prepare()

        use cursor = cmd.ExecuteReader(readerBehavior)        

        // No explicit NextResult calls, Load takes care of it
        let results =
            resultSets
            |> Array.map (fun resultSet ->
                if Array.isEmpty resultSet.ExpectedColumns then
                    null
                else
                    ``ISqlCommand Implementation``.VerifyOutputColumns(cursor, resultSet.ExpectedColumns)
                    ``ISqlCommand Implementation``.LoadDataTable cursor (cmd.Clone()) resultSet.ExpectedColumns |> box)

        ``ISqlCommand Implementation``.SetNumberOfAffectedRecords results cmd.Statements
        box results

    static member internal ExecuteDataTable(cmd, setupConnection, readerBehavior, parameters, resultSets, prepare) = 
        use cursor = ``ISqlCommand Implementation``.ExecuteReader(cmd, setupConnection, readerBehavior, parameters, resultSets, prepare) 
        ``ISqlCommand Implementation``.LoadDataTable cursor (cmd.Clone()) resultSets.[0].ExpectedColumns

    static member internal AsyncExecuteDataTable(cmd, setupConnection, readerBehavior, parameters, resultSets, prepare) = 
        async {
            use! reader = ``ISqlCommand Implementation``.AsyncExecuteReader(cmd, setupConnection, readerBehavior, parameters, resultSets, prepare) 
            return ``ISqlCommand Implementation``.LoadDataTable reader (cmd.Clone()) resultSets.[0].ExpectedColumns
        }

    static member internal ExecuteList<'TItem> (resultType, isTypeReuseEnabled) = fun(cmd: NpgsqlCommand, setupConnection, readerBehavior, parameters, resultSetDefinitions: ResultSetDefinition[], prepare) -> 
        let hasOutputParameters = cmd.Parameters |> Seq.cast<NpgsqlParameter> |> Seq.exists (fun x -> x.Direction.HasFlag( ParameterDirection.Output))

        if not hasOutputParameters
        then
            use reader = ``ISqlCommand Implementation``.ExecuteReader(cmd, setupConnection, readerBehavior, parameters, resultSetDefinitions, prepare)
            let xs = reader.MapRowValues<'TItem>(resultType, resultSetDefinitions.[0], isTypeReuseEnabled) |> Seq.toList

            if readerBehavior.HasFlag(CommandBehavior.SingleRow)
            then 
                xs |> listToOption |> box
            else 
                box xs 
        else
            use reader = ``ISqlCommand Implementation``.ExecuteReader(cmd, setupConnection, readerBehavior, parameters, resultSetDefinitions, prepare)
            let resultset = reader.MapRowValues<'TItem>(resultType, resultSetDefinitions.[0], isTypeReuseEnabled) |> Seq.toList

            if hasOutputParameters
            then
                for i = 0 to parameters.Length - 1 do
                    let name, _ = parameters.[i]
                    let p = cmd.Parameters.[name]
                    if p.Direction.HasFlag( ParameterDirection.Output)
                    then 
                        parameters.[i] <- name, p.Value

            box resultset

    // TODO output params
    static member internal ExecuteSingle<'TItem> (reader: Common.DbDataReader, readerBehavior: CommandBehavior, resultType, resultSetDefinition, isTypeReuseEnabled) = 
        let xs = reader.MapRowValues<'TItem>(resultType, resultSetDefinition, isTypeReuseEnabled) |> Seq.toList

        if readerBehavior.HasFlag(CommandBehavior.SingleRow)
        then 
            xs |> listToOption |> box
        else 
            box xs 
            
    static member internal AsyncExecuteList<'TItem> (resultType, isTypeReuseEnabled) = fun(cmd, setupConnection, readerBehavior, parameters, resultSetDefinitions, prepare) ->
        let xs = 
            async {
                use! reader = ``ISqlCommand Implementation``.AsyncExecuteReader(cmd, setupConnection, readerBehavior, parameters, resultSetDefinitions, prepare)
                return reader.MapRowValues<'TItem>(resultType, resultSetDefinitions.[0], isTypeReuseEnabled) |> Seq.toList
            }

        if readerBehavior.HasFlag(CommandBehavior.SingleRow)
        then
            async {
                let! xs = xs 
                return xs |> listToOption
            }
            |> box
        else 
            box xs 

    static member private ReadResultSet (cursor: Common.DbDataReader) readerBehavior resultType resultSetDefinition isTypeReuseEnabled =
        ``ISqlCommand Implementation``.VerifyOutputColumns(cursor, resultSetDefinition.ExpectedColumns)
        let itemType = Type.GetType(resultSetDefinition.SeqItemTypeName, throwOnError = true)
        
        let executeHandle = 
            typeof<``ISqlCommand Implementation``>
                .GetMethod("ExecuteSingle", BindingFlags.NonPublic ||| BindingFlags.Static)
                .MakeGenericMethod(itemType)
                
        executeHandle.Invoke(null, [| cursor; readerBehavior; resultType; resultSetDefinition; isTypeReuseEnabled |])

    static member internal ExecuteMulti (resultType, isTypeReuseEnabled) = fun (cmd, setupConnection, readerBehavior, parameters, resultSets: ResultSetDefinition[], prepare) ->
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
        setupConnection() |> ignore

        if prepare then
            cmd.Prepare()

        use cursor = cmd.ExecuteReader(readerBehavior)

        let results = Array.zeroCreate cursor.Statements.Count

        // Command contains at least one query
        if resultSets |> Array.exists (fun x -> Array.isEmpty x.ExpectedColumns |> not) then
            let mutable go = true

            while go do
                let currentStatement = cursor.GetStatementIndex()
                results.[currentStatement] <- ``ISqlCommand Implementation``.ReadResultSet cursor readerBehavior resultType resultSets.[currentStatement] isTypeReuseEnabled
                go <- cursor.NextResult()

        ``ISqlCommand Implementation``.SetNumberOfAffectedRecords results cmd.Statements
        box results

    static member internal AsyncExecuteMulti (resultType, isTypeReuseEnabled) = fun (cmd, setupConnection, readerBehavior: CommandBehavior, parameters, resultSets: ResultSetDefinition[], prepare) -> async {
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
        do! setupConnection() |> Async.Ignore

        if prepare then
            do! cmd.PrepareAsync() |> Async.AwaitTask

        use! cursor = cmd.ExecuteReaderAsync(readerBehavior) |> Async.AwaitTask

        let results = Array.zeroCreate cmd.Statements.Count

        // Command contains at least one query
        if resultSets |> Array.exists (fun x -> Array.isEmpty x.ExpectedColumns |> not) then
            let mutable go = true

            while go do
                let currentStatement = cursor.GetStatementIndex()
                results.[currentStatement] <- ``ISqlCommand Implementation``.ReadResultSet cursor readerBehavior resultType resultSets.[currentStatement] isTypeReuseEnabled
                let! more = cursor.NextResultAsync() |> Async.AwaitTask
                go <- more

        ``ISqlCommand Implementation``.SetNumberOfAffectedRecords results cmd.Statements
        return box results }

    static member internal ExecuteNonQuery (cmd, setupConnection, _, parameters, _, prepare) = 
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)  
        use __ = setupConnection()

        if prepare then
            cmd.Prepare()

        let recordsAffected = cmd.ExecuteNonQuery() 
        for i = 0 to parameters.Length - 1 do
            let name, _ = parameters.[i]
            let p = cmd.Parameters.[name]
            if p.Direction.HasFlag( ParameterDirection.Output)
            then 
                parameters.[i] <- name, p.Value
        recordsAffected

    static member internal AsyncExecuteNonQuery (cmd, setupConnection, _, parameters, _, prepare) = 
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)  
        async {         
            use! __ = setupConnection()

            if prepare then
                do! cmd.PrepareAsync() |> Async.AwaitTask

            return! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask
        }

    static member internal SetNumberOfAffectedRecords (results: obj[]) (statements: System.Collections.Generic.IReadOnlyList<NpgsqlStatement>) =
        for i in 0 .. statements.Count - 1 do
            if isNull results.[i] then
                results.[i] <- int statements.[i].Rows |> box
