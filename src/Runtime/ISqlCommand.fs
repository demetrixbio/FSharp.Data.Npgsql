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
type ResultSetDefinition = {
    Row2ItemMapping: obj[] -> obj
    SeqItemTypeName: string
    ExpectedColumns: DataColumn[]
}

[<EditorBrowsable(EditorBrowsableState.Never)>]
type DesignTimeConfig = {
    SqlStatement: string
    Parameters: NpgsqlParameter[]
    ResultType: ResultType
    SingleRow: bool
    ResultSets: ResultSetDefinition[]
    UseLegacyPostgis: bool
    Prepare: bool
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
            if cfg.UseLegacyPostgis then cmd.Connection.TypeMapper.UseLegacyPostgis() |> ignore
            upcast cmd.Connection
        
    let asyncSetupConnection() = 
        async {
            match connection with
            | Choice2Of2 _ -> 
                return setupConnection()
            | Choice1Of2 connectionString -> 
                cmd.Connection <- new NpgsqlConnection(connectionString)
                do! cmd.Connection.OpenAsync() |> Async.AwaitTask
                if cfg.UseLegacyPostgis then cmd.Connection.TypeMapper.UseLegacyPostgis() |> ignore
                return upcast cmd.Connection
        }

    static let listToOption source =  
        match source |> List.truncate 2 with
        | [] -> None
        | [ x ] -> Some x
        | _ -> invalidOp "The output sequence contains more than one element."

    static let buildResultSetsBackingDict (dataPerResultSet: seq<obj>) expectedResultSetCount =
        Seq.zip [ for i in 1 .. expectedResultSetCount do yield sprintf "ResultSet%d" i ] dataPerResultSet |> Map.ofSeq

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
                if isNull resultSet.SeqItemTypeName || isNull (box resultSet.Row2ItemMapping) then
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
                            
                    executeHandle.Invoke(null, [| resultSet.Row2ItemMapping |]) |> unbox >> box, 
                    asyncExecuteHandle.Invoke(null, [| resultSet.Row2ItemMapping |]) |> unbox >> box
            | _ ->
                ``ISqlCommand Implementation``.ExecuteMulti >> box,
                ``ISqlCommand Implementation``.AsyncExecuteMulti >> box
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
            return cursor
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
                ``ISqlCommand Implementation``.VerifyOutputColumns(cursor, resultSet.ExpectedColumns)
                ``ISqlCommand Implementation``.LoadDataTable cursor (cmd.Clone()) resultSet.ExpectedColumns |> box)

        return buildResultSetsBackingDict results resultSets.Length }

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
                ``ISqlCommand Implementation``.VerifyOutputColumns(cursor, resultSet.ExpectedColumns)
                ``ISqlCommand Implementation``.LoadDataTable cursor (cmd.Clone()) resultSet.ExpectedColumns |> box)

        buildResultSetsBackingDict results resultSets.Length |> box

    static member internal ExecuteDataTable(cmd, setupConnection, readerBehavior, parameters, resultSets, prepare) = 
        use cursor = ``ISqlCommand Implementation``.ExecuteReader(cmd, setupConnection, readerBehavior, parameters, resultSets, prepare) 
        ``ISqlCommand Implementation``.LoadDataTable cursor (cmd.Clone()) resultSets.[0].ExpectedColumns

    static member internal AsyncExecuteDataTable(cmd, setupConnection, readerBehavior, parameters, resultSets, prepare) = 
        async {
            use! reader = ``ISqlCommand Implementation``.AsyncExecuteReader(cmd, setupConnection, readerBehavior, parameters, resultSets, prepare) 
            return ``ISqlCommand Implementation``.LoadDataTable reader (cmd.Clone()) resultSets.[0].ExpectedColumns
        }

    static member internal ExecuteList<'TItem> (rowMapper) = fun(cmd: NpgsqlCommand, setupConnection, readerBehavior, parameters, resultSetDefinitions: ResultSetDefinition[], prepare) -> 
        let hasOutputParameters = cmd.Parameters |> Seq.cast<NpgsqlParameter> |> Seq.exists (fun x -> x.Direction.HasFlag( ParameterDirection.Output))

        if not hasOutputParameters
        then
            use reader = ``ISqlCommand Implementation``.ExecuteReader(cmd, setupConnection, readerBehavior, parameters, resultSetDefinitions, prepare)
            let xs = reader.MapRowValues<'TItem>(rowMapper) |> Seq.toList

            if readerBehavior.HasFlag(CommandBehavior.SingleRow)
            then 
                xs |> listToOption |> box
            else 
                box xs 
        else
            use reader = ``ISqlCommand Implementation``.ExecuteReader(cmd, setupConnection, readerBehavior, parameters, resultSetDefinitions, prepare)
            let resultset = reader.MapRowValues<'TItem>(rowMapper) |> Seq.toList

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
    static member internal ExecuteSingle<'TItem> (reader: NpgsqlDataReader, readerBehavior: CommandBehavior, resultSetDefinition) = 
        let xs = reader.MapRowValues<'TItem>(resultSetDefinition.Row2ItemMapping) |> Seq.toList

        if readerBehavior.HasFlag(CommandBehavior.SingleRow)
        then 
            xs |> listToOption |> box
        else 
            box xs 
            
    static member internal AsyncExecuteList<'TItem> (rowMapper) = fun(cmd, setupConnection, readerBehavior, parameters, resultSetDefinitions, prepare) ->
        let xs = 
            async {
                use! reader = ``ISqlCommand Implementation``.AsyncExecuteReader(cmd, setupConnection, readerBehavior, parameters, resultSetDefinitions, prepare)
                return reader.MapRowValues<'TItem>(rowMapper) |> Seq.toList
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

    static member private ReadResultSet (cursor: Common.DbDataReader) readerBehavior resultSetDefinition =
        // Is this a result set for a non-query?
        if isNull resultSetDefinition.SeqItemTypeName || isNull (box resultSetDefinition.Row2ItemMapping) then
            box cursor.RecordsAffected
        else
            ``ISqlCommand Implementation``.VerifyOutputColumns(cursor, resultSetDefinition.ExpectedColumns)
            let itemType = Type.GetType(resultSetDefinition.SeqItemTypeName, throwOnError = true)
            
            let executeHandle = 
                typeof<``ISqlCommand Implementation``>
                    .GetMethod("ExecuteSingle", BindingFlags.NonPublic ||| BindingFlags.Static)
                    .MakeGenericMethod(itemType)
                    
            executeHandle.Invoke(null, [| cursor; readerBehavior; resultSetDefinition |])

    static member internal ExecuteMulti (cmd, setupConnection, readerBehavior, parameters, resultSets: ResultSetDefinition[], prepare) =
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
        setupConnection() |> ignore

        if prepare then
            cmd.Prepare()

        use cursor = cmd.ExecuteReader(readerBehavior)

        let mutable currentResultSet = 0
        let results = ResizeArray<obj>()
        let mutable go = true

        while go do
            results.Add(``ISqlCommand Implementation``.ReadResultSet cursor readerBehavior resultSets.[currentResultSet])
            currentResultSet <- currentResultSet + 1
            go <- cursor.NextResult()

        buildResultSetsBackingDict results resultSets.Length |> box

    static member internal AsyncExecuteMulti (cmd, setupConnection, readerBehavior: CommandBehavior, parameters, resultSets: ResultSetDefinition[], prepare) = async {
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
        do! setupConnection() |> Async.Ignore

        if prepare then
            do! cmd.PrepareAsync() |> Async.AwaitTask

        use! cursor = cmd.ExecuteReaderAsync(readerBehavior) |> Async.AwaitTask

        let mutable currentResultSet = 0
        let results = ResizeArray<obj>()
        let mutable go = true

        while go do
            results.Add(``ISqlCommand Implementation``.ReadResultSet cursor readerBehavior resultSets.[currentResultSet])
            currentResultSet <- currentResultSet + 1
            let! more = cursor.NextResultAsync() |> Async.AwaitTask
            go <- more

        return buildResultSetsBackingDict results resultSets.Length |> box }

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



