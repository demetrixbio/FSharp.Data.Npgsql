namespace FSharp.Data

open System
open System.Data
open Npgsql
open System.Reflection
open FSharp.Data.Runtime
open System.Data.Common

[<CompilerMessageAttribute("This API supports the FSharp.Data.Npgsql infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
type ISqlCommand = 
    
    abstract Execute: parameters: (string * obj)[] -> obj
    abstract AsyncExecute: parameters: (string * obj)[] -> obj

[<CompilerMessageAttribute("This API supports the FSharp.Data.Npgsql infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
[<RequireQualifiedAccess>]
type ResultRank = 
    | Sequence = 0
    | SingleRow = 1
    | ScalarValue = 2

[<CompilerMessageAttribute("This API supports the FSharp.Data.Npgsql infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
type DesignTimeConfig = {
    SqlStatement: string
    IsStoredProcedure: bool 
    Parameters: NpgsqlParameter[]
    ResultType: ResultType
    Rank: ResultRank
    Row2ItemMapping: (obj[] -> obj)
    SeqItemTypeName: string
    ExpectedColumns: (string * string)[]
}

type internal Connection = Choice<string, NpgsqlTransaction>

[<CompilerMessageAttribute("This API supports the FSharp.Data.Npgsql infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
type ``ISqlCommand Implementation``(cfg: DesignTimeConfig, connection: Connection, commandTimeout) = 

    let cmd = new NpgsqlCommand(cfg.SqlStatement, CommandTimeout = commandTimeout)
    let manageConnection = 
        match connection with
        | Choice1Of2 connectionString -> 
            cmd.Connection <- new NpgsqlConnection(connectionString)
            true
        | Choice2Of2 tran -> 
            cmd.Transaction <- tran 
            cmd.Connection <- tran.Connection
            false

    do
        cmd.CommandType <- if cfg.IsStoredProcedure then CommandType.StoredProcedure else CommandType.Text
        cmd.Parameters.AddRange( cfg.Parameters)
        
    let getReaderBehavior() = 
        let mutable behaviour = CommandBehavior.SingleResult
        if cmd.Connection.State <> ConnectionState.Open && manageConnection
        then
            cmd.Connection.Open() 
            behaviour <- behaviour ||| CommandBehavior.CloseConnection

        if cfg.Rank = ResultRank.SingleRow 
        then 
            behaviour <- behaviour ||| CommandBehavior.SingleRow 

        if cfg.ResultType = ResultType.DataTable 
        then 
            behaviour <- behaviour ||| CommandBehavior.KeyInfo 

        behaviour

    static let seqToOption source =  
        match source |> Seq.truncate 2 |> Seq.toArray with
        | [||] -> None
        | [| x |] -> Some x
        | _ -> invalidArg "source" "The input sequence contains more than one element."


    let execute, asyncExecute = 
        match cfg.ResultType with
        | ResultType.DataReader -> 
            ``ISqlCommand Implementation``.ExecuteReader >> box, 
            ``ISqlCommand Implementation``.AsyncExecuteReader >> box
        | ResultType.DataTable ->
            ``ISqlCommand Implementation``.ExecuteDataTable >> box, 
            ``ISqlCommand Implementation``.AsyncExecuteDataTable >> box
        | ResultType.Records | ResultType.Tuples ->
            match box cfg.Row2ItemMapping, cfg.SeqItemTypeName with
            | null, null ->
                ``ISqlCommand Implementation``.ExecuteNonQuery manageConnection >> box, 
                ``ISqlCommand Implementation``.AsyncExecuteNonQuery manageConnection >> box
            | rowMapping, itemTypeName ->
                assert (rowMapping <> null && itemTypeName <> null)
                let itemType = Type.GetType( itemTypeName, throwOnError = true)
                
                let executeHandle = 
                    typeof<``ISqlCommand Implementation``>
                        .GetMethod("ExecuteSeq", BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod(itemType)
                
                let asyncExecuteHandle = 
                    typeof<``ISqlCommand Implementation``>
                        .GetMethod("AsyncExecuteSeq", BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod(itemType)
                        
                executeHandle.Invoke(null, [| cfg.Rank; cfg.Row2ItemMapping |]) |> unbox >> box, 
                asyncExecuteHandle.Invoke(null, [| cfg.Rank; cfg.Row2ItemMapping |]) |> unbox >> box

        | unexpected -> failwithf "Unexpected ResultType value: %O" unexpected

    member this.CommandTimeout = cmd.CommandTimeout

    interface ISqlCommand with

        member this.Execute parameters = execute(cmd, getReaderBehavior, parameters, cfg.ExpectedColumns)
        member this.AsyncExecute parameters = asyncExecute(cmd, getReaderBehavior, parameters, cfg.ExpectedColumns)

    interface IDisposable with
        member this.Dispose() =
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

    static member internal VerifyResultsetColumns(cursor: DbDataReader, expected) = 
        if Configuration.Current.ResultsetRuntimeVerification
        then 
            if cursor.FieldCount < Array.length expected
            then 
                let message = sprintf "Expected at least %i columns in result set but received only %i." expected.Length cursor.FieldCount
                cursor.Close()
                invalidOp message

            for i = 0 to expected.Length - 1 do
                let expectedName, expectedType = fst expected.[i], Type.GetType( snd expected.[i], throwOnError = true)
                let actualName, actualType = cursor.GetName( i), cursor.GetFieldType( i)
                if actualName <> expectedName || actualType <> expectedType
                then 
                    let message = sprintf """Expected column [%s] of type "%A" at position %i (0-based indexing) but received column [%s] of type "%A".""" expectedName expectedType i actualName actualType
                    cursor.Close()
                    invalidOp message

    static member internal ExecuteReader(cmd, getReaderBehavior, parameters, expectedDataReaderColumns) = 
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
        let cursor = cmd.ExecuteReader( getReaderBehavior())
        ``ISqlCommand Implementation``.VerifyResultsetColumns(cursor, expectedDataReaderColumns)
        cursor

    static member internal AsyncExecuteReader(cmd, getReaderBehavior, parameters, expectedDataReaderColumns) = 
        async {
            ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
            let! cursor = cmd.ExecuteReaderAsync( getReaderBehavior(): CommandBehavior) |> Async.AwaitTask
            ``ISqlCommand Implementation``.VerifyResultsetColumns(cursor, expectedDataReaderColumns)
            return cursor
        }
    
    static member internal ExecuteDataTable(cmd, getReaderBehavior, parameters, expectedDataReaderColumns) = 
        use cursor = ``ISqlCommand Implementation``.ExecuteReader(cmd, getReaderBehavior, parameters, expectedDataReaderColumns) 
        let result = new FSharp.Data.DataTable<DataRow>(cmd)
        for c in cursor.GetColumnSchema() do
            let x = result.Columns.Add( c.ColumnName, c.DataType)
            x.AllowDBNull <- c.AllowDBNull.GetValueOrDefault(true)
            if c.DataTypeName = "timestamptz" && c.DataType = typeof<DateTime>
            then 
                x.DateTimeMode <- Data.DataSetDateTime.Local

        result.Load(cursor)
        result

    static member internal AsyncExecuteDataTable(cmd, getReaderBehavior, parameters, expectedDataReaderColumns) = 
        async {
            use! reader = ``ISqlCommand Implementation``.AsyncExecuteReader(cmd, getReaderBehavior, parameters, expectedDataReaderColumns) 
            let result = new FSharp.Data.DataTable<DataRow>(cmd)
            result.Load(reader)
            return result
        }

    static member internal ExecuteSeq<'TItem> (rank, rowMapper) = fun(cmd: NpgsqlCommand, getReaderBehavior, parameters, expectedDataReaderColumns) -> 
        let hasOutputParameters = cmd.Parameters |> Seq.cast<NpgsqlParameter> |> Seq.exists (fun x -> x.Direction.HasFlag( ParameterDirection.Output))

        if not hasOutputParameters
        then 
            let xs = Seq.delay <| fun() -> 
                ``ISqlCommand Implementation``
                    .ExecuteReader(cmd, getReaderBehavior, parameters, expectedDataReaderColumns)
                    .MapRowValues<'TItem>( rowMapper)

            if rank = ResultRank.SingleRow 
            then 
                xs |> seqToOption |> box
            elif rank = ResultRank.ScalarValue 
            then 
                xs |> Seq.exactlyOne |> box
            else 
                assert (rank = ResultRank.Sequence)
                box xs 
        else
            let resultset = 
                ``ISqlCommand Implementation``
                    .ExecuteReader(cmd, getReaderBehavior, parameters, expectedDataReaderColumns)
                    .MapRowValues<'TItem>( rowMapper)
                    |> Seq.toList

            if hasOutputParameters
            then
                for i = 0 to parameters.Length - 1 do
                    let name, _ = parameters.[i]
                    let p = cmd.Parameters.[name]
                    if p.Direction.HasFlag( ParameterDirection.Output)
                    then 
                        parameters.[i] <- name, p.Value

            box resultset
            
    static member internal AsyncExecuteSeq<'TItem> (rank, rowMapper) = fun(cmd, getReaderBehavior, parameters, expectedDataReaderColumns) ->
        let xs = 
            async {
                let! reader = ``ISqlCommand Implementation``.AsyncExecuteReader(cmd, getReaderBehavior, parameters, expectedDataReaderColumns)
                return reader.MapRowValues<'TItem>( rowMapper)
            }

        if rank = ResultRank.SingleRow
        then
            async {
                let! xs = xs 
                return xs |> seqToOption
            }
            |> box
        elif rank = ResultRank.ScalarValue 
        then 
            async {
                let! xs = xs 
                return xs |> Seq.exactlyOne
            }
            |> box       
        else 
            assert (rank = ResultRank.Sequence)
            box xs 

    static member internal ExecuteNonQuery manageConnection (cmd, _, parameters, _) = 
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)  
        try
            if manageConnection 
            then cmd.Connection.Open()

            let recordsAffected = cmd.ExecuteNonQuery() 
            for i = 0 to parameters.Length - 1 do
                let name, _ = parameters.[i]
                let p = cmd.Parameters.[name]
                if p.Direction.HasFlag( ParameterDirection.Output)
                then 
                    parameters.[i] <- name, p.Value
            recordsAffected
        finally
            if manageConnection 
            then cmd.Connection.Close()

    static member internal AsyncExecuteNonQuery manageConnection (cmd, _, parameters, _) = 
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)  
        async {         
            try 
                if manageConnection 
                then 
                    do! cmd.Connection.OpenAsync() |> Async.AwaitTask
                return! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask
            finally
                if manageConnection 
                then cmd.Connection.Close()
        }



