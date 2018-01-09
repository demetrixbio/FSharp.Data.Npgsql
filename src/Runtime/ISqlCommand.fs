namespace FSharp.Data

open System
open System.Data
open Npgsql
open System.Data.Common
open System.Reflection

///<summary>Enum describing output type</summary>
type ResultType =
///<summary>Sequence of custom records with properties matching column names and types</summary>
    | Records = 0
///<summary>Sequence of tuples matching column types with the same order</summary>
    | Tuples = 1
///<summary>Typed DataTable <see cref='T:FSharp.Data.DataTable`1'/></summary>
    | DataTable = 2
///<summary>raw DataReader</summary>
    | DataReader = 3

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
    ExpectedColumns: DataColumn[]
}

type internal Connection = Choice<string, NpgsqlTransaction>

[<AutoOpen>]
[<CompilerMessageAttribute("This API supports the FSharp.Data.Npgsql infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
module Extensions =
    type internal DbDataReader with
        member this.MapRowValues<'TItem>( rowMapping) = 
            seq {
                use _ = this
                let values = Array.zeroCreate this.FieldCount
                while this.Read() do
                    this.GetValues(values) |> ignore
                    yield values |> rowMapping |> unbox<'TItem>
            }

    let DbNull = box DBNull.Value

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

    static member internal OptionToObj<'T> value = <@@ match %%value with Some (x : 'T) -> box x | None -> DbNull @@>    

    static member SetRef<'t>(r : byref<'t>, arr: (string * obj)[], i) = r <- arr.[i] |> snd |> unbox

    static member GetMapperWithNullsToOptions(nullsToOptions, mapper: obj[] -> obj) = 
        fun values -> 
            nullsToOptions values
            mapper values


//Execute/AsyncExecute versions

    static member internal VerifyOutputColumns(cursor: NpgsqlDataReader, expectedColumns: DataColumn[]) = 
        let verificationRequested = Array.length expectedColumns > 0
        if verificationRequested
        then 
            if  cursor.FieldCount < expectedColumns.Length
            then 
                let message = sprintf "Expected at least %i columns in result set but received only %i." expectedColumns.Length cursor.FieldCount
                cursor.Close()
                invalidOp message

            for i = 0 to expectedColumns.Length - 1 do
                let expectedName, expectedType = expectedColumns.[i].ColumnName, expectedColumns.[i].DataType
                let actualName, actualType = cursor.GetName( i), cursor.GetFieldType( i)
                let maybeEnum = 
                    (expectedType = typeof<string> && actualType = typeof<obj>)
                    || (expectedType = typeof<string[]> && actualType = typeof<Array>)
                let typeless = expectedType = typeof<obj> && actualType = typeof<string>
                if (expectedName <> "" && actualName <> expectedName) 
                    || (actualType <> expectedType && not maybeEnum && not typeless)
                then 
                    let message = sprintf """Expected column "%s" of type "%A" at position %i (0-based indexing) but received column "%s" of type "%A".""" expectedName expectedType i actualName actualType
                    cursor.Close()
                    invalidOp message

    static member internal ExecuteReader(cmd, getReaderBehavior, parameters, expectedColumns) = 
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
        let cursor = cmd.ExecuteReader( getReaderBehavior())
        ``ISqlCommand Implementation``.VerifyOutputColumns(cursor, expectedColumns)
        cursor

    static member internal AsyncExecuteReader(cmd, getReaderBehavior, parameters, expectedColumns) = 
        async {
            ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
            let! cursor = cmd.ExecuteReaderAsync( getReaderBehavior(): CommandBehavior) |> Async.AwaitTask
            ``ISqlCommand Implementation``.VerifyOutputColumns(downcast cursor, expectedColumns)
            return cursor
        }
    
    static member internal ExecuteDataTable(cmd, getReaderBehavior, parameters, expectedColumns) = 
        use cursor = ``ISqlCommand Implementation``.ExecuteReader(cmd, getReaderBehavior, parameters, expectedColumns) 
        let result = new FSharp.Data.DataTable<DataRow>(cmd)
        result.Columns.AddRange(expectedColumns)
        result.Load(cursor)
        result

    static member internal AsyncExecuteDataTable(cmd, getReaderBehavior, parameters, expectedColumns) = 
        async {
            use! reader = ``ISqlCommand Implementation``.AsyncExecuteReader(cmd, getReaderBehavior, parameters, expectedColumns) 
            let result = new FSharp.Data.DataTable<DataRow>(cmd)
            result.Load(reader)
            return result
        }

    static member internal ExecuteSeq<'TItem> (rank, rowMapper) = fun(cmd: NpgsqlCommand, getReaderBehavior, parameters, expectedColumns) -> 
        let hasOutputParameters = cmd.Parameters |> Seq.cast<NpgsqlParameter> |> Seq.exists (fun x -> x.Direction.HasFlag( ParameterDirection.Output))

        if not hasOutputParameters
        then 
            let xs = Seq.delay <| fun() -> 
                ``ISqlCommand Implementation``
                    .ExecuteReader(cmd, getReaderBehavior, parameters, expectedColumns)
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
                    .ExecuteReader(cmd, getReaderBehavior, parameters, expectedColumns)
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
                then do! cmd.Connection.OpenAsync() |> Async.AwaitTask
                return! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask
            finally
                if manageConnection 
                then cmd.Connection.Close()
        }



