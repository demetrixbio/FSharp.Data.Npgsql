﻿namespace FSharp.Data.Npgsql

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
    Row2ItemMapping: (obj[] -> obj)
    SeqItemTypeName: string
    ExpectedColumns: DataColumn[]
    UseLegacyPostgis: bool
}

[<Sealed>]
[<EditorBrowsable(EditorBrowsableState.Never)>]
type ``ISqlCommand Implementation``(cfg: DesignTimeConfig, connection, commandTimeout) = 

    let cmd = new NpgsqlCommand(cfg.SqlStatement, CommandTimeout = commandTimeout)
    do
        cmd.Parameters.AddRange( cfg.Parameters)

    let readerBehavior = 
        CommandBehavior.SingleResult
        ||| if cfg.SingleRow then CommandBehavior.SingleRow else CommandBehavior.Default
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

    static let seqToOption source =  
        match source |> Seq.truncate 2 |> Seq.toArray with
        | [||] -> None
        | [| x |] -> Some x
        | _ -> invalidOp "The output sequence contains more than one element."

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
                ``ISqlCommand Implementation``.ExecuteNonQuery setupConnection >> box, 
                ``ISqlCommand Implementation``.AsyncExecuteNonQuery asyncSetupConnection >> box
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
                        
                executeHandle.Invoke(null, [| rowMapping |]) |> unbox >> box, 
                asyncExecuteHandle.Invoke(null, [| rowMapping |]) |> unbox >> box

        | unexpected -> failwithf "Unexpected ResultType value: %O" unexpected

    member __.CommandTimeout = cmd.CommandTimeout

    interface ISqlCommand with

        member __.Execute parameters = execute(cmd, setupConnection, readerBehavior, parameters, cfg.ExpectedColumns)
        member __.AsyncExecute parameters = asyncExecute(cmd, asyncSetupConnection, readerBehavior, parameters, cfg.ExpectedColumns)

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

    static member internal VerifyOutputColumns(cursor: NpgsqlDataReader, expectedColumns: DataColumn[]) = 
        if  cursor.FieldCount < expectedColumns.Length
        then 
            let message = sprintf "Expected at least %i columns in result set but received only %i." expectedColumns.Length cursor.FieldCount
            cursor.Close()
            invalidOp message
        
        //https://stackoverflow.com/questions/2253437/npgsqldatareader-getordinal-throwing-exceptions-any-way-around
        let actualRow = Dictionary<string, Type>()
        for i = 0 to cursor.FieldCount - 1 do 
            actualRow.Add(cursor.GetName(i), cursor.GetFieldType(i))

        for i = 0 to expectedColumns.Length - 1 do
            let expectedName, expectedType = expectedColumns.[i].ColumnName, expectedColumns.[i].DataType

            let (isColumnPresent, actualType) = actualRow.TryGetValue(expectedName)
            if not isColumnPresent then
                let message = sprintf """Expected column "%s" of type "%A" was not found in actual row "%A".""" expectedName expectedType actualRow.Keys
                cursor.Close()
                invalidOp message
            else
                //TO DO: add extended property on column to mark enums
                let maybeEnum = expectedType = typeof<string> && actualType = typeof<obj>
                let maybeArray = expectedType.IsArray && actualType = typeof<Array>
                let typeless = expectedType = typeof<obj> && actualType = typeof<string>
                if actualType = expectedType || maybeArray || maybeEnum || typeless then
                    ()
                else
                    let message = sprintf """Expected column "%s" of type "%A" in a row but received column of type "%A".""" expectedName expectedType actualType
                    cursor.Close()
                    invalidOp message

    static member internal ExecuteReader(cmd, setupConnection, readerBehavior, parameters, expectedColumns) = 
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
        setupConnection() |> ignore
        let cursor = cmd.ExecuteReader(readerBehavior)
        ``ISqlCommand Implementation``.VerifyOutputColumns(cursor, expectedColumns)
        cursor

    static member internal AsyncExecuteReader(cmd, setupConnection, readerBehavior: CommandBehavior, parameters, expectedColumns) = 
        async {
            ``ISqlCommand Implementation``.SetParameters(cmd, parameters)
            let! _ = setupConnection() 
            let! cursor = cmd.ExecuteReaderAsync( readerBehavior) |> Async.AwaitTask
            ``ISqlCommand Implementation``.VerifyOutputColumns(downcast cursor, expectedColumns)
            return cursor
        }
    
    static member internal ExecuteDataTable(cmd, setupConnection, readerBehavior, parameters, expectedColumns) = 
        use cursor = ``ISqlCommand Implementation``.ExecuteReader(cmd, setupConnection, readerBehavior, parameters, expectedColumns) 
        let result = new FSharp.Data.Npgsql.DataTable<DataRow>(selectCommand = cmd.Clone())
        result.Columns.AddRange(expectedColumns)
        //result.PrimaryKey <- expectedColumns |> Array.filter (fun c -> unbox c.ExtendedProperties.["IsKey"])
        result.Load(cursor)
        result

    static member internal AsyncExecuteDataTable(cmd, setupConnection, readerBehavior, parameters, expectedColumns) = 
        async {
            use! reader = ``ISqlCommand Implementation``.AsyncExecuteReader(cmd, setupConnection, readerBehavior, parameters, expectedColumns) 
            let result = new FSharp.Data.Npgsql.DataTable<DataRow>(selectCommand = cmd.Clone())
            result.Columns.AddRange(expectedColumns)
            result.Load(reader)
            return result
        }

    static member internal ExecuteSeq<'TItem> (rowMapper) = fun(cmd: NpgsqlCommand, setupConnection, readerBehavior, parameters, expectedColumns) -> 
        let hasOutputParameters = cmd.Parameters |> Seq.cast<NpgsqlParameter> |> Seq.exists (fun x -> x.Direction.HasFlag( ParameterDirection.Output))

        if not hasOutputParameters
        then 
            let xs = Seq.delay <| fun() -> 
                ``ISqlCommand Implementation``
                    .ExecuteReader(cmd, setupConnection, readerBehavior, parameters, expectedColumns)
                    .MapRowValues<'TItem>( rowMapper)

            if readerBehavior.HasFlag(CommandBehavior.SingleRow)
            then 
                xs |> seqToOption |> box
            else 
                box xs 
        else
            let resultset = 
                ``ISqlCommand Implementation``
                    .ExecuteReader(cmd, setupConnection, readerBehavior, parameters, expectedColumns)
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
            
    static member internal AsyncExecuteSeq<'TItem> (rowMapper) = fun(cmd, setupConnection, readerBehavior, parameters, expectedDataReaderColumns) ->
        let xs = 
            async {
                let! reader = ``ISqlCommand Implementation``.AsyncExecuteReader(cmd, setupConnection, readerBehavior, parameters, expectedDataReaderColumns)
                return reader.MapRowValues<'TItem>( rowMapper)
            }

        if readerBehavior.HasFlag(CommandBehavior.SingleRow)
        then
            async {
                let! xs = xs 
                return xs |> seqToOption
            }
            |> box
        else 
            box xs 

    static member internal ExecuteNonQuery setupConnection (cmd, _, _, parameters, _) = 
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)  
        use __ = setupConnection()

        let recordsAffected = cmd.ExecuteNonQuery() 
        for i = 0 to parameters.Length - 1 do
            let name, _ = parameters.[i]
            let p = cmd.Parameters.[name]
            if p.Direction.HasFlag( ParameterDirection.Output)
            then 
                parameters.[i] <- name, p.Value
        recordsAffected

    static member internal AsyncExecuteNonQuery setupConnection (cmd, _, _, parameters, _) = 
        ``ISqlCommand Implementation``.SetParameters(cmd, parameters)  
        async {         
            use! __ = setupConnection()
            return! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask
        }



