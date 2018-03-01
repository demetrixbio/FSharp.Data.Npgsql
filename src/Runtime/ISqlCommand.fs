namespace FSharp.Data.Npgsql

open System
open System.Data
open Npgsql
open System.Data.Common
open System.Reflection
open System.Runtime.CompilerServices

[<CompilerMessageAttribute(Const.infraMessage, 101, IsHidden = true)>]
type ISqlCommand = 
    abstract Execute: parameters: (string * obj)[] -> obj
    abstract AsyncExecute: parameters: (string * obj)[] -> obj

[<CompilerMessageAttribute(Const.infraMessage, 101, IsHidden = true)>]
type DesignTimeConfig = {
    SqlStatement: string
    Parameters: NpgsqlParameter[]
    ResultType: ResultType
    SingleRow: bool
    Row2ItemMapping: (obj[] -> obj)
    SeqItemTypeName: string
    ExpectedColumns: DataColumn[]
}

type internal CommandBuilder(source: DataTable<DataRow>) = 
    inherit DbCommandBuilder(QuotePrefix = "\"", QuoteSuffix = "\"") 

    let npgsql = new NpgsqlCommandBuilder()  
    let rowUpdatingCleanUp = ref null

    let schemaTable = 
        use reader = new DataTableReader(source)
        reader.GetSchemaTable()

    override __.ApplyParameterInfo(p, row, _, _) = 
        //let param: NpgsqlParameter = downcast p
        //param.NpgsqlDbType <- unbox row.[SchemaTableColumn.ProviderType]
        ()

    override __.GetParameterName parameterName = sprintf "@%s" parameterName
    override __.GetParameterName parameterOrdinal = sprintf "@p%i" parameterOrdinal
    override this.GetParameterPlaceholder parameterOrdinal = this.GetParameterName(parameterOrdinal)
    override __.QuoteIdentifier unquotedIdentifier = npgsql.QuoteIdentifier unquotedIdentifier
    override __.UnquoteIdentifier quotedIdentifier = npgsql.UnquoteIdentifier quotedIdentifier
                
    member private __.SqlRowUpdatingHandler eventArgs = base.RowUpdatingHandler(eventArgs)

    override this.SetRowUpdatingHandler adapter = 
        if (adapter <> this.DataAdapter)
        then
            rowUpdatingCleanUp := (adapter :?> NpgsqlDataAdapter).RowUpdating.Subscribe(this.SqlRowUpdatingHandler)
        else
            rowUpdatingCleanUp.Value.Dispose()

    override __.GetSchemaTable(cmd) = schemaTable

[<Extension>]
[<CompilerMessageAttribute(Const.infraMessage, 101, IsHidden = true)>]
type Utils private() =

    [<Extension>]
    [<CompilerMessageAttribute(Const.infraMessage, 101, IsHidden = true)>]
    static member MapRowValues<'TItem>(cursor: DbDataReader ,rowMapping) = 
        seq {
            use _ = cursor
            let values = Array.zeroCreate cursor.FieldCount
            while cursor.Read() do
                cursor.GetValues(values) |> ignore
                yield values |> rowMapping |> unbox<'TItem>
        }

    [<CompilerMessageAttribute(Const.infraMessage, 101, IsHidden = true)>]
    static member DbNull = box DBNull.Value

    [<CompilerMessageAttribute(Const.infraMessage, 101, IsHidden = true)>]
    static member GetMapperWithNullsToOptions(nullsToOptions, mapper: obj[] -> obj) = 
        fun values -> 
            nullsToOptions values
            mapper values

    [<CompilerMessageAttribute(Const.infraMessage, 101, IsHidden = true)>]
    static member SetRef<'t>(r : byref<'t>, arr: (string * obj)[], i) = r <- arr.[i] |> snd |> unbox

    [<CompilerMessageAttribute(Const.infraMessage, 101, IsHidden = true)>]
    static member UpdateDataTable(table: DataTable<DataRow>, connection, transaction, updateBatchSize, continueUpdateOnError, conflictOption) = 

        let selectCommand = table.SelectCommand

        if connection <> null then selectCommand.Connection <- connection
        if transaction <> null then selectCommand.Transaction <- transaction

        use dataAdapter = new NpgsqlDataAdapter(selectCommand, UpdateBatchSize = updateBatchSize, ContinueUpdateOnError = continueUpdateOnError)

        use commandBuilder = new CommandBuilder(table, DataAdapter = dataAdapter, ConflictOption = conflictOption)

        dataAdapter.InsertCommand <- downcast commandBuilder.GetInsertCommand()
        dataAdapter.DeleteCommand <- downcast commandBuilder.GetDeleteCommand()
        dataAdapter.UpdateCommand <- downcast commandBuilder.GetUpdateCommand()

        use __ = dataAdapter.RowUpdating.Subscribe(fun args ->

            if  args.Errors = null 
                && args.StatementType = Data.StatementType.Insert 
                && dataAdapter.UpdateBatchSize = 1
            then 
                let columnsToRefresh = ResizeArray()
                for c in table.Columns do
                    if c.AutoIncrement  
                        || (c.AllowDBNull && args.Row.IsNull c.Ordinal)
                    then 
                        columnsToRefresh.Add( commandBuilder.QuoteIdentifier c.ColumnName)

                if columnsToRefresh.Count > 0
                then                        
                    let returningClause = columnsToRefresh |> String.concat "," |> sprintf " RETURNING %s"
                    let cmd = args.Command
                    cmd.CommandText <- cmd.CommandText + returningClause
                    cmd.UpdatedRowSource <- UpdateRowSource.FirstReturnedRecord
        )

        dataAdapter.Update(table)   

[<Sealed>]
[<CompilerMessageAttribute(Const.infraMessage, 101, IsHidden = true)>]
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
            upcast cmd.Connection
        
    let asyncSetupConnection() = 
        async {
            match connection with
            | Choice2Of2 _ -> 
                return setupConnection()
            | Choice1Of2 connectionString -> 
                cmd.Connection <- new NpgsqlConnection(connectionString)
                do! cmd.Connection.OpenAsync() |> Async.AwaitTask
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

        for i = 0 to expectedColumns.Length - 1 do
            let expectedName, expectedType = expectedColumns.[i].ColumnName, expectedColumns.[i].DataType
            let actualName, actualType = cursor.GetName( i), cursor.GetFieldType( i)
                
            //TO DO: add extended property on column to mark enums
            let maybeEnum = expectedType = typeof<string> && actualType = typeof<obj>
            let maybeArray = expectedType.IsArray && actualType = typeof<Array>
            let typeless = expectedType = typeof<obj> && actualType = typeof<string>
            if (expectedName <> "" && actualName <> expectedName) 
                || (actualType <> expectedType && not (maybeArray || maybeEnum) && not typeless)
            then 
                let message = sprintf """Expected column "%s" of type "%A" at position %i (0-based indexing) but received column "%s" of type "%A".""" expectedName expectedType i actualName actualType
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



