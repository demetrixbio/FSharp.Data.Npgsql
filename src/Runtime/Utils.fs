namespace FSharp.Data.Npgsql

open System
open System.Data
open System.Data.Common
open System.Collections.Concurrent
open System.ComponentModel
open System.Threading
open Npgsql
open NpgsqlTypes
open FSharp.Control.Tasks.NonAffine
open System.Linq.Expressions

#nowarn "0025"

[<AutoOpen>]
module internal StringExtensions =

    type String with
        member this.ErrorClass =
            if this.Length >= 2
            then this.Substring 2
            else raise (InvalidOperationException ())

[<EditorBrowsable(EditorBrowsableState.Never)>]
type Utils () =

    static let ShouldRetry (tries, retries, exn : Exception) =
        let exceptionRetry =
            match exn with
            | :? PostgresException as pgexn ->
                let sqlState = pgexn.SqlState
                let errorClass = sqlState.ErrorClass
                if errorClass = PostgresErrorCodes.ConnectionException.ErrorClass then true
                elif errorClass = PostgresErrorCodes.InsufficientResources.ErrorClass then true
                elif sqlState = PostgresErrorCodes.IoError then true
                elif sqlState = PostgresErrorCodes.DeadlockDetected then true
                elif sqlState = PostgresErrorCodes.LockNotAvailable then true
                elif sqlState = PostgresErrorCodes.TransactionIntegrityConstraintViolation then true
                elif sqlState = PostgresErrorCodes.InFailedSqlTransaction then true
                else false
            | :? NpgsqlException -> true
            | _ -> false
        if exceptionRetry then
            retries < 1 ||
            tries < retries
        else false

    static let rec SetupConnectionAsync' (tries, exns, retries, wait, cmd: NpgsqlCommand, connection) =
        async {
            match connection with
            | Choice1Of2 connectionString ->
                cmd.Connection <- new NpgsqlConnection (connectionString)
                let! choice = cmd.Connection.OpenAsync () |> Async.AwaitTask |> Async.Catch
                match choice with
                | Choice1Of2 () -> ()
                | Choice2Of2 exn ->
                    if ShouldRetry (tries, retries, exn) &&
                       (cmd.Connection.State &&& ConnectionState.Open = ConnectionState.Open) then
                        do! Async.Sleep wait
                        do! SetupConnectionAsync' (tries+1, exn :: exns, retries, wait, cmd, connection)
                    else raise (AggregateException (Seq.rev exns))
            | Choice2Of2 (conn, tx) ->
                cmd.Connection <- conn
                cmd.Transaction <- tx }

    static let rec Read' (tries, exns, retries, wait: int, cursor: DbDataReader) =
        try cursor.Read ()
        with exn ->
            if ShouldRetry (tries, retries, exn) then
                Thread.Sleep wait
                Read' (tries+1, exn :: exns, retries, wait, cursor)
            else
                raise (AggregateException (Seq.rev exns))

    static let rec ReadAsync' (tries, exns, retries, wait, cursor: DbDataReader) =
        async {
            let! choice = cursor.ReadAsync () |> Async.AwaitTask |> Async.Catch
            match choice with
            | Choice1Of2 go -> return go
            | Choice2Of2 exn ->
                if ShouldRetry (tries, retries, exn) then
                    do! Async.Sleep wait
                    return! ReadAsync' (tries+1, exn :: exns, retries, wait, cursor)
                else
                    return (raise (AggregateException (Seq.rev exns))) }

    static let rec NextResultAsync' (tries, exns, retries, wait, cursor: DbDataReader) =
        async {
            let! choice = cursor.NextResultAsync () |> Async.AwaitTask |> Async.Catch
            match choice with
            | Choice1Of2 go -> return go
            | Choice2Of2 exn ->
                if ShouldRetry (tries, retries, exn) then
                    do! Async.Sleep wait
                    return! NextResultAsync' (tries+1, exn :: exns, retries, wait, cursor)
                else
                    return (raise (AggregateException (Seq.rev exns))) }

    static let rec PrepareAsync' (tries, exns, retries, wait, cmd: NpgsqlCommand) =
        async {
            let! choice = cmd.PrepareAsync () |> Async.AwaitTask |> Async.Catch
            match choice with
            | Choice1Of2 () -> return ()
            | Choice2Of2 exn ->
                if  ShouldRetry (tries, retries, exn) &&
                    (cmd.Connection.State &&& ConnectionState.Open = ConnectionState.Open) then
                    do! Async.Sleep wait
                    return! PrepareAsync' (tries+1, exn :: exns, retries, wait, cmd)
                else
                    return (raise (AggregateException (Seq.rev exns))) }

    static let rec ExecuteReaderAsync' (tries, exns, retries, wait, behavior: CommandBehavior, cmd: NpgsqlCommand) =
        async {
            let! choice = cmd.ExecuteReaderAsync behavior |> Async.AwaitTask |> Async.Catch
            match choice with
            | Choice1Of2 task -> return task
            | Choice2Of2 exn ->
                if  ShouldRetry (tries, retries, exn) &&
                    (cmd.Connection.State &&& ConnectionState.Open = ConnectionState.Open) then
                    do! Async.Sleep wait
                    return! ExecuteReaderAsync' (tries+1, exn :: exns, retries, wait, behavior, cmd)
                else
                    return (raise (AggregateException (Seq.rev exns))) }

    static let rec ExecuteNonQueryAsync' (tries, exns, retries, wait, cmd: NpgsqlCommand) =
        async {
            let! choice = cmd.ExecuteNonQueryAsync () |> Async.AwaitTask |> Async.Catch
            match choice with
            | Choice1Of2 rowsAffected -> return rowsAffected
            | Choice2Of2 exn ->
                if  ShouldRetry (tries, retries, exn) &&
                    (cmd.Connection.State &&& ConnectionState.Open = ConnectionState.Open) then
                    do! Async.Sleep wait
                    return! ExecuteNonQueryAsync' (tries+1, exn :: exns, retries, wait, cmd)
                else
                    return (raise (AggregateException (Seq.rev exns))) }

    static let getColumnMapping =
        let cache = ConcurrentDictionary<Type, obj -> obj> ()
        let factory = Func<_, _>(fun (typeParam: Type) ->
            // PreComputeUnionConstructor returns obj[] -> obj, which would require an extra array allocation when creating option values
            // Since we're only interested in Some (one argument) here, the array can be left out
            let param = Expression.Parameter typeof<obj>
            let expr =
                Expression.Lambda<Func<obj, obj>> (
                    Expression.Condition (
                        Expression.Call (typeof<Convert>.GetMethod (nameof Convert.IsDBNull, Reflection.BindingFlags.Static ||| Reflection.BindingFlags.Public), param),
                        Expression.Constant (null, typedefof<_ option>.MakeGenericType typeParam),
                        Expression.New (
                            typedefof<_ option>.MakeGenericType(typeParam).GetConstructor [| typeParam |],
                            Expression.Convert (param, typeParam)
                        )
                    ),
                    param)

            expr.Compile().Invoke)

        fun (x: DataColumn) -> if x.AllowDBNull then cache.GetOrAdd (x.DataType, factory) else id

    static let getRowToTupleReader =
        let cache = ConcurrentDictionary<int, Func<DbDataReader, obj>> ()
        let rec constituentTuple (t: Type) (columns: (int * DataColumn)[]) param startIndex: Expression =
            let genericArgs = t.GetGenericArguments ()
            Expression.New (
                t.GetConstructor genericArgs,
                [ 
                    for paramIndex in 0 .. genericArgs.Length - 1 do
                        let genericArg = genericArgs.[paramIndex]

                        if paramIndex = 7 then
                            constituentTuple genericArg columns param (startIndex + 7)
                        else
                            let i, c = columns.[paramIndex + startIndex]
                            let v = Expression.Call (param, typeof<DbDataReader>.GetMethod("GetFieldValue").MakeGenericMethod c.DataType, Expression.Constant i)

                            if c.AllowDBNull then
                                Expression.Condition (
                                    Expression.Call (param, typeof<DbDataReader>.GetMethod "IsDBNull", Expression.Constant i),
                                    Expression.Constant (null, typedefof<_ option>.MakeGenericType c.DataType),
                                    Expression.New (typedefof<_ option>.MakeGenericType(c.DataType).GetConstructor [| c.DataType |], v))
                            else
                                v
                ]) :> Expression

        fun resultSet sortColumns ->
            let mutable func = Unchecked.defaultof<_>
            if cache.TryGetValue (resultSet.ExpectedColumns.GetHashCode (), &func) then
                func
            else
                let func =
                    let param = Expression.Parameter typeof<DbDataReader>
                    let expr =
                        Expression.Lambda<Func<DbDataReader, obj>> (
                            constituentTuple resultSet.ErasedRowType (resultSet.ExpectedColumns |> Array.indexed |> (if sortColumns then Array.sortBy (fun (_, c) -> c.ColumnName) else id)) param 0,
                            param)

                    expr.Compile ()

                cache.[resultSet.ExpectedColumns.GetHashCode ()] <- func
                func

    static member SetupConnectionAsync (retries, wait, cmd, connection) =
        async {
            do! SetupConnectionAsync' (0, [], retries, wait, cmd, connection) }

    static member Read (retries, wait, cursor) =
        Read' (0, [], retries, wait, cursor)

    static member ReadAsync (retries, wait, cursor) =
        async {
            return! ReadAsync' (0, [], retries, wait, cursor) }

    static member NextResultAsync (retries, wait, cursor) =
        async {
            return! NextResultAsync' (0, [], retries, wait, cursor) }

    static member PrepareAsync (retries, wait, cmd) =
        async {
            return! PrepareAsync' (0, [], retries, wait, cmd) }

    static member ExecuteReaderAsync (retries, wait, behavior, cmd) =
        async {
            return! ExecuteReaderAsync' (0, [], retries, wait, behavior, cmd) }

    static member ExecuteNonQueryAsync (retries, wait, cmd) =
        async {
            return! ExecuteNonQueryAsync' (0, [], retries, wait, cmd) }

    static member ResizeArrayToList ra =
        let rec inner (ra: ResizeArray<'a>, index, acc) = 
            if index = 0 then
                acc
            else
                inner (ra, index - 1, ra.[index - 1] :: acc)

        inner (ra, ra.Count, [])

    static member ResizeArrayToOption (source: ResizeArray<'a>) =  
        match source.Count with
        | 0 -> None
        | 1 -> Some source.[0]
        | _ -> invalidOp "The output sequence contains more than one element."

    static member val GetStatementIndex =
        let mi = typeof<NpgsqlDataReader>.GetProperty("StatementIndex", Reflection.BindingFlags.Instance ||| Reflection.BindingFlags.NonPublic).GetMethod
        Delegate.CreateDelegate (typeof<Func<NpgsqlDataReader, int>>, mi) :?> Func<NpgsqlDataReader, int>

    static member ToSqlParam (name, dbType: NpgsqlDbType, size, scale, precision) = 
        NpgsqlParameter (name, dbType, size, Scale = scale, Precision = precision)

    static member CloneDataColumn (column: DataColumn) =
        let c = new DataColumn (column.ColumnName, column.DataType)
        c.AutoIncrement <- column.AutoIncrement
        c.AllowDBNull <- column.AllowDBNull
        c.ReadOnly <- column.ReadOnly
        c.MaxLength <- column.MaxLength
        c.DateTimeMode <- column.DateTimeMode

        for p in column.ExtendedProperties do
            let p = p :?> System.Collections.DictionaryEntry
            c.ExtendedProperties.Add (p.Key, p.Value)

        c

    static member CreateResultSetDefinition (columns: DataColumn[], resultType) =
        let t =
            match columns with
            | [||] -> typeof<int>
            | [| c |] -> if c.AllowDBNull then typedefof<_ option>.MakeGenericType c.DataType else c.DataType
            | _ ->
                match resultType with
                | ResultType.Records -> Utils.ToTupleType (columns |> Array.sortBy (fun c -> c.ColumnName))
                | ResultType.Tuples -> Utils.ToTupleType columns
                | _ -> null

        { ErasedRowType = t; ExpectedColumns = columns }

    static member GetType typeName = 
        if isNull typeName then
            null
        else
            let t = Type.GetType typeName

            if isNull t then
                let t = Type.GetType (typeName + ", FSharp.Core")

                if isNull t then
                    let t = Type.GetType (typeName + ", Npgsql")

                    if isNull t then
                        Type.GetType (typeName + ", NetTopologySuite", true)
                    else
                        t
                else
                    t
            else
                t

    static member ToDataColumn (stringValues: string, isEnum, autoIncrement, allowDbNull, readonly, maxLength, partOfPk: bool, nullable: bool) =
        let [| columnName; typeName; pgTypeName; baseSchemaName; baseTableName |] = stringValues.Split '|'
        let x = new DataColumn (columnName, Utils.GetType typeName)

        x.AutoIncrement <- autoIncrement
        x.AllowDBNull <- allowDbNull
        x.ReadOnly <- readonly
        x.MaxLength <- maxLength
        
        if pgTypeName = "timestamptz" then
            //https://github.com/npgsql/npgsql/issues/1076#issuecomment-355400785
            x.DateTimeMode <- DataSetDateTime.Local
            //https://www.npgsql.org/doc/types/datetime.html#detailed-behavior-sending-values-to-the-database
            x.ExtendedProperties.Add (SchemaTableColumn.ProviderType, NpgsqlDbType.TimestampTz)
        elif pgTypeName = "timestamp" then
            //https://www.npgsql.org/doc/types/datetime.html#detailed-behavior-sending-values-to-the-database
            x.ExtendedProperties.Add (SchemaTableColumn.ProviderType, NpgsqlDbType.Timestamp)
        elif isEnum then
            // value is an enum and should be sent to npgsql as unknown (auto conversion from string to appropriate enum type)
            x.ExtendedProperties.Add (SchemaTableColumn.ProviderType, NpgsqlDbType.Unknown)
        elif pgTypeName = "json" then
            x.ExtendedProperties.Add (SchemaTableColumn.ProviderType, NpgsqlDbType.Json)
        elif pgTypeName = "jsonb" then
            x.ExtendedProperties.Add (SchemaTableColumn.ProviderType, NpgsqlDbType.Jsonb)
        
        x.ExtendedProperties.Add (SchemaTableColumn.IsKey, partOfPk)
        x.ExtendedProperties.Add (SchemaTableColumn.AllowDBNull, nullable)
        x.ExtendedProperties.Add (SchemaTableColumn.BaseSchemaName, baseSchemaName)
        x.ExtendedProperties.Add (SchemaTableColumn.BaseTableName, baseTableName)
        x

    static member ToTupleType (columns: DataColumn[]) =
        Reflection.FSharpType.MakeTupleType (columns |> Array.map (fun c -> if c.AllowDBNull then typedefof<_ option>.MakeGenericType c.DataType else c.DataType))

    static member ToDataColumnSlim (stringValues: string) =
        let [| columnName; typeName; nullable |] = stringValues.Split '|'
        new DataColumn (columnName, Utils.GetType typeName, AllowDBNull = (nullable = "1"))

    static member MapRowValuesOntoTuple<'TItem> (cursor: DbDataReader, resultType, resultSet) = Unsafe.uply {
        let results = ResizeArray<'TItem> ()
        let rowReader = getRowToTupleReader resultSet (resultType = ResultType.Records)
        
        let! go = Utils.ReadAsync (10, 1000, cursor) (* TODO: pull args from cfg. *)
        let mutable go = go

        while go do
            rowReader.Invoke cursor
            |> unbox
            |> results.Add

            let! cont = Utils.ReadAsync (10, 1000, cursor) (* TODO: pull args from cfg. *)
            go <- cont

        return results }

    static member MapRowValuesOntoTupleLazy<'TItem> (cursor: DbDataReader, resultType, resultSet) =
        seq {
            let rowReader = getRowToTupleReader resultSet (resultType = ResultType.Records)

            while Utils.Read (10, 1000, cursor) do (* TODO: pull args from cfg. *)
                rowReader.Invoke cursor |> unbox<'TItem>
        }

    static member MapRowValues<'TItem> (cursor: DbDataReader, resultType, resultSet: ResultSetDefinition) =
        if resultSet.ExpectedColumns.Length > 1 then
            Utils.MapRowValuesOntoTuple<'TItem> (cursor, resultType, resultSet)
        else Unsafe.uply {
            let columnMapping = getColumnMapping resultSet.ExpectedColumns.[0]
            let results = ResizeArray<'TItem> ()
            
            let! go = Utils.ReadAsync (10, 1000, cursor) (* TODO: pull args from cfg. *)
            let mutable go = go

            while go do
                cursor.GetValue 0
                |> columnMapping
                |> unbox
                |> results.Add

                let! cont = Utils.ReadAsync (10, 1000, cursor) (* TODO: pull args from cfg. *)
                go <- cont

            return results }

    static member MapRowValuesLazy<'TItem> (cursor: DbDataReader, resultSet) =
        seq {
            let columnMapping = getColumnMapping resultSet.ExpectedColumns.[0]

            while Utils.Read (10, 1000, cursor) do (* TODO: pull args from cfg. *)
                cursor.GetValue 0
                |> columnMapping
                |> unbox<'TItem>
        }
    
    static member OptionToObj<'a> (value: obj) =
        match value :?> 'a option with
        | Some x -> box x
        | _ -> box DBNull.Value

    static member GetNullableValueFromDataRow<'a> (row: DataRow, name: string) =
        if row.IsNull name then None else Some (row.[name] :?> 'a)

    static member SetNullableValueInDataRow<'a> (row: DataRow, name: string, value: obj) =
        row.[name] <- Utils.OptionToObj<'a> value

    static member UpdateDataTable(table: DataTable<DataRow>, connection : NpgsqlConnection, transaction, batchSize, continueUpdateOnError, conflictOption, batchTimeout) = 
        if batchSize <= 0 then invalidArg "batchSize" "Batch size has to be larger than 0."
        
        let batchTimeout = if batchTimeout = 0 then connection.CommandTimeout else batchTimeout
        if batchTimeout <= 0 then invalidArg "batchTimeout" "Batch timeout has to be larger than 0."

        use selectCommand = table.SelectCommand.Clone()

        selectCommand.Connection <- connection
        if transaction <> null then selectCommand.Transaction <- transaction

        use dataAdapter = new BatchDataAdapter(selectCommand, batchTimeout, UpdateBatchSize = batchSize, ContinueUpdateOnError = continueUpdateOnError)
        use commandBuilder = new CommandBuilder(table, DataAdapter = dataAdapter, ConflictOption = conflictOption)

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

    static member BinaryImport (table: DataTable<DataRow>, connection: NpgsqlConnection, ignoreIdentityColumns) =
        let columnsToInsert =
            Seq.cast<DataColumn> table.Columns
            |> Seq.indexed
            |> Seq.filter (fun (_, x) -> not ignoreIdentityColumns || not x.AutoIncrement)
            |> Seq.toArray

        let copyFromCommand = 
            columnsToInsert
            |> Array.map (fun (_, x) -> x.ColumnName)
            |> String.concat ", "
            |> sprintf "COPY %s (%s) FROM STDIN (FORMAT BINARY)" table.TableName

        use writer = connection.BeginBinaryImport copyFromCommand

        for row in table.Rows do
            writer.StartRow ()
            for i, _ in columnsToInsert do
                writer.Write row.[i]

        writer.Complete ()

    #if DEBUG
    static member private Udp =
        let c = new System.Net.Sockets.UdpClient ()
        c.Connect ("localhost", 2180)
        c
    static member Log what =
        let b = System.Text.Encoding.UTF8.GetBytes (sprintf "%s - %s" (DateTime.Now.TimeOfDay.ToString "hh':'mm':'ss'.'ff") what)
        Utils.Udp.Send (b, b.Length) |> ignore
    #endif
