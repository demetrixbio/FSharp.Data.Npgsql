module internal FSharp.Data.NpgsqlProvider

open System
open System.Data
open System.Diagnostics
open System.Reflection
open System.Collections.Concurrent
open System.Collections.Generic

open Microsoft.FSharp.Quotations
open ProviderImplementation.ProvidedTypes

open Npgsql
open Npgsql.TypeMapping
open NpgsqlTypes

open InformationSchema

let methodsCache = new ConcurrentDictionary<_, ProvidedMethod>()

let addCreateCommandMethod
    (
        conn: NpgsqlConnection, 
        designTimeConnectionString, 
        rootType: ProvidedTypeDefinition, 
        commands: ProvidedTypeDefinition, 
        customTypes: IDictionary<string, ProvidedTypeDefinition>, 
        tag
    ) = 
        
    let staticParams = [
        ProvidedStaticParameter("CommandText", typeof<string>) 
        ProvidedStaticParameter("ResultType", typeof<ResultType>, ResultType.Records) 
        ProvidedStaticParameter("SingleRow", typeof<bool>, false)   
        ProvidedStaticParameter("NullableParameters", typeof<bool>, false) 
        ProvidedStaticParameter("TypeName", typeof<string>, "") 
    ]
    let m = ProvidedMethod("CreateCommand", [], typeof<obj>, isStatic = true, invokeCode = Unchecked.defaultof<_>)
    m.DefineStaticParameters(staticParams, (fun methodName args ->

        let getMethodImpl () = 

            let sqlStatement, resultType, singleRow, allParametersOptional, typename = args.[0] :?> _ , args.[1] :?> _, args.[2] :?> _, args.[3] :?> _, args.[4] :?> _
            
            if singleRow && not (resultType = ResultType.Records || resultType = ResultType.Tuples)
            then 
                invalidArg "singleRow" "SingleRow can be set only for ResultType.Records or ResultType.Tuples."

            use __ = conn.UseLocally()

            let parameters = DesignTime.ExtractParameters(conn, sqlStatement, allParametersOptional)

            let outputColumns = 
                if resultType <> ResultType.DataReader
                then conn.GetOutputColumns(sqlStatement, CommandType.Text, parameters, ref customTypes)
                else []

            let rank = if singleRow then ResultRank.SingleRow else ResultRank.Sequence
            let returnType = 
                let hasOutputParameters = false
                DesignTime.GetOutputTypes(outputColumns, resultType, rank, hasOutputParameters)

            let commandTypeName = if typename <> "" then typename else methodName.Replace("=", "").Replace("@", "")
            let cmdProvidedType = ProvidedTypeDefinition(commandTypeName, Some typeof<``ISqlCommand Implementation``>, hideObjectMethods = true)

            do  
                cmdProvidedType.AddMember( ProvidedProperty( "Connection", typeof<string>, isStatic = true, getterCode = fun _ -> <@@ tag @@>))

            do  //AsyncExecute, Execute, and ToTraceString

                let executeArgs = DesignTime.GetExecuteArgs(parameters, customTypes)

                let addRedirectToISqlCommandMethod outputType name = 
                    let hasOutputParameters = false
                    DesignTime.AddGeneratedMethod(parameters, hasOutputParameters, executeArgs, cmdProvidedType.BaseType, outputType, name) 
                    |> cmdProvidedType.AddMember

                addRedirectToISqlCommandMethod returnType.Single "Execute" 
                            
                let asyncReturnType = ProvidedTypeBuilder.MakeGenericType(typedefof<_ Async>, [ returnType.Single ])
                addRedirectToISqlCommandMethod asyncReturnType "AsyncExecute" 

            commands.AddMember cmdProvidedType

            if resultType = ResultType.Records 
            then
                returnType.PerRow 
                |> Option.filter (fun x -> x.Provided <> x.ErasedTo)
                |> Option.iter (fun x -> 
                    cmdProvidedType.AddMember x.Provided
                )

            elif resultType = ResultType.DataTable 
            then
                returnType.Single |> cmdProvidedType.AddMember

            let designTimeConfig = 
                let expectedDataReaderColumns = 
                    Expr.NewArray(
                        typeof<string * string>, 
                        [ for c in outputColumns -> Expr.NewTuple [ Expr.Value c.Name; Expr.Value c.ClrType.FullName ] ]
                    )

                <@@ {
                    SqlStatement = sqlStatement
                    IsStoredProcedure = false
                    Parameters = %%Expr.NewArray( typeof<NpgsqlParameter>, parameters |> List.map QuotationsFactory.ToSqlParam)
                    ResultType = resultType
                    Rank = rank
                    Row2ItemMapping = %%returnType.Row2ItemMapping
                    SeqItemTypeName = %%returnType.SeqItemTypeName
                    ExpectedColumns = %%expectedDataReaderColumns
                } @@>


            let ctorsAndFactories = 
                DesignTime.GetCommandCtors(
                    cmdProvidedType, 
                    designTimeConfig, 
                    designTimeConnectionString,
                    factoryMethodName = methodName
                )
            assert (ctorsAndFactories.Length = 4)
            let impl: ProvidedMethod = downcast ctorsAndFactories.[1] 
            rootType.AddMember impl
            impl

        methodsCache.GetOrAdd(methodName, fun _ -> getMethodImpl())
    ))
    rootType.AddMember m

let getTableTypes(conn: NpgsqlConnection, schema, connectionString, tagProvidedType, customTypes: Map<_, ProvidedTypeDefinition list>) = 
    let tables = ProvidedTypeDefinition("Tables", Some typeof<obj>)
    tagProvidedType tables
    tables.AddMembersDelayed <| fun() ->
        use __ = conn.UseLocally()

        conn.GetTables(schema)
        |> List.map (fun (tableName, baseTableName, baseSchemaName, description) -> 

            use builder = new NpgsqlCommandBuilder()
            let twoPartTableName = sprintf "%s.%s" (builder.QuoteIdentifier(schema)) (builder.QuoteIdentifier(tableName))
                
            let cmd = conn.CreateCommand()
            cmd.CommandText <- sprintf """
                SELECT 
                    c.table_schema,
                    c.column_name,
                    c.data_type,
                    c.udt_name,
                    c.is_nullable,
                    c.character_maximum_length,
                    c.is_updatable,
                    c.is_identity,
                    c.column_default,
                    pgd.description
                FROM information_schema.columns c
                    LEFT JOIN pg_catalog.pg_statio_all_tables as st ON
                        c.table_schema = st.schemaname
                        AND c.table_name = st.relname
                    LEFT JOIN pg_catalog.pg_description pgd ON
                        pgd.objsubid = c.ordinal_position
                        AND pgd.objoid = st.relid
                    WHERE c.table_schema = '%s' AND c.table_name = '%s';
            """ baseSchemaName baseTableName

            let columns = [
                use row = cmd.ExecuteReader()
                while row.Read() do

                    let udt = string row.["udt_name"]
                    let schema = unbox row.["table_schema"] 
                    yield {
                        Column.Name = unbox row.["column_name"]
                        DataType = 
                            {
                                Name = udt
                                Schema = schema
                                ClrType = 
                                    match unbox row.["data_type"] with 
                                    | "ARRAY" -> 
                                        let elemType = InformationSchema.postresTypeToClrType.[udt.TrimStart('_')]               
                                        elemType.MakeArrayType()
                                    | "USER-DEFINED" -> 
                                        typeof<obj>
                                    | _ -> 
                                        InformationSchema.postresTypeToClrType.[udt]
                            }

                        Nullable = unbox row.["is_nullable"] = "YES"
                        MaxLength = row.GetValueOrDefault("character_maximum_length", -1)
                        ReadOnly = unbox row.["is_updatable"] = "NO"
                        Identity = unbox row.["is_identity"] = "YES"
                        DefaultConstraint = row.GetValueOrDefault("column_default", "")
                        Description = row.GetValueOrDefault("description", "")
                        UDT = 
                            customTypes
                            |> Map.tryFind schema
                            |> Option.bind (List.tryFind (fun t -> t.Name = udt))
                    }
                ]
                

            //type data row
            let dataRowType = ProvidedTypeDefinition("Row", Some typeof<DataRow>)
            do 
                for c in columns do
                    let property = 
                        let name, dataType = c.Name, c.ClrType
                        if c.Nullable 
                        then
                            //let propertType = typedefof<_ option>.MakeGenericType dataType
                            let property = 
                                ProvidedProperty(
                                    name, 
                                    c.GetProvidedType(), 
                                    getterCode = QuotationsFactory.GetBody("GetNullableValueFromDataRow", dataType, name),
                                    ?setterCode = 
                                        if not c.ReadOnly 
                                        then QuotationsFactory.GetBody("SetNullableValueInDataRow", dataType, name) |> Some
                                        else None
                                )
                                
                            property
                        else
                            let property = 
                                ProvidedProperty(
                                    name, 
                                    c.GetProvidedType(),
                                    getterCode = (fun args -> <@@ (%%args.[0] : DataRow).[name] @@>),
                                    ?setterCode = 
                                        if not c.ReadOnly
                                        then Some( fun args -> <@@ (%%args.[0] : DataRow).[name] <- %%Expr.Coerce(args.[1], typeof<obj>) @@>)  
                                        else None
                                )
                                
                                
                            property

                    if c.Description <> "" 
                    then property.AddXmlDoc c.Description

                    dataRowType.AddMember property

            //type data table
            let dataTableType = DesignTime.GetDataTableType(tableName, dataRowType, columns)
            tagProvidedType dataTableType
            dataTableType.AddMember dataRowType
        
            do
                description |> Option.iter (fun x -> dataTableType.AddXmlDoc( sprintf "<summary>%s</summary>" x))

            do //ctor
                let invokeCode _ = 

                    let columnExprs = [
                        for c in columns -> 
                            let columnName = c.Name
                            let typeName = c.ClrType.FullName
                            let allowDBNull = c.Nullable || c.HasDefaultConstraint
                            <@@ 
                                let x = new DataColumn(columnName, Type.GetType( typeName, throwOnError = true))
                                x.AllowDBNull <- %%Expr.Value(allowDBNull)
                                if x.DataType = typeof<string>
                                then 
                                    x.MaxLength <- %%Expr.Value(c.MaxLength)
                                x.ReadOnly <- %%Expr.Value(c.ReadOnly)
                                x.AutoIncrement <- %%Expr.Value(c.Identity)

                                x
                            @@>
                    ]

                    let enumTypeColumns = 
                        Expr.NewArray( typeof<string>, columns |> List.choose(fun c -> c.UDT |> Option.map (fun _ -> Expr.Value c.Name)))

                    <@@ 
                        let selectCommand = new NpgsqlCommand(twoPartTableName, CommandType = CommandType.TableDirect)
                        let table = new DataTable<DataRow>(selectCommand, connectionString, %%enumTypeColumns)
                        table.TableName <- twoPartTableName
                        table.Columns.AddRange(%%Expr.NewArray(typeof<DataColumn>, columnExprs))
                        table
                    @@>

                let ctor = ProvidedConstructor([], invokeCode)
                dataTableType.AddMember ctor
                
            do
                let parameters, updateableColumns = 
                    [ 
                        for c in columns do 
                            if not(c.Identity || c.ReadOnly)
                            then 
                                let dataType = c.GetProvidedType(forceNullability = c.OptionalForInsert)
                                let parameter = 
                                    if c.OptionalForInsert
                                    then ProvidedParameter(c.Name, parameterType = dataType, optionalValue = null)
                                    else ProvidedParameter(c.Name, dataType)

                                yield parameter, c
                    ] 
                    |> List.sortBy (fun (_, c) -> c.OptionalForInsert) //move non-nullable params in front
                    |> List.unzip

                let methodXmlDoc = 
                    String.concat "\n" [
                        for c in updateableColumns do
                            if c.Description <> "" 
                            then 
                                let defaultConstrain = 
                                    if c.HasDefaultConstraint 
                                    then sprintf " Default constraint: %s." c.DefaultConstraint
                                    else ""
                                yield sprintf "<param name='%s'>%s%s</param>" c.Name c.Description defaultConstrain
                    ]


                let invokeCode = fun (args: _ list)-> 

                    let argsValuesConverted = 
                        (args.Tail, updateableColumns)
                        ||> List.map2 (fun valueExpr c ->
                            if c.OptionalForInsert
                            then 
                                typeof<QuotationsFactory>
                                    .GetMethod("OptionToObj", BindingFlags.NonPublic ||| BindingFlags.Static)
                                    .MakeGenericMethod(c.ClrType)
                                    .Invoke(null, [| box valueExpr |])
                                    |> unbox
                            else
                                valueExpr
                        )

                    <@@ 
                        let table: DataTable<DataRow> = %%args.[0]
                        let row = table.NewRow()

                        let values: obj[] = %%Expr.NewArray(typeof<obj>, [ for x in argsValuesConverted -> Expr.Coerce(x, typeof<obj>) ])
                        let namesOfUpdateableColumns: string[] = %%Expr.NewArray(typeof<string>, [ for c in updateableColumns -> Expr.Value(c.Name) ])
                        let optionalParams: bool[] = %%Expr.NewArray(typeof<bool>, [ for c in updateableColumns -> Expr.Value(c.OptionalForInsert) ])

                        Debug.Assert(values.Length = namesOfUpdateableColumns.Length, "values.Length = namesOfUpdateableColumns.Length")
                        Debug.Assert(values.Length = optionalParams.Length, "values.Length = optionalParams.Length")

                        for name, value, optional in Array.zip3 namesOfUpdateableColumns values optionalParams do 
                            row.[name] <- if value = null && optional then box DbNull else value
                        row
                    @@>

                do 
                    let newRowMethod = ProvidedMethod("NewRow", parameters, dataRowType, invokeCode)
                    if methodXmlDoc <> "" then newRowMethod.AddXmlDoc methodXmlDoc
                    dataTableType.AddMember newRowMethod

                    let addRowMethod =
                        ProvidedMethod(
                            "AddRow", 
                            parameters, 
                            typeof<Void>, 
                            invokeCode = fun args ->
                                let newRow = invokeCode args
                                <@@
                                    let table: DataTable<DataRow> = %%args.[0]
                                    let row: DataRow = %%newRow
                                    table.Rows.Add row
                                @@>
                        )

                    if methodXmlDoc <> "" then addRowMethod.AddXmlDoc methodXmlDoc
                    dataTableType.AddMember addRowMethod

            dataTableType
        )

    tables

let getEnums(conn: NpgsqlConnection) = 
    use __ = conn.UseLocally()
    use cmd = conn.CreateCommand()
    cmd.CommandText <- "
        SELECT
          n.nspname              AS schema,
          t.typname              AS name,
          array_agg(e.enumlabel) AS values
        FROM pg_type t
          JOIN pg_enum e ON t.oid = e.enumtypid
          JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        GROUP BY
          schema, name
    "
    [
        use cursor = cmd.ExecuteReader()
        while cursor.Read() do
            let schema = cursor.GetString(0)
            let name = cursor.GetString(1)
            let values: string[] = cursor.GetValue(2) :?> _
            let t = new ProvidedTypeDefinition(name, Some typeof<string>, hideObjectMethods = true, nonNullable = true)
            for value in values do
                t.AddMember( ProvidedField.Literal(value, t, value))

            yield schema, t
    ]
    |> List.groupBy fst
    |> List.map(fun (schema, types) ->
        schema, List.map snd types
    )
    |> Map.ofList
    
let getUserSchemas(conn: NpgsqlConnection) = 
    use __ = conn.UseLocally()
    use cmd = new NpgsqlCommand("
        SELECT n.nspname  
        FROM pg_catalog.pg_namespace n                                       
        WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema';
    ", conn)

    using (cmd.ExecuteReader()) <| fun cursor -> 
    [ 
        while cursor.Read() do 
            yield cursor.GetString(0) 
    ]
        
let createRootType( assembly, nameSpace: string, typeName, connection) =
    if String.IsNullOrWhiteSpace connection then invalidArg "Connection" "Value is empty!" 
        
    let conn = new NpgsqlConnection(connection)
    use __ = conn.UseLocally()

    let databaseRootType = ProvidedTypeDefinition(assembly, nameSpace, typeName, baseType = Some typeof<obj>, hideObjectMethods = true)

    let tagProvidedType(t: ProvidedTypeDefinition) =
        let p = ProvidedProperty( "Connection", typeof<string>, getterCode = (fun _ -> <@@ connection @@>), isStatic = true)
        t.AddMember( p)

    let schemas = 
        conn
        |> getUserSchemas
        |> List.map (fun schema -> ProvidedTypeDefinition(schema, baseType = Some typeof<obj>, hideObjectMethods = true))
        
    let enums = getEnums(conn) 

    databaseRootType.AddMembers schemas

    let customTypes = Dictionary()

    for s in schemas do
        let ts = ProvidedTypeDefinition("Types", Some typeof<obj>, hideObjectMethods = true)

        //enums |> Map.tryFind s.Name |> Option.iter ts.AddMembers

        //ts.AddMembersDelayed <| fun() ->
        //    enums |> Map.tryFind s.Name |> Option.defaultValue [] 
        
        enums 
        |> Map.tryFind s.Name 
        |> Option.iter (fun xs ->
            for x in xs do  
                ts.AddMember x
                customTypes.Add(sprintf "%s.%s" s.Name x.Name, x)
        )

        s.AddMember ts

        //for t in ts.GetNestedTypes() do    
        //    //let typeName = t.Name
        //    //let ns = t.Namespace
        //    customTypes.Add(sprintf "%s.%s" s.Name t.Name, downcast t)
        
    for schemaType in schemas do
        schemaType.AddMemberDelayed <| fun() -> getTableTypes(conn, schemaType.Name, connection, tagProvidedType, enums)

    let commands = ProvidedTypeDefinition( "Commands", None)
    databaseRootType.AddMember commands
    addCreateCommandMethod(conn, connection, databaseRootType, commands, customTypes, connection)

    databaseRootType           

let getProviderType(assembly, nameSpace, cache: ConcurrentDictionary<_, ProvidedTypeDefinition>) = 

    let providerType = ProvidedTypeDefinition(assembly, nameSpace, "Npgsql", Some typeof<obj>, hideObjectMethods = true)

    do 
        providerType.DefineStaticParameters(
            parameters = [ 
                ProvidedStaticParameter("Connection", typeof<string>) 
            ],
            instantiationFunction = (fun typeName args ->
                cache.GetOrAdd(
                    typeName, fun _ -> createRootType(assembly, nameSpace, typeName, unbox args.[0])
                )
            ) 
        )

        providerType.AddXmlDoc """
<summary>Typed access to PostgreSQL programmable objects: tables and functions.</summary> 
<param name='Connection'>String used to open a Postgresql database or the name of the connection string in the configuration file in the form of “name=&lt;connection string name&gt;”.</param>
"""
    providerType


 

