﻿module internal FSharp.Data.Npgsql.DesignTime.NpgsqlConnectionProvider

open System
open System.Data
open System.Collections.Concurrent
open System.Collections.Generic

open FSharp.Quotations
open ProviderImplementation.ProvidedTypes

open Npgsql

open FSharp.Data.Npgsql
open InformationSchema

let methodsCache = new ConcurrentDictionary<_, ProvidedMethod>()

let addCreateCommandMethod(connectionString, rootType: ProvidedTypeDefinition, commands: ProvidedTypeDefinition, customTypes, fsx, isHostedExecution, globalXCtor) = 
        
    let xctorParam = ProvidedStaticParameter("XCtor", typeof<bool>, false) 

    let staticParams = 
        [
            ProvidedStaticParameter("CommandText", typeof<string>) 
            ProvidedStaticParameter("ResultType", typeof<ResultType>, ResultType.Records) 
            ProvidedStaticParameter("SingleRow", typeof<bool>, false)   
            ProvidedStaticParameter("AllParametersOptional", typeof<bool>, false) 
            ProvidedStaticParameter("TypeName", typeof<string>, "") 
        ] @ [ 
            if not globalXCtor then yield xctorParam
        ]

    let m = ProvidedMethod("CreateCommand", [], typeof<obj>, isStatic = true, invokeCode = Unchecked.defaultof<_>)
    m.DefineStaticParameters(staticParams, (fun methodName args ->

        let getMethodImpl () = 

            let sqlStatement, resultType, singleRow, allParametersOptional, typename, xctor  = 
                if not globalXCtor
                then 
                    args.[0] :?> _ , args.[1] :?> _, args.[2] :?> _, args.[3] :?> _, args.[4] :?> _, args.[5] :?> _
                else
                    args.[0] :?> _ , args.[1] :?> _, args.[2] :?> _, args.[3] :?> _, args.[4] :?> _, true
                    
            if singleRow && not (resultType = ResultType.Records || resultType = ResultType.Tuples)
            then 
                invalidArg "singleRow" "SingleRow can be set only for ResultType.Records or ResultType.Tuples."

            let parameters = extractParameters(connectionString, sqlStatement, allParametersOptional)

            let outputColumns = 
                if resultType <> ResultType.DataReader
                then getOutputColumns(connectionString, sqlStatement, CommandType.Text, parameters, ref customTypes)
                else []

            let commandBehaviour = if singleRow then CommandBehavior.SingleRow else CommandBehavior.Default

            let returnType = 
                QuotationsFactory.GetOutputTypes(
                    outputColumns, 
                    resultType, 
                    commandBehaviour, 
                    hasOutputParameters = false, 
                    allowDesignTimeConnectionStringReUse = (isHostedExecution && fsx),
                    designTimeConnectionString = (if fsx then connectionString else null)
                )

            let commandTypeName = if typename <> "" then typename else methodName.Replace("=", "").Replace("@", "")
            let cmdProvidedType = ProvidedTypeDefinition(commandTypeName, Some typeof<``ISqlCommand Implementation``>, hideObjectMethods = true)

            do  
                let executeArgs = QuotationsFactory.GetExecuteArgs(parameters, customTypes)

                let addRedirectToISqlCommandMethod outputType name = 
                    let hasOutputParameters = false
                    QuotationsFactory.AddGeneratedMethod(parameters, hasOutputParameters, executeArgs, cmdProvidedType.BaseType, outputType, name) 
                    |> cmdProvidedType.AddMember

                addRedirectToISqlCommandMethod returnType.Single "Execute" 
                addRedirectToISqlCommandMethod returnType.Single "ExecutePrepared" 
                            
                let asyncReturnType = ProvidedTypeBuilder.MakeGenericType(typedefof<_ Async>, [ returnType.Single ])
                addRedirectToISqlCommandMethod asyncReturnType "AsyncExecute" 
                addRedirectToISqlCommandMethod asyncReturnType "AsyncExecutePrepared" 

            commands.AddMember cmdProvidedType

            if resultType = ResultType.Records 
            then
                returnType.PerRow 
                |> Option.filter (fun x -> x.Provided <> x.ErasedTo && outputColumns.Length > 1)
                |> Option.iter (fun x -> cmdProvidedType.AddMember x.Provided)

            elif resultType = ResultType.DataTable 
            then
                returnType.Single |> cmdProvidedType.AddMember

            
            let useLegacyPostgis = 
                (parameters |> List.exists (fun p -> p.DataType.ClrType = typeof<LegacyPostgis.PostgisGeometry>))
                ||
                (outputColumns |> List.exists (fun c -> c.ClrType = typeof<LegacyPostgis.PostgisGeometry>))


            let designTimeConfig = 
                <@@ {
                    SqlStatement = sqlStatement
                    Parameters = %%Expr.NewArray( typeof<NpgsqlParameter>, parameters |> List.map QuotationsFactory.ToSqlParam)
                    ResultType = %%Expr.Value(resultType)
                    SingleRow = singleRow
                    Row2ItemMapping = %%returnType.Row2ItemMapping
                    SeqItemTypeName = %%returnType.SeqItemTypeName
                    ExpectedColumns = %%Expr.NewArray(typeof<DataColumn>, [ for c in outputColumns -> c.ToDataColumnExpr() ])
                    UseLegacyPostgis = useLegacyPostgis
                } @@>


            let ctorsAndFactories = 
                QuotationsFactory.GetCommandCtors(
                    cmdProvidedType, 
                    designTimeConfig, 
                    allowDesignTimeConnectionStringReUse = (isHostedExecution && fsx),
                    ?connectionString  = (if fsx then Some connectionString else None), 
                    factoryMethodName = methodName
                )
            assert (ctorsAndFactories.Length = 4)
            let impl: ProvidedMethod = downcast ctorsAndFactories.[if xctor then 3 else 1] 
            rootType.AddMember impl
            impl

        methodsCache.GetOrAdd(methodName, fun _ -> getMethodImpl())
    ))
    rootType.AddMember m

//https://stackoverflow.com/questions/12445608/psql-list-all-tables#12455382

let getTableTypes(connectionString: string, schema, customTypes: Map<_, ProvidedTypeDefinition list>, fsx, isHostedExecution) = 
    let tables = ProvidedTypeDefinition("Tables", Some typeof<obj>)
    tables.AddMembersDelayed <| fun() ->
        
        getTables(connectionString, schema)
        |> List.map (fun (tableName, description) -> 
                
            let columns = getTableColumns(connectionString, schema, tableName, customTypes)

            //type data row
            let dataRowType = QuotationsFactory.GetDataRowType(columns)
            //type data table
            let dataTableType = 
                QuotationsFactory.GetDataTableType(
                    tableName, 
                    dataRowType, 
                    columns, 
                    isHostedExecution && fsx,
                    designTimeConnectionString = (if fsx then connectionString else null)
                )

            dataTableType.AddMember dataRowType
        
            do
                description |> Option.iter (fun x -> dataTableType.AddXmlDoc( sprintf "<summary>%s</summary>" x))

            do //ctor
                let invokeCode _ = 

                    let columnExprs = [ for c in columns -> c.ToDataColumnExpr() ]

                    let twoPartTableName = 
                        use x = new NpgsqlCommandBuilder()
                        sprintf "%s.%s" (x.QuoteIdentifier schema) (x.QuoteIdentifier tableName)

                    let cmdText =  
                        columns
                        |> List.map(fun c ->  c.Name)
                        |> String.concat " ,"
                        |> sprintf "SELECT %s FROM %s" twoPartTableName

                    <@@ 
                        let selectCommand = new NpgsqlCommand(cmdText)
                        let table = new FSharp.Data.Npgsql.DataTable<DataRow>(selectCommand)
                        table.TableName <- twoPartTableName
                        table.Columns.AddRange(%%Expr.NewArray(typeof<DataColumn>, columnExprs))
                        table
                    @@>

                let ctor = ProvidedConstructor([], invokeCode)
                dataTableType.AddMember ctor    

                let binaryImport = 
                    ProvidedMethod(
                        "BinaryImport", 
                        [ ProvidedParameter("connection", typeof<NpgsqlConnection> ) ],
                        typeof<Void>,
                        invokeCode = fun args -> <@@ Utils.BinaryImport(%%args.[0], %%args.[1]) @@>
                    )
                dataTableType.AddMember binaryImport

            dataTableType
        )

    tables

let getEnums connectionString = 
    use conn = openConnection(connectionString)
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

            //let valuesFieldType = ProvidedTypeBuilder.MakeGenericType(typedefof<_ list>, [ t ])
            //let valuesField = ProvidedField("Values", valuesFieldType) 
            //valuesField.SetFieldAttributes( FieldAttributes.Public ||| FieldAttributes.InitOnly ||| FieldAttributes.Static)
            //t.AddMember( valuesField)

            //let typeInit = 
            //    let valuesExpr = Expr.NewArray(typeof<string>, [ for v in values -> Expr.Value(v)])
            //    ProvidedConstructor(
            //        [], 
            //        invokeCode = (fun _ -> Expr.FieldSet(valuesField, Expr.Coerce(valuesExpr, valuesFieldType))),
            //        IsTypeInitializer = true
            //    )

            //t.AddMember typeInit 

            yield schema, t
    ]
    |> List.groupBy fst
    |> List.map(fun (schema, types) ->
        schema, List.map snd types
    )
    |> Map.ofList
    
let getUserSchemas connectionString = 
    use conn = openConnection connectionString
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
        
let createRootType
    ( 
        assembly, nameSpace: string, typeName, isHostedExecution, resolutionFolder,
        connectionStringOrName, configType, config, xctor, fsx
    ) =

    if String.IsNullOrWhiteSpace connectionStringOrName then invalidArg "Connection" "Value is empty!" 
    let connectionString = Configuration.readConnectionString(connectionStringOrName, configType, config, resolutionFolder)
        
    let databaseRootType = ProvidedTypeDefinition(assembly, nameSpace, typeName, baseType = Some typeof<obj>, hideObjectMethods = true)

    let schemas = 
        connectionString
        |> getUserSchemas
        |> List.map (fun schema -> ProvidedTypeDefinition(schema, baseType = Some typeof<obj>, hideObjectMethods = true))
        
    let enums = getEnums connectionString

    databaseRootType.AddMembers schemas

    let customTypes = Dictionary()

    for s in schemas do
        let ts = ProvidedTypeDefinition("Types", Some typeof<obj>, hideObjectMethods = true)

        enums 
        |> Map.tryFind s.Name 
        |> Option.iter (fun xs ->
            for x in xs do  
                ts.AddMember x
                customTypes.Add(sprintf "%s.%s" s.Name x.Name, x)
        )

        s.AddMember ts

    for schemaType in schemas do
        schemaType.AddMemberDelayed <| fun() -> 
            getTableTypes(connectionString, schemaType.Name, enums, fsx, isHostedExecution)

    let commands = ProvidedTypeDefinition( "Commands", None)
    databaseRootType.AddMember commands
    addCreateCommandMethod(connectionString, databaseRootType, commands, customTypes, fsx, isHostedExecution, xctor)

    databaseRootType           

let getProviderType(assembly, nameSpace, isHostedExecution, resolutionFolder, cache: ConcurrentDictionary<_, ProvidedTypeDefinition>) = 

    let providerType = ProvidedTypeDefinition(assembly, nameSpace, "NpgsqlConnection", Some typeof<obj>, hideObjectMethods = true)

    do 
        providerType.DefineStaticParameters(
            parameters = [ 
                ProvidedStaticParameter("Connection", typeof<string>) 
                ProvidedStaticParameter("ConfigType", typeof<ConfigType>, ConfigType.JsonFile) 
                ProvidedStaticParameter("Config", typeof<string>, "") 
                ProvidedStaticParameter("XCtor", typeof<bool>, false) 
                ProvidedStaticParameter("Fsx", typeof<bool>, false) 
            ],
            instantiationFunction = (fun typeName args ->
                cache.GetOrAdd(
                    typeName, fun _ -> 
                        createRootType(
                            assembly, nameSpace, typeName, isHostedExecution, resolutionFolder,
                            unbox args.[0], unbox args.[1], unbox args.[2], unbox args.[3], unbox args.[4]
                        )
                )   
            ) 
        )

        providerType.AddXmlDoc """
<summary>Typed access to PostgreSQL programmable objects: tables and functions.</summary> 
<param name='Connection'>String used to open a Postgresql database or the name of the connection string in the configuration file in the form of “name=&lt;connection string name&gt;”.</param>
<param name='Fsx'>Re-use design time connection string for the type provider instantiation from *.fsx files.</param>
<param name='ConfigType'>JsonFile, Environment or UserStore. Default is JsonFile.</param>
<param name='Config'>JSON configuration file with connection string information. Matches 'Connection' parameter as name in 'ConnectionStrings' section.</param>
"""
    providerType


 

