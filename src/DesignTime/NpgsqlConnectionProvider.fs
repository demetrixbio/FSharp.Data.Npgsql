module internal FSharp.Data.Npgsql.DesignTime.NpgsqlConnectionProvider

open System
open System.Data
open System.Collections.Concurrent

open FSharp.Quotations
open ProviderImplementation.ProvidedTypes

open Npgsql

open System.Collections.Generic
open FSharp.Data.Npgsql
open InformationSchema

let methodsCache = new ConcurrentDictionary<_, ProvidedMethod>()

let addCreateCommandMethod(connectionString, rootType: ProvidedTypeDefinition, commands: ProvidedTypeDefinition, dbSchemaLookups : DbSchemaLookups, fsx, isHostedExecution, globalXCtor) = 
        
    let staticParams = 
        [
            yield ProvidedStaticParameter("CommandText", typeof<string>) 
            yield ProvidedStaticParameter("ResultType", typeof<ResultType>, ResultType.Records) 
            yield ProvidedStaticParameter("SingleRow", typeof<bool>, false)   
            yield ProvidedStaticParameter("AllParametersOptional", typeof<bool>, false) 
            yield ProvidedStaticParameter("TypeName", typeof<string>, "") 
            if not globalXCtor then yield ProvidedStaticParameter("XCtor", typeof<bool>, false)
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

            let customTypes = dbSchemaLookups.Enums :> IDictionary<string, ProvidedTypeDefinition> |> ref
            
            let (parameters, outputColumns) = InformationSchema.extractParametersAndOutputColumns(connectionString, sqlStatement, resultType, allParametersOptional, customTypes)

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
                let executeArgs = QuotationsFactory.GetExecuteArgs(parameters, dbSchemaLookups.Enums)

                let addRedirectToISqlCommandMethod outputType name = 
                    let hasOutputParameters = false
                    QuotationsFactory.AddGeneratedMethod(parameters, hasOutputParameters, executeArgs, cmdProvidedType.BaseType, outputType, name) 
                    |> cmdProvidedType.AddMember

                addRedirectToISqlCommandMethod returnType.Single "Execute" 
                            
                let asyncReturnType = ProvidedTypeBuilder.MakeGenericType(typedefof<_ Async>, [ returnType.Single ])
                addRedirectToISqlCommandMethod asyncReturnType "AsyncExecute" 

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

let createTableTypes(connectionString: string, item: DbSchemaLookupItem, fsx, isHostedExecution) = 
    let tables = ProvidedTypeDefinition("Tables", Some typeof<obj>)
    tables.AddMembersDelayed <| fun() ->
        
        item.Tables
        |> Seq.map (fun s -> 
            let tableName = s.Key.Name
            let description = s.Key.Description
            let columns = s.Value |> List.ofSeq

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
                        sprintf "%s.%s" (x.QuoteIdentifier item.Schema.Name) (x.QuoteIdentifier tableName)

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
        |> List.ofSeq

    tables

let createRootType
    ( 
        assembly, nameSpace: string, typeName, isHostedExecution, resolutionFolder, (schemaCache : ConcurrentDictionary<string, DbSchemaLookups>),
        connectionStringOrName, configType, config, xctor, fsx
    ) =

    if String.IsNullOrWhiteSpace connectionStringOrName then invalidArg "Connection" "Value is empty!" 
    let connectionString = Configuration.readConnectionString(connectionStringOrName, configType, config, resolutionFolder)
        
    let databaseRootType = ProvidedTypeDefinition(assembly, nameSpace, typeName, baseType = Some typeof<obj>, hideObjectMethods = true)

    let schemaLookups =
        schemaCache.GetOrAdd(
            connectionString, fun _ ->
                InformationSchema.getDbSchemaLookups(connectionString))
    
    let schemas = schemaLookups.Schemas
                  |> Seq.map (fun s -> ProvidedTypeDefinition(s.Key, baseType = Some typeof<obj>, hideObjectMethods = true))
                  |> List.ofSeq
                  
    databaseRootType.AddMembers schemas
    
    for schemaType in schemas do
        let es = ProvidedTypeDefinition("Types", Some typeof<obj>, hideObjectMethods = true)
        for enum in schemaLookups.Schemas.[schemaType.Name].Enums do
            es.AddMember enum.Value
        schemaType.AddMember es
        
    for schemaType in schemas do
        schemaType.AddMemberDelayed <| fun () ->
            createTableTypes(connectionString, schemaLookups.Schemas.[schemaType.Name], fsx, isHostedExecution)

    let commands = ProvidedTypeDefinition("Commands", None)
    databaseRootType.AddMember commands
    addCreateCommandMethod(connectionString, databaseRootType, commands, schemaLookups, fsx, isHostedExecution, xctor)

    databaseRootType           

let getProviderType(assembly,
                    nameSpace,
                    isHostedExecution,
                    resolutionFolder,
                    cache: ConcurrentDictionary<_, ProvidedTypeDefinition>,
                    schemaCache : ConcurrentDictionary<string, DbSchemaLookups>) = 

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
                            assembly, nameSpace, typeName, isHostedExecution, resolutionFolder, schemaCache,
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


 

