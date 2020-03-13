module internal FSharp.Data.Npgsql.DesignTime.NpgsqlConnectionProvider

open System
open System.Data

open FSharp.Quotations
open ProviderImplementation.ProvidedTypes

open Npgsql

open FSharp.Data.Npgsql
open InformationSchema

let methodsCache = Cache<ProvidedMethod>()

let addCreateCommandMethod(connectionString, rootType: ProvidedTypeDefinition, commands: ProvidedTypeDefinition, customTypes : Map<string, ProvidedTypeDefinition>,
                           dbSchemaLookups : DbSchemaLookups, fsx, isHostedExecution, globalXCtor, globalPrepare, providedTypeReuse) = 
        
    let staticParams = 
        [
            yield ProvidedStaticParameter("CommandText", typeof<string>) 
            yield ProvidedStaticParameter("ResultType", typeof<ResultType>, ResultType.Records) 
            yield ProvidedStaticParameter("SingleRow", typeof<bool>, false)   
            yield ProvidedStaticParameter("AllParametersOptional", typeof<bool>, false) 
            yield ProvidedStaticParameter("TypeName", typeof<string>, "") 
            if not globalXCtor then yield ProvidedStaticParameter("XCtor", typeof<bool>, false)
            yield ProvidedStaticParameter("Prepare", typeof<bool>, globalPrepare)   
        ]

    let m = ProvidedMethod("CreateCommand", [], typeof<obj>, isStatic = true)
    m.DefineStaticParameters(staticParams, (fun methodName args ->
        methodsCache.GetOrAdd(
            methodName,
            lazy
                let sqlStatement, resultType, singleRow, allParametersOptional, typename, xctor, prepare  = 
                    if not globalXCtor
                    then 
                        args.[0] :?> _ , args.[1] :?> _, args.[2] :?> _, args.[3] :?> _, args.[4] :?> _, args.[5] :?> _, args.[6] :?> _
                    else
                        args.[0] :?> _ , args.[1] :?> _, args.[2] :?> _, args.[3] :?> _, args.[4] :?> _, true, args.[5] :?> _
                        
                if singleRow && not (resultType = ResultType.Records || resultType = ResultType.Tuples)
                then 
                    invalidArg "singleRow" "SingleRow can be set only for ResultType.Records or ResultType.Tuples."

                let (parameters, outputColumns, _, _) = InformationSchema.extractParametersAndOutputColumns(connectionString, sqlStatement, resultType, allParametersOptional, dbSchemaLookups)
                
                let commandBehaviour = if singleRow then CommandBehavior.SingleRow else CommandBehavior.Default

                let returnTypes =
                    outputColumns |> List.mapi (fun i cs ->
                        QuotationsFactory.GetOutputTypes(
                            cs,
                            customTypes,
                            resultType, 
                            commandBehaviour, 
                            hasOutputParameters = false, 
                            allowDesignTimeConnectionStringReUse = (isHostedExecution && fsx),
                            designTimeConnectionString = (if fsx then connectionString else null),
                            typeNameSuffix = (if outputColumns.Length > 1 then (i + 1).ToString () else ""),
                            providedTypeReuse = providedTypeReuse))

                let commandTypeName = if typename <> "" then typename else methodName.Replace("=", "").Replace("@", "")

                let cmdProvidedType = ProvidedTypeDefinition(commandTypeName, Some typeof<``ISqlCommand Implementation``>, hideObjectMethods = true)
                commands.AddMember cmdProvidedType
                
                QuotationsFactory.AddTopLevelTypes cmdProvidedType parameters resultType customTypes returnTypes outputColumns (if resultType <> ResultType.Records || providedTypeReuse = NoReuse then cmdProvidedType else rootType)

                commands.AddMember cmdProvidedType

                let useLegacyPostgis = 
                    (parameters |> List.exists (fun p -> p.DataType.ClrType = typeof<LegacyPostgis.PostgisGeometry>))
                    ||
                    (outputColumns |> List.concat |> List.exists (fun c -> c.ClrType = typeof<LegacyPostgis.PostgisGeometry>))

                let isTypeReuseEnabled = providedTypeReuse <> NoReuse

                let designTimeConfig = 
                    <@@ {
                        SqlStatement = sqlStatement
                        Parameters = %%Expr.NewArray( typeof<NpgsqlParameter>, parameters |> List.map QuotationsFactory.ToSqlParam)
                        ResultType = %%Expr.Value(resultType)
                        SingleRow = singleRow
                        ResultSets = %%Expr.NewArray(typeof<ResultSetDefinition>, QuotationsFactory.BuildResultSetDefinitions outputColumns returnTypes)
                        UseLegacyPostgis = useLegacyPostgis
                        Prepare = prepare
                        IsTypeReuseEnabled = isTypeReuseEnabled
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
                impl)
    ))
    rootType.AddMember m

let createTableTypes(connectionString: string, customTypes : Map<string, ProvidedTypeDefinition>, item: DbSchemaLookupItem, fsx, isHostedExecution) = 
    let tables = ProvidedTypeDefinition("Tables", Some typeof<obj>)
    tables.AddMembersDelayed <| fun() ->
        
        item.Tables
        |> Seq.map (fun s -> 
            let tableName = s.Key.Name
            let description = s.Key.Description
            let columns = s.Value |> List.ofSeq

            //type data row
            let dataRowType = QuotationsFactory.GetDataRowType(customTypes, columns)
            //type data table
            let dataTableType = 
                QuotationsFactory.GetDataTableType(
                    tableName, 
                    dataRowType,
                    customTypes,
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

                    let columns = columns |> List.map(fun c ->  c.Name) |> String.concat " ,"
                    let cmdText = sprintf "SELECT %s FROM %s" columns twoPartTableName

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
        assembly, nameSpace: string, typeName, isHostedExecution, resolutionFolder, schemaCache: Cache<DbSchemaLookups>,
        connectionStringOrName, configType, config, xctor, fsx, prepare, reuseProvidedTypes, cache
    ) =

    if String.IsNullOrWhiteSpace connectionStringOrName then invalidArg "Connection" "Value is empty!" 
    let connectionString = Configuration.readConnectionString(connectionStringOrName, configType, config, resolutionFolder)
        
    let databaseRootType = ProvidedTypeDefinition(assembly, nameSpace, typeName, baseType = Some typeof<obj>, hideObjectMethods = true)

    let schemaLookups =
        schemaCache.GetOrAdd(
            connectionString,
            lazy InformationSchema.getDbSchemaLookups(connectionString))
    
    let dbSchemas = schemaLookups.Schemas
                    |> Seq.map (fun s -> ProvidedTypeDefinition(s.Key, baseType = Some typeof<obj>, hideObjectMethods = true))
                    |> List.ofSeq
                  
    databaseRootType.AddMembers dbSchemas
    
    let customTypes =
        [ for schemaType in dbSchemas do
            let es = ProvidedTypeDefinition("Types", Some typeof<obj>, hideObjectMethods = true)
            for (KeyValue(_, enum)) in schemaLookups.Schemas.[schemaType.Name].Enums do
                let t = QuotationsFactory.GetEnumType enum enum.Name
                es.AddMember t
                let udtTypeName = sprintf "%s.%s" enum.Schema enum.Name
                yield udtTypeName, t

            for (KeyValue(_, composite)) in schemaLookups.Schemas.[schemaType.Name].CompositeTypes do
                let t = QuotationsFactory.GetCompositeType composite composite.Name
                es.AddMember t
                let udtTypeName = sprintf "%s.%s" schemaType.Name composite.Name
                yield udtTypeName, t

            schemaType.AddMember es
        ] |> Map.ofList
        
    for schemaType in dbSchemas do
        schemaType.AddMemberDelayed <| fun () ->
            createTableTypes(connectionString, customTypes, schemaLookups.Schemas.[schemaType.Name], fsx, isHostedExecution)

    let commands = ProvidedTypeDefinition("Commands", None)
    databaseRootType.AddMember commands
    let providedTypeReuse = if reuseProvidedTypes then WithCache cache else NoReuse
    addCreateCommandMethod(connectionString, databaseRootType, commands, customTypes, schemaLookups, fsx, isHostedExecution, xctor, prepare, providedTypeReuse)

    databaseRootType           

let internal getProviderType(assembly, nameSpace, isHostedExecution, resolutionFolder, cache: Cache<ProvidedTypeDefinition>, schemaCache : Cache<DbSchemaLookups>) = 

    let providerType = ProvidedTypeDefinition(assembly, nameSpace, "NpgsqlConnection", Some typeof<obj>, hideObjectMethods = true)

    do 
        providerType.DefineStaticParameters(
            parameters = [ 
                ProvidedStaticParameter("Connection", typeof<string>) 
                ProvidedStaticParameter("ConfigType", typeof<ConfigType>, ConfigType.JsonFile) 
                ProvidedStaticParameter("Config", typeof<string>, "") 
                ProvidedStaticParameter("XCtor", typeof<bool>, false) 
                ProvidedStaticParameter("Fsx", typeof<bool>, false) 
                ProvidedStaticParameter("Prepare", typeof<bool>, false)
                ProvidedStaticParameter("ReuseProvidedTypes", typeof<bool>, false) 
            ],
            instantiationFunction = (fun typeName args ->
                cache.GetOrAdd(
                    typeName,
                    lazy
                        createRootType(
                            assembly, nameSpace, typeName, isHostedExecution, resolutionFolder, schemaCache,
                            unbox args.[0], unbox args.[1], unbox args.[2], unbox args.[3], unbox args.[4], unbox args.[5], unbox args.[6], cache
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
<param name='Prepare'>If set the command will be executed as prepared. See Npgsql documentation for prepared statements.</param>
<param name='ReuseProvidedTypes'>Reuse the return type for commands that select data of identical shape. Please see the readme for details.</param>
"""
    providerType


 

