module internal FSharp.Data.Npgsql.DesignTime.NpgsqlConnectionProvider

open System
open System.Data

open FSharp.Quotations
open ProviderImplementation.ProvidedTypes

open Npgsql

open FSharp.Data.Npgsql
open InformationSchema
open System.Collections.Concurrent

let methodsCache = ConcurrentDictionary<string, ProvidedMethod>()

let addCreateCommandMethod(connectionString, rootType: ProvidedTypeDefinition, commands: ProvidedTypeDefinition, customTypes: Map<string, ProvidedTypeDefinition>,
                           dbSchemaLookups: DbSchemaLookups, globalXCtor, globalPrepare, providedTypeReuse) = 
        
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
            fun methodName ->
                let sqlStatement, resultType, singleRow, allParametersOptional, typename, xctor, prepare  = 
                    if not globalXCtor
                    then 
                        args.[0] :?> _ , args.[1] :?> _, args.[2] :?> _, args.[3] :?> _, args.[4] :?> _, args.[5] :?> _, args.[6] :?> _
                    else
                        args.[0] :?> _ , args.[1] :?> _, args.[2] :?> _, args.[3] :?> _, args.[4] :?> _, true, args.[5] :?> _
                        
                if singleRow && not (resultType = ResultType.Records || resultType = ResultType.Tuples)
                then 
                    invalidArg "singleRow" "SingleRow can be set only for ResultType.Records or ResultType.Tuples."

                let (parameters, outputColumns, _) = InformationSchema.extractParametersAndOutputColumns(connectionString, sqlStatement, resultType, allParametersOptional, dbSchemaLookups)
                
                let commandBehaviour = if singleRow then CommandBehavior.SingleRow else CommandBehavior.Default

                let returnTypes =
                    outputColumns |> List.mapi (fun i cs ->
                        QuotationsFactory.GetOutputTypes(
                            cs,
                            customTypes,
                            resultType, 
                            commandBehaviour,
                            typeNameSuffix = (if outputColumns.Length > 1 then (i + 1).ToString () else ""),
                            providedTypeReuse = providedTypeReuse))

                let commandTypeName = if typename <> "" then typename else methodName.Replace("=", "").Replace("@", "")

                let cmdProvidedType = ProvidedTypeDefinition(commandTypeName, Some typeof<``ISqlCommand Implementation``>, hideObjectMethods = true)
                commands.AddMember cmdProvidedType
                
                QuotationsFactory.AddTopLevelTypes cmdProvidedType parameters resultType customTypes returnTypes outputColumns (if resultType <> ResultType.Records || providedTypeReuse = NoReuse then cmdProvidedType else rootType)

                commands.AddMember cmdProvidedType

                let useNetTopologySuite = 
                    (parameters |> List.exists (fun p -> p.DataType.ClrType = typeof<NetTopologySuite.Geometries.Geometry>))
                    ||
                    (outputColumns |> List.concat |> List.exists (fun c -> c.ClrType = typeof<NetTopologySuite.Geometries.Geometry>))

                let isTypeReuseEnabled = providedTypeReuse <> NoReuse

                let designTimeConfig = 
                    <@@ {
                        SqlStatement = sqlStatement
                        Parameters = %%Expr.NewArray( typeof<NpgsqlParameter>, parameters |> List.map QuotationsFactory.ToSqlParam)
                        ResultType = %%Expr.Value(resultType)
                        SingleRow = singleRow
                        ResultSets = %%Expr.NewArray(typeof<ResultSetDefinition>, QuotationsFactory.BuildResultSetDefinitions outputColumns returnTypes)
                        UseNetTopologySuite = useNetTopologySuite
                        Prepare = prepare
                        IsTypeReuseEnabled = isTypeReuseEnabled
                    } @@>

                let method = QuotationsFactory.GetCommandFactoryMethod (cmdProvidedType, designTimeConfig, xctor, methodName)

                rootType.AddMember method
                method)
    ))
    rootType.AddMember m

let createTableTypes(customTypes : Map<string, ProvidedTypeDefinition>, item: DbSchemaLookupItem) = 
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
                    columns
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
                        [ ProvidedParameter("connection", typeof<NpgsqlConnection>) ],
                        typeof<uint64>,
                        invokeCode = fun args -> <@@ Utils.BinaryImport(%%args.[0], %%args.[1]) @@>
                    )
                dataTableType.AddMember binaryImport

            dataTableType
        )
        |> List.ofSeq

    tables

let createRootType
    ( 
        assembly, nameSpace: string, typeName, schemaCache: ConcurrentDictionary<string, DbSchemaLookups>,
        connectionString, xctor, prepare, reuseProvidedTypes, cache
    ) =

    if String.IsNullOrWhiteSpace connectionString then invalidArg "Connection" "Value is empty!" 
        
    let databaseRootType = ProvidedTypeDefinition(assembly, nameSpace, typeName, baseType = Some typeof<obj>, hideObjectMethods = true)

    let schemaLookups =
        schemaCache.GetOrAdd(
            connectionString,
            fun connectionString -> InformationSchema.getDbSchemaLookups(connectionString))
    
    let dbSchemas = schemaLookups.Schemas
                    |> Seq.map (fun s -> ProvidedTypeDefinition(s.Key, baseType = Some typeof<obj>, hideObjectMethods = true))
                    |> List.ofSeq
                  
    databaseRootType.AddMembers dbSchemas
    
    let customTypes =
        [ for schemaType in dbSchemas do
            let es = ProvidedTypeDefinition("Types", Some typeof<obj>, hideObjectMethods = true)
            for (KeyValue(_, enum)) in schemaLookups.Schemas.[schemaType.Name].Enums do
                let t = ProvidedTypeDefinition(enum.Name, Some typeof<string>, hideObjectMethods = true, nonNullable = true)
                for value in enum.Values do t.AddMember(ProvidedField.Literal(value, t, value))
                es.AddMember t
                let udtTypeName = sprintf "%s.%s" enum.Schema enum.Name
                yield udtTypeName, t
            schemaType.AddMember es
        ] |> Map.ofList
        
    for schemaType in dbSchemas do
        schemaType.AddMemberDelayed <| fun () ->
            createTableTypes(customTypes, schemaLookups.Schemas.[schemaType.Name])

    let commands = ProvidedTypeDefinition("Commands", None)
    databaseRootType.AddMember commands
    let providedTypeReuse = if reuseProvidedTypes then WithCache cache else NoReuse
    addCreateCommandMethod(connectionString, databaseRootType, commands, customTypes, schemaLookups, xctor, prepare, providedTypeReuse)

    databaseRootType           

let internal getProviderType(assembly, nameSpace, cache: ConcurrentDictionary<string, ProvidedTypeDefinition>, schemaCache: ConcurrentDictionary<string, DbSchemaLookups>) = 

    let providerType = ProvidedTypeDefinition(assembly, nameSpace, "NpgsqlConnection", Some typeof<obj>, hideObjectMethods = true)

    do 
        providerType.DefineStaticParameters(
            parameters = [ 
                ProvidedStaticParameter("Connection", typeof<string>) 
                ProvidedStaticParameter("XCtor", typeof<bool>, false) 
                ProvidedStaticParameter("Prepare", typeof<bool>, false)
                ProvidedStaticParameter("ReuseProvidedTypes", typeof<bool>, false) 
            ],
            instantiationFunction = (fun typeName args ->
                cache.GetOrAdd(
                    typeName,
                    fun typeName ->
                        createRootType (assembly, nameSpace, typeName, schemaCache,
                            unbox args.[0], unbox args.[1], unbox args.[2], unbox args.[3], cache)
                )   
            ) 
        )

        providerType.AddXmlDoc """
<summary>Typed access to PostgreSQL programmable objects: tables and functions.</summary> 
<param name='Connection'>String used to open a Postgresql database or the name of the connection string in the configuration file in the form of “name=&lt;connection string name&gt;”.</param>
<param name='Prepare'>If set the command will be executed as prepared. See Npgsql documentation for prepared statements.</param>
<param name='ReuseProvidedTypes'>Reuse the return type for commands that select data of identical shape. Please see the readme for details.</param>
"""
    providerType


 

