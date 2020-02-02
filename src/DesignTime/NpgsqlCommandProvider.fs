module FSharp.Data.Npgsql.DesignTime.NpgsqlCommandProvider

open System
open System.Data
open FSharp.Quotations
open Npgsql
open ProviderImplementation.ProvidedTypes
open FSharp.Data.Npgsql
open FSharp.Data.Npgsql.DesignTime.InformationSchema
open System

let internal createRootType
    (
        assembly, nameSpace, typeName, isHostedExecution, resolutionFolder, schemaCache: Cache<DbSchemaLookups>,
        sqlStatement, connectionStringOrName, resultType, singleRow, fsx, allParametersOptional, configType, config, prepare
    ) = 

    if String.IsNullOrWhiteSpace( connectionStringOrName) then invalidArg "Connection" "Value is empty!" 

    let connectionString = Configuration.readConnectionString(connectionStringOrName, configType, config, resolutionFolder)

    let schemaLookups =
        schemaCache.GetOrAdd(
            connectionString,
            lazy InformationSchema.getDbSchemaLookups(connectionString))
    
    //todo not possible with multiple statements
    if singleRow && not (resultType = ResultType.Records || resultType = ResultType.Tuples)
    then invalidArg "singleRow" "SingleRow can be set only for ResultType.Records or ResultType.Tuples."

    let (parameters, outputColumns, customTypes) = InformationSchema.extractParametersAndOutputColumns(connectionString, sqlStatement, resultType, allParametersOptional, schemaLookups)

    let cmdProvidedType = ProvidedTypeDefinition(assembly, nameSpace, typeName, Some typeof<``ISqlCommand Implementation``>, hideObjectMethods = true)

    customTypes
    |> Seq.map (fun s -> s.Value)
    |> List.ofSeq
    |> cmdProvidedType.AddMembers
    
    let commandBehaviour = if singleRow then CommandBehavior.SingleRow else CommandBehavior.Default

    let returnTypes = 
        outputColumns |> List.mapi (fun i cs ->
            QuotationsFactory.GetOutputTypes(
                cs, 
                resultType, 
                commandBehaviour, 
                hasOutputParameters = false, 
                allowDesignTimeConnectionStringReUse = (fsx && isHostedExecution),
                designTimeConnectionString = (if fsx then connectionString else null),
                typeNameSuffix = if outputColumns.Length > 1 then (i + 1).ToString () else ""))

    let useLegacyPostgis = 
        (parameters |> List.exists (fun p -> p.DataType.ClrType = typeof<LegacyPostgis.PostgisGeometry>))
        ||
        (outputColumns |> List.concat |> List.exists (fun c -> c.ClrType = typeof<LegacyPostgis.PostgisGeometry>))

    do  //ctors
        let designTimeConfig = 
            <@@ {
                SqlStatement = sqlStatement
                Parameters = %%Expr.NewArray( typeof<NpgsqlParameter>, parameters |> List.map QuotationsFactory.ToSqlParam)
                ResultType = resultType
                SingleRow = singleRow
                ResultSets = %%Expr.NewArray(typeof<ResultSetDefinition>, QuotationsFactory.BuildResultSetDefinitions outputColumns returnTypes)
                UseLegacyPostgis = useLegacyPostgis
                Prepare = prepare
            } @@>

        do
            QuotationsFactory.GetCommandCtors(
                cmdProvidedType, 
                designTimeConfig, 
                allowDesignTimeConnectionStringReUse = (fsx && isHostedExecution),
                ?connectionString  = (if fsx then Some connectionString else None), 
                factoryMethodName = "Create"
            )
            |> cmdProvidedType.AddMembers

    QuotationsFactory.AddTopLevelTypes cmdProvidedType parameters resultType customTypes returnTypes outputColumns

    cmdProvidedType

let internal getProviderType(assembly, nameSpace, isHostedExecution, resolutionFolder, cache: Cache<ProvidedTypeDefinition>, schemaCache : Cache<DbSchemaLookups>) = 

    let providerType = ProvidedTypeDefinition(assembly, nameSpace, "NpgsqlCommand", Some typeof<obj>, hideObjectMethods = true)

    providerType.DefineStaticParameters(
        parameters = [ 
            ProvidedStaticParameter("CommandText", typeof<string>) 
            ProvidedStaticParameter("Connection", typeof<string>) 
            ProvidedStaticParameter("ResultType", typeof<ResultType>, ResultType.Records) 
            ProvidedStaticParameter("SingleRow", typeof<bool>, false)   
            ProvidedStaticParameter("Fsx", typeof<bool>, false) 
            ProvidedStaticParameter("AllParametersOptional", typeof<bool>, false) 
            ProvidedStaticParameter("ConfigType", typeof<ConfigType>, ConfigType.JsonFile) 
            ProvidedStaticParameter("Config", typeof<string>, "") 
            ProvidedStaticParameter("Prepare", typeof<bool>, false) 
        ],             
        instantiationFunction = (fun typeName args ->
            cache.GetOrAdd(
                typeName, 
                lazy 
                    createRootType(
                        assembly, nameSpace, typeName, isHostedExecution, resolutionFolder, schemaCache,
                        unbox args.[0], unbox args.[1], unbox args.[2], unbox args.[3], unbox args.[4], unbox args.[5], unbox args.[6], unbox args.[7], unbox args.[8]
                    )
            )
        )
    )

    providerType.AddXmlDoc """
<summary>Typed representation of a SQL statement to execute against a PostgreSQL database.</summary> 
<param name='CommandText'>SQL statement to execute at the data source.</param>  
<param name='Connection'>String used to open a PostgreSQL database or the name of the connection string in the configuration file.</param>
<param name='ResultType'>A value that defines structure of result: Records, Tuples, DataTable or DataReader.</param>
<param name='SingleRow'>If set the query is expected to return a single row of the result set. See MSDN documentation for details on CommandBehavior.SingleRow.</param>
<param name='AllParametersOptional'>If set all parameters become optional. NULL input values must be handled inside SQL script.</param>
<param name='Fsx'>Re-use design time connection string for the type provider instantiation from *.fsx files.</param>
<param name='ConfigType'>JsonFile, Environment or UserStore. Default is JsonFile.</param>
<param name='Config'>JSON configuration file with connection string information. Matches 'Connection' parameter as name in 'ConnectionStrings' section.</param>
<param name='Prepare'>If set the command will be executed as prepared. See Npgsql documentation for prepared statements.</param>
"""
    
    providerType
