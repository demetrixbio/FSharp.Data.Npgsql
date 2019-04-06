﻿module FSharp.Data.Npgsql.DesignTime.NpgsqlCommandProvider

open System
open System.Data
open System.Collections.Concurrent
open FSharp.Quotations
open Npgsql
open ProviderImplementation.ProvidedTypes
open FSharp.Data.Npgsql

let createRootType
    (
        assembly, nameSpace, typeName, isHostedExecution, resolutionFolder,
        sqlStatement, connectionStringOrName, resultType, singleRow, fsx, allParametersOptional, configType, config
    ) = 

    if String.IsNullOrWhiteSpace( connectionStringOrName) then invalidArg "Connection" "Value is empty!" 

    let connectionString = Configuration.readConnectionString(connectionStringOrName, configType, config, resolutionFolder)

    if singleRow && not (resultType = ResultType.Records || resultType = ResultType.Tuples)
    then invalidArg "singleRow" "SingleRow can be set only for ResultType.Records or ResultType.Tuples."

    let parameters = InformationSchema.extractParameters(connectionString, sqlStatement, allParametersOptional)

    let customTypes = ref( dict [])

    let outputColumns = 
        if resultType <> ResultType.DataReader
        then InformationSchema.getOutputColumns(connectionString, sqlStatement, CommandType.Text, parameters, customTypes)
        else []

    let cmdProvidedType = ProvidedTypeDefinition(assembly, nameSpace, typeName, Some typeof<``ISqlCommand Implementation``>, hideObjectMethods = true)

    cmdProvidedType.AddMembers [ for x in customTypes.Value.Values -> x ]
    
    let commandBehaviour = if singleRow then CommandBehavior.SingleRow else CommandBehavior.Default

    let returnType = 
        QuotationsFactory.GetOutputTypes(
            outputColumns, 
            resultType, 
            commandBehaviour, 
            hasOutputParameters = false, 
            allowDesignTimeConnectionStringReUse = (fsx && isHostedExecution),
            designTimeConnectionString = (if fsx then connectionString else null)
        )

    do
        if resultType = ResultType.Records 
        then
            returnType.PerRow 
            |> Option.filter (fun x -> x.Provided <> x.ErasedTo && outputColumns.Length > 1 )
            |> Option.iter (fun x -> cmdProvidedType.AddMember x.Provided)

        elif resultType = ResultType.DataTable 
        then
            returnType.Single |> cmdProvidedType.AddMember

    let useLegacyPostgis = 
        (parameters |> List.exists (fun p -> p.DataType.ClrType = typeof<LegacyPostgis.PostgisGeometry>))
        ||
        (outputColumns |> List.exists (fun c -> c.ClrType = typeof<LegacyPostgis.PostgisGeometry>))


    do  //ctors
        let designTimeConfig = 

            <@@ {
                SqlStatement = sqlStatement
                Parameters = %%Expr.NewArray( typeof<NpgsqlParameter>, parameters |> List.map QuotationsFactory.ToSqlParam)
                ResultType = resultType
                SingleRow = singleRow
                Row2ItemMapping = %%returnType.Row2ItemMapping
                SeqItemTypeName = %%returnType.SeqItemTypeName
                ExpectedColumns = %%Expr.NewArray(typeof<DataColumn>, [ for c in outputColumns -> c.ToDataColumnExpr() ])
                UseLegacyPostgis = useLegacyPostgis
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

    do  
        let executeArgs = QuotationsFactory.GetExecuteArgs(parameters, !customTypes)

        let hasOutputParameters = false
        let addRedirectToISqlCommandMethod outputType name = 
            QuotationsFactory.AddGeneratedMethod(parameters, hasOutputParameters, executeArgs, cmdProvidedType.BaseType, outputType, name) 
            |> cmdProvidedType.AddMember

        addRedirectToISqlCommandMethod returnType.Single "Execute" 
        addRedirectToISqlCommandMethod returnType.Single "ExecutePrepared" 
                            
        let asyncReturnType = ProvidedTypeBuilder.MakeGenericType(typedefof<_ Async>, [ returnType.Single ])
        addRedirectToISqlCommandMethod asyncReturnType "AsyncExecute" 
        addRedirectToISqlCommandMethod asyncReturnType "AsyncExecutePrepared" 

    cmdProvidedType

let getProviderType(assembly, nameSpace, isHostedExecution, resolutionFolder, cache: ConcurrentDictionary<_, ProvidedTypeDefinition>) = 

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
        ],             
        instantiationFunction = (fun typeName args ->
            cache.GetOrAdd(
                typeName, 
                fun _ -> 
                    createRootType(
                        assembly, nameSpace, typeName, isHostedExecution, resolutionFolder,
                        unbox args.[0],  unbox args.[1],  unbox args.[2], unbox args.[3], unbox args.[4], unbox args.[5], unbox args.[6], unbox args.[7]
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
"""
    providerType
