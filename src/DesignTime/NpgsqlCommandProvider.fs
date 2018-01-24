module FSharp.Data.NpgsqlCommandProvider

open System
open System.Data

open Microsoft.FSharp.Quotations

open ProviderImplementation.ProvidedTypes

open Npgsql
open System.Collections.Concurrent

let createRootType(assembly, nameSpace, typeName, sqlStatement, connectionString, resultType, singleRow, scripting: bool, allParametersOptional, verifyOutputAtRuntime) = 

    if singleRow && not (resultType = ResultType.Records || resultType = ResultType.Tuples)
    then 
        invalidArg "singleRow" "SingleRow can be set only for ResultType.Records or ResultType.Tuples."
        
    if String.IsNullOrWhiteSpace( connectionString)
    then invalidArg "ConnectionStringOrName" "Value is empty!" 

    let parameters = InformationSchema.extractParameters(connectionString, sqlStatement, allParametersOptional)

    let customTypes = ref( dict [])

    let outputColumns = 
        if resultType <> ResultType.DataReader
        then InformationSchema.getOutputColumns(connectionString, sqlStatement, CommandType.Text, parameters, customTypes)
        else []

    let cmdProvidedType = ProvidedTypeDefinition(assembly, nameSpace, typeName, Some typeof<``ISqlCommand Implementation``>, hideObjectMethods = true)

    cmdProvidedType.AddMembers [ for x in customTypes.Value.Values -> x ]
    
    let rank = if singleRow then ResultRank.SingleRow else ResultRank.Sequence
    let returnType = QuotationsFactory.GetOutputTypes(outputColumns, resultType, rank, hasOutputParameters = false)

    do
        if resultType = ResultType.Records then
            returnType.PerRow 
            |> Option.filter (fun x -> x.Provided <> x.ErasedTo && outputColumns.Length > 1 )
            |> Option.iter (fun x -> cmdProvidedType.AddMember x.Provided)

        elif resultType = ResultType.DataTable then
            returnType.Single |> cmdProvidedType.AddMember

    do  //ctors
        let designTimeConfig = 
            let expectedColumns = 
                if verifyOutputAtRuntime 
                then [ for c in outputColumns -> c.ToDataColumnExpr() ]
                else []
                

            <@@ {
                SqlStatement = sqlStatement
                IsStoredProcedure = false
                Parameters = %%Expr.NewArray( typeof<NpgsqlParameter>, parameters |> List.map QuotationsFactory.ToSqlParam)
                ResultType = resultType
                Rank = rank
                Row2ItemMapping = %%returnType.Row2ItemMapping
                SeqItemTypeName = %%returnType.SeqItemTypeName
                ExpectedColumns = %%Expr.NewArray(typeof<DataColumn>, expectedColumns)
            } @@>

        do
            QuotationsFactory.GetCommandCtors(
                cmdProvidedType, 
                designTimeConfig, 
                ?connectionString  = (if scripting then Some connectionString else None), 
                factoryMethodName = "Create"
            )
            |> cmdProvidedType.AddMembers

    do  //AsyncExecute, Execute, and ToTraceString

        let executeArgs = QuotationsFactory.GetExecuteArgs(parameters, !customTypes)

        let hasOutputParameters = false
        let addRedirectToISqlCommandMethod outputType name = 
            QuotationsFactory.AddGeneratedMethod(parameters, hasOutputParameters, executeArgs, cmdProvidedType.BaseType, outputType, name) 
            |> cmdProvidedType.AddMember

        addRedirectToISqlCommandMethod returnType.Single "Execute" 
                            
        let asyncReturnType = ProvidedTypeBuilder.MakeGenericType(typedefof<_ Async>, [ returnType.Single ])
        addRedirectToISqlCommandMethod asyncReturnType "AsyncExecute" 

    cmdProvidedType

let getProviderType(assembly, nameSpace, cache: ConcurrentDictionary<_, ProvidedTypeDefinition>) = 

    let providerType = ProvidedTypeDefinition(assembly, nameSpace, "NpgsqlCommand", Some typeof<obj>, hideObjectMethods = true)

    providerType.DefineStaticParameters(
        parameters = [ 
            ProvidedStaticParameter("CommandText", typeof<string>) 
            ProvidedStaticParameter("Connection", typeof<string>) 
            ProvidedStaticParameter("ResultType", typeof<ResultType>, ResultType.Records) 
            ProvidedStaticParameter("SingleRow", typeof<bool>, false)   
            ProvidedStaticParameter("Scripting", typeof<bool>, false) 
            ProvidedStaticParameter("AllParametersOptional", typeof<bool>, false) 
            ProvidedStaticParameter("VerifyOutputAtRuntime", typeof<bool>, false) 
        ],             
        instantiationFunction = (fun typeName args ->
            cache.GetOrAdd(
                typeName, 
                fun _ -> 
                    createRootType(
                        assembly, 
                        nameSpace, 
                        typeName, 
                        args.[0] :?> _, 
                        args.[1] :?> _, 
                        args.[2] :?> _, 
                        args.[3] :?> _, 
                        args.[4] :?> _, 
                        args.[5] :?> _, 
                        args.[6] :?> _
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
"""

    providerType
