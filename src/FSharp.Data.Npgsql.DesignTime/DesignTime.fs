namespace FSharp.Data.Npgsql

open System.Reflection
open Microsoft.FSharp.Core.CompilerServices
open ProviderImplementation.ProvidedTypes
open FSharp.Data.Npgsql
open FSharp.Data.Npgsql.DesignTime
open FSharp.Data.Npgsql.DesignTime.InformationSchema
open System.IO

[<TypeProvider>]
type NpgsqlProviders(config) as this = 
    inherit TypeProviderForNamespaces(
        config, 
        assemblyReplacementMap = [("FSharp.Data.Npgsql.DesignTime", Path.GetFileNameWithoutExtension(config.RuntimeAssembly))],
        addDefaultProbingLocation = true
    )
    
    let cache = Cache<ProvidedTypeDefinition>()
    let schemaCache = Cache<DbSchemaLookups>()

    do 
        this.Disposing.Add <| fun _ ->
            try 
                NpgsqlConnectionProvider.methodsCache.Clear()
            with _ -> ()
    do
        let assembly = Assembly.GetExecutingAssembly()
        let assemblyName = assembly.GetName().Name
        let nameSpace = this.GetType().Namespace
        
        assert (typeof<``ISqlCommand Implementation``>.Assembly.GetName().Name = assemblyName) 

        this.AddNamespace(
            nameSpace, [ 
                NpgsqlCommandProvider.getProviderType(assembly, nameSpace, config.IsHostedExecution, config.ResolutionFolder, cache, schemaCache)
                NpgsqlConnectionProvider.getProviderType(assembly, nameSpace, config.IsHostedExecution, config.ResolutionFolder, cache, schemaCache)
            ]
        )
        
[<TypeProviderAssembly>]
do ()