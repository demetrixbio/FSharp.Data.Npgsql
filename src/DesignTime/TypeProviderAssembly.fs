namespace FSharp.Data.Npgsql

open DesignTime.InformationSchema
open System.Reflection
open Microsoft.FSharp.Core.CompilerServices
open ProviderImplementation.ProvidedTypes
open FSharp.Data.Npgsql.DesignTime
open System.IO
open Npgsql
open System.Collections.Concurrent

[<TypeProvider>]
type NpgsqlProviders(config) as this = 
    inherit TypeProviderForNamespaces(
        config, 
        assemblyReplacementMap = [("FSharp.Data.Npgsql.DesignTime", Path.GetFileNameWithoutExtension(config.RuntimeAssembly))],
        addDefaultProbingLocation = true
    )
    
    let cache = ConcurrentDictionary<string, ProvidedTypeDefinition>()
    let schemaCache = ConcurrentDictionary<string, DbSchemaLookups>()

    do 
        // register extension mappings
        Npgsql.NpgsqlConnection.GlobalTypeMapper.UseNetTopologySuite() |> ignore
    
        this.Disposing.Add <| fun _ ->
            try 
                NpgsqlConnectionProvider.methodsCache.Clear()
            with _ -> ()

        let assembly = Assembly.GetExecutingAssembly()
        let assemblyName = assembly.GetName().Name
        let nameSpace = this.GetType().Namespace
        
        assert (typeof<ISqlCommandImplementation>.Assembly.GetName().Name = assemblyName) 

        this.AddNamespace(
            nameSpace, [ 
                NpgsqlConnectionProvider.getProviderType(assembly, nameSpace, cache, schemaCache)
            ]
        )