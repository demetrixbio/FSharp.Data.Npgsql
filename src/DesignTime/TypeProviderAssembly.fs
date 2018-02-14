namespace FSharp.Data.Npgsql

open System.Reflection
open Microsoft.FSharp.Core.CompilerServices
open ProviderImplementation.ProvidedTypes
open System.Collections.Concurrent
open FSharp.Data.Npgsql.DesignTime
open System.IO

[<assembly:TypeProviderAssembly()>]
do()

[<TypeProvider>]
type NpgsqlProviders(config) as this = 
    inherit TypeProviderForNamespaces(config, assemblyReplacementMap = [(Const.designTimeComponent, Path.GetFileNameWithoutExtension(config.RuntimeAssembly))])
    
    let cache = ConcurrentDictionary()

    do 
        this.Disposing.Add <| fun _ ->
            try 
                cache.Clear()
                NpgsqlConnectionProvider.methodsCache.Clear()
            with _ -> ()
    do 
        let assembly = Assembly.GetExecutingAssembly()
        let assemblyName = assembly.GetName().Name
        let nameSpace = this.GetType().Namespace
        
        assert (typeof<``ISqlCommand Implementation``>.Assembly.GetName().Name = assemblyName) 
            
        this.AddNamespace(
            nameSpace, [ 
                NpgsqlCommandProvider.getProviderType(assembly, nameSpace, config.IsHostedExecution, config.ResolutionFolder, cache)
                NpgsqlConnectionProvider.getProviderType(assembly, nameSpace, config.IsHostedExecution, config.ResolutionFolder, cache)
            ]
        )

