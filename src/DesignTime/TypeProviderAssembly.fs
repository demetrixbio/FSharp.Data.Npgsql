namespace FSharp.Data

open System.Reflection
open Microsoft.FSharp.Core.CompilerServices
open ProviderImplementation.ProvidedTypes
open System.Collections.Concurrent

[<assembly:TypeProviderAssembly()>]
do()

[<TypeProvider>]
type NpgsqlProviders(config) as this = 
    inherit TypeProviderForNamespaces(config, assemblyReplacementMap = [(Const.designTimeComponent, "FSharp.Data.Npgsql")])
    
    [<Literal>]
    let nameSpace = "FSharp.Data"

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
        
        do 
            assert (typeof<``ISqlCommand Implementation``>.Assembly.GetName().Name = assemblyName) 
            assert (Const.designTimeComponent = assemblyName)
            
        this.AddNamespace(
            nameSpace, [ 
                NpgsqlCommandProvider.getProviderType(assembly, nameSpace, config.IsHostedExecution, config.ResolutionFolder, cache)
                NpgsqlConnectionProvider.getProviderType(assembly, nameSpace, config.IsHostedExecution, config.ResolutionFolder, cache)
            ]
        )

    //override this.ResolveAssembly args = 
    //    config.ReferencedAssemblies 
    //    |> Array.tryFind (fun x -> AssemblyName.ReferenceMatchesDefinition(AssemblyName.GetAssemblyName x, AssemblyName args.Name)) 
    //    |> Option.map Assembly.LoadFrom
    //    |> defaultArg 
    //    <| base.ResolveAssembly args

