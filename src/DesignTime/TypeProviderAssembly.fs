namespace FSharp.Data

open System.Reflection
open Microsoft.FSharp.Core.CompilerServices
open ProviderImplementation.ProvidedTypes
open System.Collections.Concurrent

[<assembly:TypeProviderAssembly()>]
do()

[<TypeProvider>]
type NpgsqlProviders(config) as this = 
    inherit TypeProviderForNamespaces(config, [("FSharp.Data.Npgsql.DesignTime", "FSharp.Data.Npgsql")])
    
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
        do assert (typeof<``ISqlCommand Implementation``>.Assembly.GetName().Name = assembly.GetName().Name) 

        let resultType = 
            let runtime = Assembly.LoadFrom( config.RuntimeAssembly)
            runtime.GetType("FSharp.Data.ResultType")

        this.AddNamespace(
            nameSpace, [ 
                NpgsqlCommandProvider.getProviderType( assembly, nameSpace, cache, resultType)
                NpgsqlConnectionProvider.getProviderType( assembly, nameSpace, cache, resultType)
            ]
        )

    override this.ResolveAssembly args = 
        config.ReferencedAssemblies 
        |> Array.tryFind (fun x -> AssemblyName.ReferenceMatchesDefinition(AssemblyName.GetAssemblyName x, AssemblyName args.Name)) 
        |> Option.map Assembly.LoadFrom
        |> defaultArg 
        <| base.ResolveAssembly args

