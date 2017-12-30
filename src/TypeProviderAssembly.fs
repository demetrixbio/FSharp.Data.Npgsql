namespace FSharp.Data

open System.Reflection
open Microsoft.FSharp.Core.CompilerServices
open ProviderImplementation.ProvidedTypes
open System.Collections.Concurrent

[<assembly:TypeProviderAssembly()>]
do()

[<TypeProvider>]
[<CompilerMessageAttribute("This API supports the FSharp.Data.Npgsql infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
type NpgsqlProviders(config) as this = 
    inherit TypeProviderForNamespaces(config)
    
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
        let assembly = Assembly.LoadFrom( config.RuntimeAssembly)

        this.AddNamespace(
            nameSpace, [ 
                NpgsqlCommandProvider.getProviderType( assembly, nameSpace, cache)
                NpgsqlConnectionProvider.getProviderType( assembly, nameSpace, cache)
            ]
        )

    override this.ResolveAssembly args = 
        config.ReferencedAssemblies 
        |> Array.tryFind (fun x -> AssemblyName.ReferenceMatchesDefinition(AssemblyName.GetAssemblyName x, AssemblyName args.Name)) 
        |> Option.map Assembly.LoadFrom
        |> defaultArg 
        <| base.ResolveAssembly args

