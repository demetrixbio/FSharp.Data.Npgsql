namespace FSharp.Data.Npgsql

open System.Reflection
open System.Threading
open Microsoft.FSharp.Core.CompilerServices
open ProviderImplementation.ProvidedTypes
open FSharp.Data.Npgsql.DesignTime
open System.IO
open Npgsql

[<TypeProvider>]
type NpgsqlProviders(config) as this = 
    inherit TypeProviderForNamespaces (config, assemblyReplacementMap = [("FSharp.Data.Npgsql.DesignTime", Path.GetFileNameWithoutExtension(config.RuntimeAssembly))], addDefaultProbingLocation = true)
    
    do 
        // register extension mappings
        NpgsqlConnection.GlobalTypeMapper.UseNetTopologySuite() |> ignore
        Interlocked.Increment(ref NpgsqlConnectionProvider.cacheInstanceCount) |> ignore
    
        this.Disposing.Add (fun _ ->
            if NpgsqlConnectionProvider.cacheInstanceCount = 1 then
                NpgsqlConnectionProvider.methodsCache.Clear ()
                NpgsqlConnectionProvider.typeCache.Clear ()
                NpgsqlConnectionProvider.schemaCache.Clear ()
            Interlocked.Decrement(ref NpgsqlConnectionProvider.cacheInstanceCount) |> ignore)

        let assembly = Assembly.GetExecutingAssembly()
        let assemblyName = assembly.GetName().Name
        let nameSpace = this.GetType().Namespace
        
        assert (typeof<ISqlCommandImplementation>.Assembly.GetName().Name = assemblyName) 

        this.AddNamespace (nameSpace, [ NpgsqlConnectionProvider.getProviderType (assembly, nameSpace) ])