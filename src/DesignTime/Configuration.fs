module FSharp.Data.Configuration

open Microsoft.Extensions.Configuration
open FSharp.Data

let readConnectionString(connectionString, configType, config) = 
    
    if configType = ConfigType.JsonFile && config = ""
    then 
        connectionString
    else
        let builder = 
            match configType with 
            | ConfigType.JsonFile -> 
                if not (System.IO.Path.IsPathRooted(config))
                then failwithf "Relative path %s is not allowed for config file." config
                ConfigurationBuilder().AddJsonFile(config)
            | ConfigType.Environment -> 
                ConfigurationBuilder().AddEnvironmentVariables()
            | ConfigType.UserStore -> 
                ConfigurationBuilder().AddUserSecrets(config)
            | _ -> 
                upcast ConfigurationBuilder()
            
        let config = builder.Build() 
        let allKeys = config.AsEnumerable() |> Seq.toArray |> Array.sortBy (fun x -> x.Key)
        match config.GetConnectionString(connectionString) with 
        | null -> connectionString
        | x -> x
        

