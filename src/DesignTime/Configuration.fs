module FSharp.Data.Configuration

open Microsoft.Extensions.Configuration
open FSharp.Data

let readConnectionString(connectionString, configType, configFile) = 
    
    if configType = ConfigType.JsonFile && configFile = ""
    then 
        connectionString
    else
        let builder = 
            match configType with 
            | ConfigType.JsonFile -> 
                if not (System.IO.Path.IsPathRooted(configFile))
                then failwithf "Relative path %s is not allowed for config file." configFile
                ConfigurationBuilder().AddJsonFile(configFile)
            | ConfigType.EnvironmentVariables -> 
                ConfigurationBuilder().AddEnvironmentVariables()
            | ConfigType.EncryptedUserStore -> 
                ConfigurationBuilder().AddUserSecrets<``ISqlCommand Implementation``>()
            | _ -> 
                upcast ConfigurationBuilder()
            
        let config = builder.Build() 
        let allKeys = config.AsEnumerable() |> Seq.toArray |> Array.sortBy (fun x -> x.Key)
        match config.GetConnectionString(connectionString) with 
        | null -> connectionString
        | x -> x
        

