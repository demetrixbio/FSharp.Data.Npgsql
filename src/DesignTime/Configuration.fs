module internal FSharp.Data.Configuration

open Microsoft.Extensions.Configuration
open FSharp.Data

[<Literal>]
let connectionStringsSection = "ConnectionStrings"

let readConnectionString(connectionString, configType, config) = 
    
    if configType = ConfigType.JsonFile && config = ""
    then 
        connectionString
    else
        let builder = 
            match configType with 
            | ConfigType.JsonFile -> 
                if not (System.IO.Path.IsPathRooted(config))
                then failwithf "Relative path %s is not allowed for config file. Use absolute path." config
                ConfigurationBuilder().AddJsonFile(config)
            | ConfigType.Environment -> 
                ConfigurationBuilder().AddEnvironmentVariables()
            | ConfigType.UserStore -> 
                ConfigurationBuilder().AddUserSecrets(config)
            | _ -> 
                upcast ConfigurationBuilder()
            
        let config = builder.Build() 
        let connectionStrings = config.GetSection( connectionStringsSection)

        if not( connectionStrings.Exists()) 
        then failwithf "%s section is missing in configuration" connectionStringsSection

        match connectionStrings.[connectionString] with 
        | null -> failwithf "Cannot find name %s in %s section of configuration." connectionString connectionStringsSection
        | x -> x
        

