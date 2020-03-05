module internal FSharp.Data.Npgsql.DesignTime.Configuration

open Microsoft.Extensions.Configuration
open System.Xml.Linq
open System.Xml.XPath
open System.IO
open FSharp.Data.Npgsql

[<Literal>]
let connectionStringsSection = "ConnectionStrings"

let readUserSecretIdFromProjectFile resolutionFolder = 
    match Directory.GetFiles(resolutionFolder, "*.fsproj") with 
    | [| fsproj |] -> 
        match XDocument.Load( fsproj).XPathSelectElement( "/Project/PropertyGroup/UserSecretsId") with 
        | null -> ""
        | node -> node.Value
    | _ -> ""        
    
let readConnectionString(connectionString, configType, config, resolutionFolder) = 
    
    if configType = ConfigType.JsonFile && config = ""
    then 
        connectionString
    else
        let builder =
            match configType with 
            | ConfigType.JsonFile -> 
                if not( Path.IsPathRooted( config))
                then failwithf "Relative path %s is not allowed for config file. Use absolute path." config
                ConfigurationBuilder().AddJsonFile(config)
            | ConfigType.Environment -> 
                ConfigurationBuilder().AddEnvironmentVariables()
            | ConfigType.UserStore -> 
                let userSecretsId = 
                    if config <> ""
                    then 
                        config
                    else
                        readUserSecretIdFromProjectFile resolutionFolder

                if userSecretsId = "" 
                then failwith "Provide UserSecretsId value via Config parameter."

                ConfigurationBuilder().AddUserSecrets(userSecretsId)
            | _ -> 
                upcast ConfigurationBuilder()
            
        let config = builder.Build() 
        let connectionStrings = config.GetSection( connectionStringsSection)

        if not( connectionStrings.Exists()) 
        then failwithf "%s section is missing in configuration" connectionStringsSection

        match connectionStrings.[connectionString] with 
        | null -> failwithf "Cannot find name %s in %s section of configuration." connectionString connectionStringsSection
        | x -> x
        

