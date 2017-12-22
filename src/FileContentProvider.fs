module FSharp.Data.LiteralReaderProvider

open System.IO
open System.Collections.Concurrent
open ProviderImplementation.ProvidedTypes

let getProviderType(assembly, nameSpace, resolutionFolder, cache: ConcurrentDictionary<_, ProvidedTypeDefinition>) = 

    let providerType = ProvidedTypeDefinition(assembly, nameSpace, "FileContent", Some typeof<obj>, hideObjectMethods = true)

    providerType.DefineStaticParameters(
        parameters = [ ProvidedStaticParameter("Path", typeof<string>) ],             
        instantiationFunction = (fun typeName args ->
            cache.GetOrAdd(
                typeName, 
                fun _ -> 
                    let path = 
                        let path = args.[0] :?> _
                        if Path.IsPathRooted(path) then path else Path.Combine(resolutionFolder, path)
                    
                    if not( File.Exists( path)) 
                    then failwithf "Specified file [%s] could not be found" path
                    let contentField = ProvidedField.Literal("LiteralValue", typeof<string>, File.ReadAllText(path))
                    contentField.AddXmlDoc(sprintf "Content of '%s'" path)
                    let t = new ProvidedTypeDefinition(assembly, nameSpace, typeName, baseType = None, hideObjectMethods = true)
                    t.AddMember contentField
                    t
            )
        ) 
    )

    providerType
