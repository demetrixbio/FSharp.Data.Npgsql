namespace FSharp.Data

///<summary>Enum describing output type</summary>
type ResultType =
///<summary>Sequence of custom records with properties matching column names and types</summary>
    | Records = 0
///<summary>Sequence of tuples matching column types with the same order</summary>
    | Tuples = 1
///<summary>Typed DataTable <see cref='T:FSharp.Data.DataTable`1'/></summary>
    | DataTable = 2
///<summary>raw DataReader</summary>
    | DataReader = 3

//namespace FSharp.Data.DesignTime

namespace FSharp.Data.Runtime
//this is mess. Clean up later.
type Configuration = {
    ResultsetRuntimeVerification: bool
}   

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
[<AutoOpen>]
module Configuration = 
    let private guard = obj()
    let private current = ref { Configuration.ResultsetRuntimeVerification = false }

    type Configuration with
        static member Current 
            with get() = lock guard <| fun() -> !current
            and set value = lock guard <| fun() -> current := value
