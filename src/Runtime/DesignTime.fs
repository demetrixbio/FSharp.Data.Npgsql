namespace FSharp.Data

[<assembly:CompilerServices.TypeProviderAssembly("FSharp.Data.Npgsql.DesignTime.dll")>]
do ()

//module internal ResultType = 
//    [<Literal>]
//    let Records = 0
//    [<Literal>]
//    let Tuples = 1
//    [<Literal>]
//    let DataTable = 2
//    [<Literal>]
//    let DataReader = 3

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