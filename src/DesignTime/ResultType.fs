namespace FSharp.Data

module ResultType = 
    [<Literal>]
    let internal Records = 0
    [<Literal>]
    let internal Tuples = 1
    [<Literal>]
    let internal DataTable = 2
    [<Literal>]
    let internal DataReader = 3

    let mutable typeHandle: System.Type = null

