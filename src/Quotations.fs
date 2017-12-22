module internal FSharp.Data.Quotations

let (|Arg1|) xs = 
    assert (List.length xs = 1)
    Arg1 xs.Head

let (|Arg2|) xs = 
    assert (List.length xs = 2)
    Arg2(xs.[0], xs.[1])

let (|Arg3|) xs = 
    assert (List.length xs = 3)
    Arg3(xs.[0], xs.[1], xs.[2])

let (|Arg4|) xs = 
    assert (List.length xs = 4)
    Arg4(xs.[0], xs.[1], xs.[2], xs.[3])

