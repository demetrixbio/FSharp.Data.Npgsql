open FSharp.Data.Npgsql

[<Literal>]
let dvdRental = "Host=localhost;Username=postgres;Database=dvdrental;Port=32768"

type MyCommand = NpgsqlCommand<"SELECT 42 AS Answer, current_date as today", dvdRental>

[<EntryPoint>]
let main _ =

    use cmd = new MyCommand(dvdRental)
    cmd.Execute() |> printfn "Result: %A"

    0 
