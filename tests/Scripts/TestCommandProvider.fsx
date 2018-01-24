#I "../../src/Runtime/bin/Debug/netstandard2.0"
#r "FSharp.Data.Npgsql.dll"
#r "Npgsql.dll"
#r "netstandard.dll"

open Npgsql
open FSharp.Data

[<Literal>]
let dvdRental = "Host=localhost;Username=postgres;Password=postgres;Database=dvdrental;Port=32768"

do
    use cmd = new NpgsqlCommand<"SELECT 42 AS Answer, current_date as today", dvdRental, Scripting = true>()

    cmd.Execute() |> Seq.exactlyOne |> printfn "%A"