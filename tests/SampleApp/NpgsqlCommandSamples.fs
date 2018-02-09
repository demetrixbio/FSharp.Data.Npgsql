module NpgsqlCommandSamples 

open Connection
open FSharp.Data.Npgsql

let basicQuery() = 
    use cmd = new NpgsqlCommand<"SELECT title, release_year FROM public.film LIMIT 3", dvdRental>(dvdRental)

    for x in cmd.Execute() do   
        printfn "Movie '%s' released in %i." x.title x.release_year.Value


type BasicQuery = NpgsqlCommand<"SELECT title, release_year FROM public.film LIMIT 3", dvdRental>

let basicQueryTypeAlias() = 
    use cmd = BasicQuery.Create(dvdRental)
    for x in cmd.Execute() do   
        printfn "Movie '%s' released in %i." x.title x.release_year.Value

let ``Parameterized query``() = 
    use cmd = new NpgsqlCommand<"SELECT title FROM public.film WHERE length > @longer_than", dvdRental>(dvdRental)
    let longerThan = System.TimeSpan.FromHours(3.)
    cmd.Execute(longer_than = int16 longerThan.TotalMinutes)
    |> Seq.toList 
    |> printfn "Movies longer than %A:\n%A" longerThan 

let singleton() = 
    use cmd = new NpgsqlCommand<"SELECT current_date as today", dvdRental, SingleRow = true>(dvdRental)
    cmd.Execute() |> printfn "Today is: %A"
