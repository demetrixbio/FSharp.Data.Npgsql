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

