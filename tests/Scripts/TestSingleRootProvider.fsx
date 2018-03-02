#I "../../src/Runtime/bin/Debug/net461"
//#r @"C:\Users\dmorozov\.nuget\packages\npgsql\3.2.6\lib\netstandard2.0\Npgsql.dll"
#r @"C:\Users\dmorozov\Documents\GitHub\npgsql\src\Npgsql\bin\Debug\net451\Npgsql.dll"
#r "FSharp.Data.Npgsql.dll"
//#r "netstandard.dll"

open Npgsql.Logging
open Npgsql
open System

[<Literal>]
let dvdRental = "Host=localhost;Username=postgres;Database=dvdrental;Port=32768"

type DvdRental = FSharp.Data.Npgsql.NpgsqlConnection<dvdRental, Fsx = true>

NpgsqlLogManager.Provider <- ConsoleLoggingProvider(NpgsqlLogLevel.Debug);
NpgsqlLogManager.IsParameterLoggingEnabled <- true

let conn = new NpgsqlConnection(dvdRental)
conn.Open()

let t = new  DvdRental.``public``.Tables.actor()
t.AddRow(first_name = "Tom", last_name = "Hanks")

t.Update(conn) |> printfn "records affected %i"
conn.Close()
