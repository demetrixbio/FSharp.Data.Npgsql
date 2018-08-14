//#I "../../packages"
#I @"..\bin\Debug\net461"
#r "System.Transactions"
#r @"Npgsql.dll"
#r @"Npgsql.LegacyPostgis.dll"
#r @"System.Threading.Tasks.Extensions.dll"
//#r @"C:\Users\dmorozov\.nuget\packages\npgsql\3.2.6\lib\net451\Npgsql.dll"

open Npgsql
open System.Data

do
    //NpgsqlLogManager.Provider <- ConsoleLoggingProvider(NpgsqlLogLevel.Debug);
    //NpgsqlLogManager.IsParameterLoggingEnabled <- true

let conn = new NpgsqlConnection("Host=localhost;Username=postgres;Password=postgres;Database=lims")

conn.Open()
let tx = conn.BeginTransaction()

let cmd = new NpgsqlCommand("select * from part.part where id = 1", conn, tx)

let t = new DataTable()
let cursor = cmd.ExecuteReader()
t.Load cursor
t.Rows.Count |> printfn "Rows: %i"

let adapter = new NpgsqlDataAdapter(cmd)
let builder = new NpgsqlCommandBuilder(adapter)

t.Rows.[0].["sequence"] <- t.Rows.[0].["sequence"].ToString() + "_test"

adapter.Update(t) |> printfn "Rows updated: %i"
