#I "../../packages"
#r "System.Transactions"
#r "../bin/Debug/Npgsql.dll"
#r @"System.Threading.Tasks.Extensions.4.4.0/lib/portable-net45+win8+wp8+wpa81/System.Threading.Tasks.Extensions.dll"

open Npgsql
open System.Data

let printTypesMapping() = 
    use conn = new NpgsqlConnection()
    conn.ConnectionString <- "Host=localhost;Username=postgres;Password=Leningrad1;Database=postgres"
    conn.Open()
    //[ for x in conn.GetSchema().Rows -> x.["CollectionName"] ] |> printfn "%A"
    //let types = conn.GetSchema("DataType")
    /[ for r in conn.GetSchema("DataType").Rows -> r.["TypeName"], r.["ProviderDbType"], r.["DataType"] ] |> printfn "%A"

printTypesMapping()