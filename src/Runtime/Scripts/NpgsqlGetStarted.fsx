//#I "../../packages"
#I @"..\bin\Debug\"
#r "System.Transactions"
#r @"C:\Users\dmorozov\.nuget\packages\npgsql\3.2.6\lib\net451\Npgsql.dll"
#r @"C:\Users\dmorozov\.nuget\packages\system.threading.tasks.extensions\4.4.0\lib\portable-net45+win8+wp8+wpa81\System.Threading.Tasks.Extensions.dll"
//#r @"C:\Users\dmorozov\.nuget\packages\npgsql\3.2.6\lib\net451\Npgsql.dll"

open Npgsql
open System.Data
open NpgsqlTypes
open System

do
    //NpgsqlLogManager.Provider <- ConsoleLoggingProvider(NpgsqlLogLevel.Debug);
    //NpgsqlLogManager.IsParameterLoggingEnabled <- true

    use conn = new NpgsqlConnection("Host=localhost;Username=postgres;Database=dvdrental;Port=32768")
    conn.Open()
    use cmd =  conn.CreateCommand()

    //cmd.CommandText <- "select now()"
    //let now = cmd.ExecuteScalar() |> unbox<DateTime>
    //printfn "Now: %A" now

    let now = DateTime.Now.AddMinutes(10.)
    cmd.CommandText <- "select now() < @p"
    cmd.Parameters.AddWithValue("p", NpgsqlDbType.TimestampTZ, now) |> ignore
    cmd.ExecuteScalar() |> printfn "Result: %A"

    cmd.CommandText <- "select concat(now(), '/ - /', @p)"
    cmd.Parameters.AddWithValue("p", NpgsqlDbType.TimestampTZ, now) |> ignore
    cmd.ExecuteScalar() |> printfn "Result: %A"


