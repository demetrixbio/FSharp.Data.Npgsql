#I "C:/Users/dmorozov/.nuget/packages"

#r "System.Transactions"
#r @"npgsql/3.2.6/lib/net451/Npgsql.dll"
#r @"system.threading.tasks.extensions\4.3.0\lib\portable-net45+win8+wp8+wpa81\System.Threading.Tasks.Extensions.dll"


open Npgsql
open System.Data 
open NpgsqlTypes

do
    use conn = new NpgsqlConnection("Host=localhost;Username=postgres;Password=postgres;Database=dvdrental;Port=32768")
    conn.Open()
    let cmd = conn.CreateCommand()
    //cmd.CommandText <- "SELECT @ratings::mpaa_rating[];"
    //cmd.Parameters.Add("ratings", NpgsqlDbType.Text ||| NpgsqlDbType.Array ).Value <- [| "PG-13"; "R" |]
    cmd.CommandText <- "SELECT @ratings::mpaa_rating;"
    cmd.Parameters.Add("ratings", NpgsqlDbType.Text).Value <- "PG-13"
    //let p = cmd.Parameters.AddWithValue("ratings", [| "PG-13"; "R" |] ).NpgsqlDbType |> printfn "Param type: %A"
    let x: NpgsqlDbType = enum -2147483629 ^^^ NpgsqlDbType.Array

    //do 
    //    use cursor = cmd.ExecuteReader(CommandBehavior.SchemaOnly ||| CommandBehavior.KeyInfo)
    //    [ for c in cursor.GetColumnSchema() -> c.ColumnName, c.DataType, c.DataTypeName, c.PostgresType ] |> printfn "%A"
    let res =  cmd.ExecuteScalar()

    printfn "Result: %A, type: %s" res (res.GetType().FullName)

