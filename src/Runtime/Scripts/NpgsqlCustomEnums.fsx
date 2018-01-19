
//#I "../../packages"
#I @"..\bin\Debug\"

#r "System.Transactions"
#r @"..\bin\Debug\Npgsql.dll"
#r @"C:\Users\dmorozov\.nuget\packages\system.threading.tasks.extensions\4.3.0\lib\portable-net45+win8+wp8+wpa81\System.Threading.Tasks.Extensions.dll"

open Npgsql
open System.Data 

do
    use conn = new NpgsqlConnection("Host=localhost;Username=postgres;Password=postgres;Database=dvdrental")
    conn.Open()
    let cmd = conn.CreateCommand()
    cmd.CommandText <- "SELECT '{G,PG,PG-13,R,NC-17}'::mpaa_rating[]"

    do 
        use cursor = cmd.ExecuteReader(CommandBehavior.SchemaOnly ||| CommandBehavior.KeyInfo)
        [ for c in cursor.GetColumnSchema() -> c.ColumnName, c.DataType, c.DataTypeName, c.PostgresType ] |> printfn "%A"
    //cmd.Parameters.AddWithValue("rating", [| "G"; "PG"; "PG-13"; "R"; "NC-17" |]) |>  ignore
    let res =  cmd.ExecuteScalar()

    printfn "Result: %A, type: %s" res (res.GetType().FullName)

