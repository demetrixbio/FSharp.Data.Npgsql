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
    cmd.CommandText <- 
        sprintf "select * from public.actor where first_name  = '%s' and last_name =  '%s'" "Tom" "Hanks"

    //do 
    //    use reader = cmd.ExecuteReader(CommandBehavior.KeyInfo ||| CommandBehavior.SchemaOnly)
    //    [ for  c in reader.GetColumnSchema() ->  c.ColumnName, c.DataType, c.AllowDBNull, c.IsKey, c.IsAutoIncrement ] |> printfn "\nCols 1:\n%A"

    let t = new DataTable()
    
    use adapter = new NpgsqlDataAdapter(cmd)

    //adapter.FillSchema(t, SchemaType.Source) |>  ignore


    //for c in t.Columns do 
    //    if c.DataType = typeof<DateTime> 
    //    then c.DateTimeMode <- System.Data.DataSetDateTime.Local

    do 
        use reader = cmd.ExecuteReader(CommandBehavior.KeyInfo ||| CommandBehavior.SchemaOnly)
        t.Load(reader)
        t.Columns.["last_update"].AllowDBNull <- true
    [ for c in t.Columns -> c.ColumnName, c.DataType, c.AllowDBNull, c.AutoIncrement] |> printfn "\nCols 2:\n %A"


    let meta = new DataTableReader(t)
    [ for c in meta.GetSchemaTable().Columns -> c.ColumnName] |> printfn "\nCols 2:\n %A"

    //[ for r in meta.GetSchemaTable().Rows -> r.["ColumnName"], r.["DataType"], r.["AllowDBNull"], r.["AutoIncrement"]] |> printfn "\nCols 2:\n %A"


    //do
    //    let r = t.NewRow()
    //    r.["actor_id"] <- 4444
    //    r.["first_name"] <- "Tom"
    //    r.["last_name"] <- "Hanks"
    //    t.Rows.Add(r)

    //use b = new NpgsqlCommandBuilder(adapter)

    //let i = adapter.Update(t) 
    //printfn "Records affected %i" i



