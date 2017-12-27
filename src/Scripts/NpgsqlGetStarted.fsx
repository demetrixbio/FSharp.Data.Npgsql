#I "../../packages"
#r "System.Transactions"
//#r @"Npgsql.3.2.5/lib/net451/Npgsql.dll"
#r @"..\..\..\npgsql\src\Npgsql\bin\Debug\net451\Npgsql.dll"
#r @"C:\Users\dmorozov\.nuget\packages\system.threading.tasks.extensions\4.3.0\lib\portable-net45+win8+wp8+wpa81\System.Threading.Tasks.Extensions.dll"
//#r @"C:\Users\dmorozov\.nuget\packages\npgsql\3.2.6\lib\net451\Npgsql.dll"

open Npgsql
open System.Data
open NpgsqlTypes
open Npgsql.Logging
open System

type sbol_location_type = range = 0 | cut = 1

do
    //NpgsqlLogManager.Provider <- ConsoleLoggingProvider(NpgsqlLogLevel.Debug);
    //NpgsqlLogManager.IsParameterLoggingEnabled <- true

    use conn = new NpgsqlConnection("Host=localhost;Username=postgres;Password=Leningrad1;Database=lims")
    conn.Open()
    //conn.TypeMapper.MapEnum<sbol_location_type>() |> ignore
    use cmd =  conn.CreateCommand()
    //cmd.CommandText <- "SELECT * FROM part.location"
    cmd.CommandText <- "select * from onto.namespace where id = @id"
    cmd.Parameters.Add("id", NpgsqlDbType.Integer).Value <- 20

    do 
        use reader = cmd.ExecuteReader(CommandBehavior.KeyInfo ||| CommandBehavior.SchemaOnly)
        [ for  c in reader.GetColumnSchema() ->  c.ColumnName, c.DataType, c.NpgsqlDbType ] |> printfn "\nCols 1:\n%A"

    let t = new DataTable()
    
    use adapter = new NpgsqlDataAdapter(cmd)

    //adapter.FillSchema(t, SchemaType.Source) |>  ignore


    //for c in t.Columns do 
    //    if c.DataType = typeof<DateTime> 
    //    then c.DateTimeMode <- System.Data.DataSetDateTime.Local

    do 
        use reader = cmd.ExecuteReader(CommandBehavior.KeyInfo )
        t.Load(reader)

    [ for c in t.Columns -> c.ColumnName, c.DataType, c.DateTimeMode ] |> printfn "\nCols 2:\n %A"

    //do
    //    let r = t.NewRow()
    //    r.["coord1"] <- 12
    //    r.["coord2"] <- 12
    //    r.["is_fwd"] <- true
    //    //r.["type"] <- sbol_location_type.cut
    //    r.["type"] <- "cut"
    //    t.Rows.Add(r)

    do
        let r = t.Rows.[0]
        let v =  r.["created"] |> unbox<System.DateTime>
        r.["created"] <- DateTime.SpecifyKind(r.["created"] :?> _, DateTimeKind.Local)
        r.["valid_from"] <- DateTime.SpecifyKind(r.["valid_from"] :?> _, DateTimeKind.Local)
        r.["valid_to"] <- DateTime.SpecifyKind(r.["valid_to"] :?> _, DateTimeKind.Local)
        r.["name"] <- r.["name"].ToString() + "_test"
        let v =  r.["created"] |> unbox<System.DateTime>
        printfn "%A, %A" v v.Kind
        printfn "Before %A" r.["description"]
        r.["description"] <- "haha"
        printfn "After %A" r.["description"]
   
    //adapter.ReturnProviderSpecificTypes <- true
    use b = new NpgsqlCommandBuilder(adapter)
    //b.ConflictOption <- ConflictOption.OverwriteChanges
    //let updateCommand = b.GetUpdateCommand(useColumnsForParameterNames = true)
    //[ for p in updateCommand.Parameters -> p.ParameterName, p.DbType, p.NpgsqlDbType ] |>  printfn "Update params: %A"
    //adapter.InsertCommand <- b.GetInsertCommand()

    let i = adapter.Update(t) 
    printfn "Records affected %i" i


//System.Diagnostics.Process.GetCurrentProcess().Id


do
    use conn = new NpgsqlConnection("Host=localhost;Username=postgres;Password=Leningrad1;Database=lims")
    conn.Open()
    let cmd = new NpgsqlCommand("select @p1::timestamp", conn)
    let v =  DateTime.SpecifyKind( DateTime(2017, 12, 1), DateTimeKind.Local)
    //let v =  DateTime(2017, 12, 1)
    let p = cmd.Parameters.AddWithValue("p1", v)
    printfn "%A, %A, %A" p.NpgsqlDbType p.Value ((p.Value :?> DateTime).Kind)
    let result = cmd.ExecuteScalar() |>  unbox<DateTime>
    printfn "Result: %A, %A" result result.Kind