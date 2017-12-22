#I "../../packages"
#r "System.Transactions"
//#r @"Npgsql.3.2.5/lib/net451/Npgsql.dll"
#r @"..\..\..\npgsql\src\Npgsql\bin\Debug\net451\Npgsql.dll"
#r @"C:\Users\dmorozov\.nuget\packages\system.threading.tasks.extensions\4.3.0\lib\portable-net45+win8+wp8+wpa81\System.Threading.Tasks.Extensions.dll"
//#r @"C:\Users\dmorozov\.nuget\packages\npgsql\3.2.5\lib\net451\Npgsql.dll"

open Npgsql
open System.Data

type sbol_location_type = range = 0 | cut = 1

do
    use conn = new NpgsqlConnection("Host=localhost;Username=postgres;Password=Leningrad1;Database=lims")
    conn.Open()
    //conn.TypeMapper.MapEnum<sbol_location_type>() |> ignore
    use cmd =  conn.CreateCommand()
    cmd.CommandText <- "SELECT * FROM part.location"
    let t = new DataTable()
    do 
        use reader = cmd.ExecuteReader(CommandBehavior.KeyInfo ||| CommandBehavior.SchemaOnly)
        t.Load(reader)

    [ for c in t.Columns -> c.ColumnName, c.DataType, [ for p in c.ExtendedProperties -> string p] ] |> printfn "%A"

    do
        let r = t.NewRow()
        r.["coord1"] <- 12
        r.["coord2"] <- 12
        r.["is_fwd"] <- true
        //r.["type"] <- sbol_location_type.cut
        r.["type"] <- "cut"
        t.Rows.Add(r)
   
    use adapter = new NpgsqlDataAdapter(cmd)
    adapter.ReturnProviderSpecificTypes <- true
    use b = new NpgsqlCommandBuilder(adapter)
    //adapter.InsertCommand <- b.GetInsertCommand()
    let i = adapter.Update(t) 
    printfn "Records affected %i" i



