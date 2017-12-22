
#I "../../packages"
#r "System.Transactions"
#r @"..\bin\Debug\Npgsql.dll"
#r @"C:\Users\dmorozov\.nuget\packages\system.threading.tasks.extensions\4.3.0\lib\portable-net45+win8+wp8+wpa81\System.Threading.Tasks.Extensions.dll"

open Npgsql
open System.Data

//type sbol_location_type = ragne = 0 | cut = 1

//do
//    use conn = new NpgsqlConnection("Host=localhost;Username=postgres;Password=Leningrad1;Database=lims")
//    conn.Open()
//    conn.TypeMapper.MapEnum<sbol_location_type>("part.sbol_location_type") |> ignore
//    let cmd = conn.CreateCommand()
//    cmd.CommandText <- "    
//        INSERT INTO part.location (coord1, coord2, is_fwd, type) 
//        VALUES (@coor1, @coor2, @is_fwd , @type)
//    "

//    NpgsqlCommandBuilder.DeriveParameters(cmd)
//    [ for p in cmd.Parameters -> p.ParameterName, p.NpgsqlDbType, p.SpecificType ] |> printfn "%A"


do 
    use conn = new NpgsqlConnection("Host=localhost;Username=postgres;Password=Leningrad1;Database=lims")
    //use cmd = new NpgsqlCommand("part.location", conn,  CommandType = CommandType.TableDirect)
    //use cmd = new NpgsqlCommand("select coord1, coord2, is_fwd, type from part.location", conn)
    use cmd = new NpgsqlCommand("select coord1, coord2, is_fwd, type from part.location", conn)
    conn.Open()
    use adapter = new NpgsqlDataAdapter(cmd);
    use b = new NpgsqlCommandBuilder(adapter)
    //conn.TypeMapper.MapEnum<sbol_location_type>() |> ignore
    let t = new DataTable()

    adapter.Fill(t) |> printf "Records loaded %i"
    
    //do 
    //    use cursor = cmd.ExecuteReader(CommandBehavior.KeyInfo ||| CommandBehavior.SchemaOnly)
    //    for c in cursor.GetColumnSchema() do
    //        t.Columns.Add(c.ColumnName, c.DataType ) |> ignore
        //t.Load cursor

    //let t2 = adapter.FillSchema(t, SchemaType.Source)
    //[ for c in t.Columns -> c.ColumnName, c.DataType, [ for p in c.ExtendedProperties -> string p] ] |> printfn "%A"

    do
        let r = t.NewRow()
        r.["coord1"] <- 12
        r.["coord2"] <- 42
        r.["is_fwd"] <- true
        r.["type"] <- "cut"
        t.Rows.Add(r)

    adapter.InsertCommand <- b.GetInsertCommand(true)
    adapter.InsertCommand.Parameters.["@type"].NpgsqlDbType <- NpgsqlTypes.NpgsqlDbType.Unknown
    try
        let i = adapter.Update(t) 
        printfn "Records affected %i" i    
    with _ ->
        [ for p in adapter.InsertCommand.Parameters -> p.ParameterName, p.NpgsqlDbType, p.SpecificType, p.Value ] |> printfn "%A"
        reraise()

