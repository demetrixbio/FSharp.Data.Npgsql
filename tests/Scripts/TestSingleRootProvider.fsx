//#I "../bin/Debug/net461"
#I "../../src/bin/Debug/"

#r "System.Transactions"
#r "FSharp.Data.Npgsql.dll"
#r "Npgsql.dll"
#r "System.Threading.Tasks.Extensions"
#r "System.ValueTuple.dll"

open FSharp.Data
open System.Data
open System

[<Literal>]
let dvdrental = "Host=localhost;Username=postgres;Password=postgres;Database=dvdrental"

type DvdRental = Npgsql<dvdrental>

//do  
//    let rental = DvdRental.``public``.Tables.rental()
//    let r = rental.Rows.[0]
//    r.

[<Literal>]
let connectionString = "Host=localhost;Username=postgres;Password=postgres;Database=lims"

type Db = Npgsql<connectionString>

type arc_direction = Db.material.Types.arc_direction

do 
    let t = new Db.material.Tables.arc()
    t.AddRow(arc_direction.pt, 12, 12)
    let row = t.Rows.[0]
    let dir: arc_direction = row. ``type``
    t.Update() |> ignore


do 
    let t = new Db.onto.Tables.``namespace``()
    [ for c in t.Columns -> c.ColumnName, c.DataType, c.DateTimeMode ] |> printfn "Columns: %A"
    //for c in t.Columns do
    //    if c.DataType = typeof<DateTime>
    //    then 
    //        c.DateTimeMode <- DataSetDateTime.Local

    t.AddRow("biological_process 3",  true, "http://purl.obolibrary.org/obo/go/go-basic.obo", id_user = 1, description = Some "test")
    let r = t.Rows.[0]
    r.ItemArray |> printfn "Values before update: %A"
    t.Update(conflictOption = ConflictOption.CompareAllSearchableValues) |> printfn "Records affected %i"
    r.ItemArray |> printfn "Values after update: %A"
    r.description <- r.description |>  Option.map (fun x -> x + "tail")
    t.Update(conflictOption = ConflictOption.CompareAllSearchableValues) |> printfn "Records affected %i"

do 
    let t = new Db.onto.Tables.``namespace``()
    do
        //use cmd = new Db.CreateCommand<"select * from onto.namespace where id = @id", ResultType.DataTable>(connectionString)
        use cmd = new NpgsqlCommand<"select * from onto.namespace where id = @id", connectionString, ResultType.DataReader>(connectionString)
        t.Load(cmd.Execute(20))
    [ for c in t.Columns -> c.ColumnName, c.DataType, c.DateTimeMode ] |> printfn "Columns: %A"

    if (t.Rows.Count > 0)
    then 
        let r = t.Rows.[0]
        r.name <- r.name + "_test"
        t.Update(conflictOption = System.Data.ConflictOption.CompareAllSearchableValues ) |> printfn "Rows affected: %i"


do
    let cmd = Db.CreateCommand<"select 42 as X, current_date", SingleRow = true>(connectionString)
    cmd.Execute() |> printfn "%A"

do
    use cmd = 
        Db.CreateCommand<"
            SELECT table_schema || '.' || table_name as name, table_type, row_number() over() as c
            FROM information_schema.tables 
            WHERE table_type = @tableType;
        ">(connectionString)

    let xs = [ for x in cmd.Execute(tableType = "VIEW") -> x.name, x.table_type, x.c ] 
    xs |> printfn "resultset:\n%A"

do
    let cmd = Db.CreateCommand<"select * from onto.namespace where id = @id", ResultType.DataTable>(connectionString)
    let t = cmd.Execute(id = 20)
    t.Rows.[0].name <- t.Rows.[0].name + "_test"
    t.Update() |> printfn "Rows affected: %i"

do
    use cmd = Db.CreateCommand<"select id, name from onto.namespace", ResultType.Tuples >(connectionString)

    cmd.Execute() |> Seq.toList |> printfn "resultset:\n%A"

do 
    use cmd = Db.CreateCommand<"select count(*) as c from onto.namespace", SingleRow = true>(connectionString)
    cmd.Execute() |> printfn "resultset:\n%A"

type location_type = Db.part.Types.sbol_location_type

let location = new Db.part.Tables.location()
location.AddRow(coord1 = 17, is_fwd = true, ``type`` = location_type.cut, coord2 = Some 42)
printfn "Records affected %i" <| location.Update()
printfn "New id: %i" location.Rows.[0].id 


open System
do
    let t = new Db.part.Tables.gsl_source_doc()

    let row = t.NewRow(title="[copy]", content="", created = DateTime.Now, updated=DateTime.Now, favorite = true, locked=false, parents=[| 1|], id_user=1 )
    t.Rows.Add(row)

    t.AddRow(title="[copy]", content="",  updated=DateTime.Now, created = DateTime.Now, favorite = true, locked=false, parents=[| 1|], id_user=1 )
    let row = t.Rows.[t.Rows.Count - 1]

    let inserted = t.Update() 
    printfn "Records inserted %i" inserted

do
    use cmd = 
        Db.CreateCommand<"
            SELECT 
                'pt'::material.arc_direction as col1,
                'tp'::material.arc_direction as col2
        ">(connectionString)
    cmd.Execute() 
    |> Seq.map (fun x -> string x.col1.Value, string x.col2.Value) 
    |> Seq.toList
    |> printfn "resultset:\n%A"

do
    use cmd = 
        Db.CreateCommand<"
            INSERT INTO part.location (coord1, coord2, is_fwd, type) 
            VALUES (@coor1, @coor2, @is_fwd , @type)
        ">(connectionString)
    cmd.Execute(12, 12, true, location_type.cut) |> printfn "Records inserted %i"

do
    use cmd = 
        Db.CreateCommand<"
        INSERT INTO part.gsl_source_doc (id_user, title, content, favorite, parents, locked, created, updated) 
        VALUES (@id_user, @title, @content, @favorite,@parents, true, @created, current_timestamp) 
        RETURNING id
        ", SingleRow = true>(connectionString)
    cmd.Execute(1,  "test title", "test content", true, [| 1; 2 |], created = DateTime.Now ) |> printfn "Records inserted %A"



