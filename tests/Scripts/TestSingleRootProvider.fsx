//#I "../bin/Debug/net461"
#I "../../src/bin/Debug/"

#r "System.Transactions"
#r "FSharp.Data.Npgsql.dll"
#r "Npgsql.dll"
#r "System.Threading.Tasks.Extensions"
#r "System.ValueTuple.dll"

open FSharp.Data

[<Literal>]
let connectionString = "Host=localhost;Username=postgres;Password=Leningrad1;Database=lims"

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
    t.AddRow("biological_process 3",  true, "http://purl.obolibrary.org/obo/go/go-basic.obo", id_user = 1)
    t.Rows.[0].ItemArray |> printfn "Values before update: %A"
    t.Update() |> printfn "Records affected %i"
    t.Rows.[0].ItemArray |> printfn "Values after update: %A"
    t.Rows.[0].description <- Some "sqwsqwswq"
    t.Update() |> printfn "Records affected %i"

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
    let t = cmd.Execute(id = 4)
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

    let row = t.NewRow(title="[copy]", content="",  updated=DateTime.Now, favorite = true, locked=false, parents=[| 1|], id_user=1 )
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
            INSERT INTO part.gsl_source_doc (id_user, title, content, favorite, parents) 
            VALUES (@id_user, @title, @content, @favorite, @parents) 
            RETURNING id
        ", SingleRow = true>(connectionString)
    cmd.Execute(1,  "test title", "test content", true, [| 1; 2 |]) |> printfn "Records inserted %A"

