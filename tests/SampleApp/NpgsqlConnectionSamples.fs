module NpgsqlConnectionSamples

open FSharp.Data.Npgsql
open Connection
open System

type DvdRental = NpgsqlConnection<dvdRental>

let ``Basic query``() = 
    use cmd = DvdRental.CreateCommand<"SELECT title, release_year FROM public.film LIMIT 3">(dvdRental)

    for x in cmd.Execute() do   
        printfn "Movie '%s' released in %i." x.title x.release_year.Value

let ``Parameterized query``() = 
    use cmd = DvdRental.CreateCommand<"SELECT title FROM public.film WHERE length > @longer_than">(dvdRental)
    let longerThan = TimeSpan.FromHours(3.)
    let xs: string list = cmd.Execute(longer_than = int16 longerThan.TotalMinutes) |> Seq.toList 
    printfn "Movies longer than %A:\n%A" longerThan xs

let singleton() = 
    use cmd = DvdRental.CreateCommand<"SELECT current_date as today", SingleRow = true>(dvdRental)
    cmd.Execute() |> printfn "Today is: %A"

let asyncBasicQuery() = 
    use cmd = DvdRental.CreateCommand<"SELECT title, release_year FROM public.film LIMIT 3">(dvdRental)
    for x in cmd.AsyncExecute() |>  Async.RunSynchronously do   
        printfn "Movie '%s' released in %i." x.title x.release_year.Value

let basicQueryTuples() = 
    use cmd = DvdRental.CreateCommand<"SELECT title, release_year FROM public.film LIMIT 3", ResultType.Tuples>(dvdRental)
    for title, releaseYear in cmd.Execute() do   
        printfn "Movie '%s' released in %i." title releaseYear.Value

//type DvdRentalForScripting = NpgsqlConnection<dvdRental, Fsx = true>
//let basicQueryFsx() = 
//    use cmd = DvdRentalForScripting.CreateCommand<"SELECT title, release_year FROM public.film LIMIT 3">()
//    ()

let tx() =
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tx = conn.BeginTransaction()
    use cmd = 
        DvdRental.CreateCommand<"        
            INSERT INTO public.actor (first_name, last_name)
            VALUES(@firstName, @lastName)
        ", XCtor = true>(conn, tx)
    assert(cmd.Execute(firstName = "Tom", lastName = "Hanks") = 1)
    //Commit to persist changes
    //tx.Commit()

type DvdRentalXCtor = NpgsqlConnection<dvdRental, XCtor = true>
let txGlobal() =
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tx = conn.BeginTransaction()
    use cmd = 
        DvdRentalXCtor.CreateCommand<"        
            INSERT INTO public.actor (first_name, last_name)
            VALUES(@firstName, @lastName)
        ">(conn, tx)
    assert(cmd.Execute(firstName = "Tom", lastName = "Hanks") = 1)
    //Commit to persist changes
    //tx.Commit()

let insertOnly() = 
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tx = conn.BeginTransaction()
    let t = new DvdRental.``public``.Tables.actor()

    let r = t.NewRow(first_name = "Tom", last_name = "Hanks")
    t.Rows.Add(r)

    //or
    //t.AddRow(first_name = "Tom", last_name = "Hanks")
    //let r = t.Rows.[0] 

    assert( t.Update(conn, tx) = 1)
    printfn "Identity 'actor_id' %i and column with default 'last update': %A auto-fetched." r.actor_id r.last_update

let binaryImport() =
    let firstName, lastName = "Tom", "Hanks"
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tx = conn.BeginTransaction()

    let actors = new DvdRental.``public``.Tables.actor()
        
    let actor_id = 
        use cmd = DvdRental.CreateCommand<"select nextval('actor_actor_id_seq' :: regclass)::int", SingleRow = true, XCtor = true>(conn, tx)
        cmd.Execute() |> Option.flatten 
    
    //Binary copy operation expects all columns including auto-generated and having defaults must be populated. 
    actors.AddRow(actor_id, first_name = "Tom", last_name = "Hanks", last_update = Some DateTime.Now)
    actors.BinaryImport(conn)

    use cmd = 
        DvdRental.CreateCommand<
            "SELECT COUNT(*) FROM public.actor WHERE first_name = @firstName AND last_name = @lastName", 
            SingleRow = true, 
            XCtor = true>(conn, tx)

    assert(Some( Some 1L) = cmd.Execute(firstName, lastName))
