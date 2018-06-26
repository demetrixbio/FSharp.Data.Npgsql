module NpgsqlCommandSamples 

open Connection
open Npgsql
open FSharp.Data.Npgsql

let basicQuery() = 
    use cmd = new NpgsqlCommand<"SELECT title, release_year FROM public.film LIMIT 3", dvdRental>(dvdRental)

    for x in cmd.Execute() do   
        printfn "Movie '%s' released in %i." x.title x.release_year.Value

type BasicQuery = NpgsqlCommand<"SELECT title, release_year FROM public.film LIMIT 3", dvdRental>

let basicQueryTypeAlias() = 
    use cmd = BasicQuery.Create(dvdRental)
    for x in cmd.Execute() do   
        printfn "Movie '%s' released in %i." x.title x.release_year.Value

let ``Parameterized query``() = 
    use cmd = new NpgsqlCommand<"SELECT title FROM public.film WHERE length > @longer_than", dvdRental>(dvdRental)
    let longerThan = System.TimeSpan.FromHours(3.)
    cmd.Execute(longer_than = int16 longerThan.TotalMinutes)
    |> Seq.toList 
    |> printfn "Movies longer than %A:\n%A" longerThan 

let singleton() = 
    use cmd = new NpgsqlCommand<"SELECT current_date as today", dvdRental, SingleRow = true>(dvdRental)
    cmd.Execute() |> printfn "Today is: %A"

let asyncBasicQuery() = 
    use cmd = new NpgsqlCommand<"SELECT title, release_year FROM public.film LIMIT 3", dvdRental>(dvdRental)
    for x in cmd.AsyncExecute() |>  Async.RunSynchronously do   
        printfn "Movie '%s' released in %i." x.title x.release_year.Value

let basicQueryFsx() = 
    use cmd = new NpgsqlCommand<"SELECT title, release_year FROM public.film LIMIT 3", dvdRental, Fsx = true>()

    for x in cmd.Execute() do   
        printfn "Movie '%s' released in %i." x.title x.release_year.Value

let tx() =
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tx = conn.BeginTransaction()
    use cmd = new NpgsqlCommand<"        
        INSERT INTO public.actor (first_name, last_name)
        VALUES(@firstName, @lastName)
    ", dvdRental>(conn, tx)
    assert(cmd.Execute(firstName = "Tom", lastName = "Hanks") = 1)
    //Commit to persist changes
    //tx.Commit()

let handWrittenUpsert() = 

    //deactivate customer if exists
    let email = "mary.smith@sakilacustomer.org"

    use cmd = new NpgsqlCommand<" 
            UPDATE public.customer 
            SET activebool = false 
            WHERE email = @email 
                AND activebool
    ", dvdRental, SingleRow = true>(dvdRental)

    let recordsAffected = cmd.Execute(email)
    if recordsAffected = 0 
    then
        printfn "Could not deactivate customer %s" email
    elif recordsAffected = 1
    then 
        use restore = 
            new NpgsqlCommand<" 
                UPDATE public.customer 
                SET activebool = true
                WHERE email = @email 
            ", dvdRental>(dvdRental)
        assert( restore.Execute(email) = 1)
        
let resultTypeDataTable() = 
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tx = conn.BeginTransaction()    
    use cmd = 
        new NpgsqlCommand<"
            SELECT customer_id, activebool
            FROM public.customer 
            WHERE email = @email  
        ", dvdRental, ResultType.DataTable>(conn, tx)
    let t = cmd.Execute(email = "mary.smith@sakilacustomer.org")
    if t.Rows.Count > 0 && t.Rows.[0].activebool
    then 
        t.Rows.[0].activebool <- true
        assert( t.Update(conn, tx) = 1)

    //Commit to persist changes
    //tx.Commit() 

let postGisSimpleSelectPoint() =
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    conn.TypeMapper.UseLegacyPostgis() |> ignore    
    use cmd = new NpgsqlCommand<"SELECT 'SRID=4;POINT(1 1)'::geometry", dvdRental, SingleRow = true>(conn)

    let actual: LegacyPostgis.PostgisPoint = downcast cmd.Execute().Value.Value
    printfn "x = %f, y = %f, SRID = %u" actual.X actual.Y actual.SRID