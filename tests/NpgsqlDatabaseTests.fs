module NpgsqlConnectionTests

open System
open Xunit

[<Literal>]
let dvdRental = "Host=localhost;Username=postgres;Password=postgres;Database=dvdrental"

open FSharp.Data
type DvdRental = NpgsqlConnection<dvdRental>

open Npgsql

[<Fact>]
let selectLiterals() =
    use cmd = 
        DvdRental.CreateCommand<"        
            SELECT 42 AS Answer, current_date as today
        ">(dvdRental)

    let x = cmd.Execute() |> Seq.exactlyOne
    Assert.Equal(Some 42, x.answer)
    Assert.Equal(Some DateTime.Today, x.today)

[<Fact>]
let selectSingleRow() =
    use cmd = DvdRental.CreateCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", SingleRow = true>(dvdRental)

    Assert.Equal(
        Some( Some 42, Some DateTime.Today), 
        cmd.Execute() |> Option.map ( fun x ->  x.answer, x.today )
    )

[<Fact>]
let selectTuple() =
    use cmd = DvdRental.CreateCommand<"    
        SELECT 42 AS Answer, current_date as today
    ", ResultType.Tuples>(dvdRental)

    Assert.Equal<_ list>(
        [ Some 42, Some DateTime.Today ],
        cmd.Execute() |>  Seq.toList
    )

[<Fact>]
let selectSingleNull() =
    use cmd = DvdRental.CreateCommand<"SELECT NULL", SingleRow = true>(dvdRental)
    Assert.Equal(Some None, cmd.Execute())

[<Fact>]
let selectSingleColumn() =
    use cmd = DvdRental.CreateCommand<"SELECT * FROM generate_series(0, 10)">(dvdRental)

    Assert.Equal<_ seq>(
        { 0 .. 10 }, 
        cmd.Execute() |> Seq.choose id 
    )

[<Fact>]
let paramInFilter() =
    use cmd = 
        DvdRental.CreateCommand<"
            SELECT * FROM generate_series(0, 10) AS xs(value) WHERE value % @div = 0
        ">(dvdRental)

    Assert.Equal<_ seq>(
        { 0 .. 2 .. 10 }, 
        cmd.Execute(div = 2) |> Seq.choose id 
    )

[<Fact>]
let paramInLimit() =
    use cmd = 
        DvdRental.CreateCommand<"
            SELECT * FROM generate_series(0, 10) LIMIT @limit
        ">(dvdRental)

    let limit = 5
    Assert.Equal<_ seq>(
        { 0 .. 10 } |> Seq.take limit , 
        cmd.Execute(int64 limit) |> Seq.choose id
    )

[<Literal>]
let getRentalById = "SELECT return_date FROM rental WHERE rental_id = @id"

[<Fact>]
let dateTableWithUpdate() =

    let rental_id = 2

    use cmd = 
        DvdRental.CreateCommand<"
            SELECT * FROM rental WHERE rental_id = @rental_id
        ", ResultType.DataTable>(dvdRental) 
        
    let t = cmd.Execute(rental_id)
    Assert.Equal(1, t.Rows.Count)
    let r = t.Rows.[0]
    let return_date = r.return_date
    let rowsAffected = ref 0
    try
        let new_return_date = Some DateTime.Now.Date
        r.return_date <- new_return_date
        rowsAffected := t.Update(dvdRental)
        Assert.Equal(1, !rowsAffected)

        use cmd = DvdRental.CreateCommand<getRentalById>(dvdRental)
        Assert.Equal( new_return_date, cmd.Execute( rental_id) |> Seq.exactlyOne ) 

    finally
        if !rowsAffected = 1
        then 
            r.return_date <- return_date
            t.Update(dvdRental) |>  ignore      
            
[<Fact>]
let dateTableWithUpdateAndTx() =
    
    let rental_id = 2
    
    use conn = new NpgsqlConnection(dvdRental)
    conn.Open()
    use tran = conn.BeginTransaction()

    use cmd = 
        DvdRental.CreateCommand<"
            SELECT * FROM rental WHERE rental_id = @rental_id
        ", ResultType.DataTable, Tx = true>(tran)    
    let t = cmd.Execute(rental_id)
    Assert.Equal(1, t.Rows.Count)
    let r = t.Rows.[0]
    let return_date = r.return_date

    let new_return_date = Some DateTime.Now.Date
    r.return_date <- new_return_date
    Assert.Equal(1, t.Update(transaction = tran))

    use getRentalByIdCmd = DvdRental.CreateCommand<getRentalById, Tx = true>(tran)
    Assert.Equal( 
        new_return_date, 
        getRentalByIdCmd.Execute( rental_id) |>  Seq.exactlyOne
    ) 

    tran.Rollback()

    Assert.Equal(
        return_date, 
        getRentalByIdCmd.Execute( rental_id) |> Seq.exactlyOne
    ) 

[<Fact>]
let dateTableWithUpdateWithConflictOptionCompareAllSearchableValues() =
    
    let rental_id = 2
    
    use conn = new NpgsqlConnection(dvdRental)
    conn.Open()
    use tran = conn.BeginTransaction()

    use cmd = 
        DvdRental.CreateCommand<"
            SELECT * FROM rental WHERE rental_id = @rental_id
        ", ResultType.DataTable, Tx = true>(tran)    
  
    let t = cmd.Execute(rental_id)

    [ for c in t.Columns ->  c.ColumnName, c.DataType, c.DateTimeMode  ] |> printfn "\nColumns:\n%A"

    Assert.Equal(1, t.Rows.Count)
    let r = t.Rows.[0]
    r.return_date <- r.return_date |> Option.map (fun d -> d.AddDays(1.))
    //Assert.Equal(1, t.Update(connection = conn, transaction = tran, conflictOption = Data.ConflictOption.CompareAllSearchableValues ))
    Assert.Equal(1, t.Update(transaction = tran, conflictOption = Data.ConflictOption.OverwriteChanges ))
     
    use getRentalByIdCmd = DvdRental.CreateCommand<getRentalById, Tx = true>(tran)
    Assert.Equal( 
        r.return_date, 
        getRentalByIdCmd.Execute( rental_id) |>  Seq.exactlyOne 
    ) 

[<Fact>]
let deleteWithTx() =
    let rental_id = 2

    use cmd = DvdRental.CreateCommand<getRentalById>(dvdRental)
    Assert.Equal(1, cmd.Execute( rental_id) |> Seq.length) 

    do 
        use conn = new NpgsqlConnection(dvdRental)
        conn.Open()
        use tran = conn.BeginTransaction()

        use del = 
            DvdRental.CreateCommand<"
                DELETE FROM rental WHERE rental_id = @rental_id
            ", Tx = true>(tran)  
        Assert.Equal(1, del.Execute(rental_id))
        Assert.Empty( DvdRental.CreateCommand<getRentalById, Tx = true>(tran).Execute( rental_id)) 


    Assert.Equal(1, cmd.Execute( rental_id) |> Seq.length) 
    
type Rating = DvdRental.``public``.Types.mpaa_rating

[<Fact>]
let selectEnum() =
    use cmd = 
        DvdRental.CreateCommand<"
            SELECT * 
            FROM UNNEST( enum_range(NULL::mpaa_rating)) AS X 
            WHERE X <> @exclude;          
        ">(dvdRental)
    Assert.Equal<_ list>(
        [ Rating.G; Rating.PG; Rating.R; Rating.``NC-17`` ],
        [ for x in cmd.Execute(exclude = Rating.``PG-13``) -> x.Value ]
    ) 

////ALTER TABLE public.country ADD ratings MPAA_RATING[] NULL;

[<Fact>]
let selectEnumWithArray() =
    use cmd = DvdRental.CreateCommand<"
        SELECT COUNT(*)  FROM film WHERE ARRAY[rating] <@ @xs::text[]::mpaa_rating[];
    ", SingleRow = true>(dvdRental)

    Assert.Equal( Some( Some 223L), cmd.Execute([| "PG-13" |])) 

[<Fact>]
let allParametersOptional() =
    let cmd = 
        DvdRental.CreateCommand<"
            SELECT coalesce(@x, 'Empty') AS x
        ", AllParametersOptional = true, SingleRow = true>(dvdRental)
    Assert.Equal(Some( Some "test"), cmd.Execute(Some "test")) 
    Assert.Equal(Some( Some "Empty"), cmd.Execute()) 

[<Fact>]
let tableInsert() =
    
    let rental_id = 2
    
    use cmd = DvdRental.CreateCommand<"SELECT * FROM rental WHERE rental_id = @rental_id", SingleRow = true>(dvdRental)  
    let x = cmd.AsyncExecute(rental_id) |> Async.RunSynchronously |> Option.get
        
    use conn = new NpgsqlConnection(dvdRental)
    conn.Open()
    use tran = conn.BeginTransaction()
    use t = new DvdRental.``public``.Tables.rental()
    let r = 
        t.NewRow(
            staff_id = x.staff_id, 
            customer_id = x.customer_id, 
            inventory_id = x.inventory_id, 
            rental_date = x.rental_date.AddDays(1.), 
            return_date = x.return_date
        )

    t.Rows.Add(r)
    Assert.Equal(1, t.Update(transaction = tran))
    let y = 
        use cmd = DvdRental.CreateCommand<"SELECT * FROM rental WHERE rental_id = @rental_id", SingleRow = true, Tx = true>(tran)
        cmd.Execute(r.rental_id) |> Option.get

    Assert.Equal(x.staff_id, y.staff_id)
    Assert.Equal(x.customer_id, y.customer_id)
    Assert.Equal(x.inventory_id, y.inventory_id)
    Assert.Equal(x.rental_date.AddDays(1.), y.rental_date)
    Assert.Equal(x.return_date, y.return_date)

    tran.Rollback()

    Assert.Equal(None, cmd.Execute(r.rental_id))
