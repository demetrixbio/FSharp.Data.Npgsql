module NpgsqlCmdTests

open System
open Xunit

[<Literal>]
let dvdRental = "Host=localhost;Username=postgres;Password=postgres;Database=dvdrental"

let openConnection() = 
    let conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    conn

open FSharp.Data

[<Fact>]
let selectLiterals() =
    use cmd = new NpgsqlCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", dvdRental>(dvdRental)

    let x = cmd.Execute() |> Seq.exactlyOne
    Assert.Equal(Some 42, x.answer)
    Assert.Equal(Some DateTime.Today, x.today)

[<Fact>]
let selectSingleRow() =
    use cmd = new NpgsqlCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", dvdRental, SingleRow = true>(dvdRental)

    Assert.Equal(
        Some( Some 42, Some DateTime.Today), 
        cmd.Execute() |> Option.map ( fun x ->  x.answer, x.today )
    )

[<Fact>]
let selectTuple() =
    use cmd = new NpgsqlCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", dvdRental, ResultType.Tuples>(dvdRental)

    Assert.Equal<_ list>(
        [ Some 42, Some DateTime.Today ],
        cmd.Execute() |>  Seq.toList
    )

[<Fact>]
let selectSingleNull() =
    use cmd = new NpgsqlCommand<"SELECT NULL", dvdRental, SingleRow = true>(dvdRental)
    Assert.Equal(Some None, cmd.Execute())

[<Fact>]
let selectSingleColumn() =
    use cmd = new NpgsqlCommand<"
        SELECT * FROM generate_series(0, 10)
    ", dvdRental>(dvdRental)

    Assert.Equal<_ seq>(
        { 0 .. 10 }, 
        cmd.Execute() |> Seq.choose id 
    )

[<Fact>]
let paramInFilter() =
    use cmd = new NpgsqlCommand<"
        SELECT * FROM generate_series(0, 10) AS xs(value) WHERE value % @div = 0
    ", dvdRental>(dvdRental)

    Assert.Equal<_ seq>(
        { 0 .. 2 .. 10 }, 
        cmd.Execute(div = 2) |> Seq.choose id 
    )

[<Fact>]
let paramInLimit() =
    use cmd = new NpgsqlCommand<"
        SELECT * FROM generate_series(0, 10) LIMIT @limit
    ", dvdRental>(dvdRental)

    let limit = 5
    Assert.Equal<_ seq>(
        { 0 .. 10 } |> Seq.take limit , 
        cmd.Execute(int64 limit) |> Seq.choose id
    )

type GetRentalById = NpgsqlCommand<"SELECT return_date FROM rental WHERE rental_id = @id", dvdRental>

[<Fact>]
let dateTableWithUpdate() =

    let rental_id = 2

    use cmd = new NpgsqlCommand<"
        SELECT * FROM rental WHERE rental_id = @rental_id
    ", dvdRental, ResultType.DataTable>(dvdRental)    
    let t = cmd.Execute(rental_id)
    Assert.Equal(1, t.Rows.Count)
    let r = t.Rows.[0]
    let return_date = r.return_date
    let rowsAffected = ref 0
    try
        let new_return_date = Some DateTime.Now.Date
        r.return_date <- new_return_date
        rowsAffected := t.Update()
        Assert.Equal(1, !rowsAffected)

        use cmd = GetRentalById.Create(dvdRental)
        Assert.Equal( new_return_date, cmd.Execute( rental_id) |> Seq.exactlyOne ) 

    finally
        if !rowsAffected = 1
        then 
            r.return_date <- return_date
            t.Update() |>  ignore      
            
[<Fact>]
let dateTableWithUpdateAndTx() =
    
    let rental_id = 2
    
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tran = conn.BeginTransaction()

    use cmd = new NpgsqlCommand<"
        SELECT * FROM rental WHERE rental_id = @rental_id
    ", dvdRental, ResultType.DataTable>(tran)    
    let t = cmd.Execute(rental_id)
    Assert.Equal(1, t.Rows.Count)
    let r = t.Rows.[0]
    let return_date = r.return_date

    let new_return_date = Some DateTime.Now.Date
    r.return_date <- new_return_date
    Assert.Equal(1, t.Update(transaction = tran))

    Assert.Equal( 
        new_return_date, 
        GetRentalById.Create(tran).Execute( rental_id) |>  Seq.exactlyOne
    ) 

    tran.Rollback()

    Assert.Equal(
        return_date, 
        GetRentalById.Create(dvdRental).Execute( rental_id) |> Seq.exactlyOne
    ) 

[<Fact>]
let dateTableWithUpdateWithConflictOptionCompareAllSearchableValues() =
    
    let rental_id = 2
    
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tran = conn.BeginTransaction()

    use cmd = new NpgsqlCommand<"
        SELECT * FROM rental WHERE rental_id = @rental_id
    ", dvdRental, ResultType.DataTable>(tran)    
  
    let t = cmd.Execute(rental_id)
    Assert.Equal(1, t.Rows.Count)
    let r = t.Rows.[0]
    r.return_date <- Some DateTime.Now.Date
    Assert.Equal(1, t.Update(transaction = tran, conflictOption = Data.ConflictOption.CompareAllSearchableValues ))

    Assert.Equal( 
        r.return_date, 
        GetRentalById.Create(tran).Execute( rental_id) |>  Seq.exactlyOne 
    ) 

[<Fact>]
let deleteWithTx() =
    let rental_id = 2

    use cmd = new GetRentalById(dvdRental)
    Assert.Equal(1, cmd.Execute( rental_id) |> Seq.length) 

    do 
        use conn = openConnection()
        use tran = conn.BeginTransaction()

        use del = new NpgsqlCommand<"
            DELETE FROM rental WHERE rental_id = @rental_id
        ", dvdRental>(tran)  
        Assert.Equal(1, del.Execute(rental_id))
        Assert.Empty( GetRentalById.Create(tran).Execute( rental_id)) 


    Assert.Equal(1, cmd.Execute( rental_id) |> Seq.length) 
    
type GetAllRatings = NpgsqlCommand<"
    SELECT * 
    FROM UNNEST( enum_range(NULL::mpaa_rating)) AS X 
    WHERE X <> @exclude;  
", dvdRental>

type Rating = GetAllRatings.``public.mpaa_rating``

[<Fact>]
let selectEnum() =
    use cmd = new GetAllRatings(dvdRental)
    Assert.Equal<_ list>(
        [ Rating.G; Rating.PG; Rating.R; Rating.``NC-17`` ],
        [ for x in cmd.Execute(exclude = Rating.``PG-13``) -> x.Value ]
    ) 

//ALTER TABLE public.country ADD ratings MPAA_RATING[] NULL;

type EchoRatingsArray = NpgsqlCommand<"
        SELECT @ratings::mpaa_rating[];
    ", dvdRental, SingleRow = true>

[<Fact>]
let selectEnumWithArray() =
    use cmd = new EchoRatingsArray(dvdRental)

    let ratings = [| 
        EchoRatingsArray.``public.mpaa_rating``.``PG-13`` 
        EchoRatingsArray.``public.mpaa_rating``.R 
    |]

    Assert.Equal( Some(  Some ratings), cmd.Execute(ratings))

[<Fact>]
let allParametersOptional() =
    let cmd = new NpgsqlCommand<"
        SELECT coalesce(@x, 'Empty') AS x
    ", dvdRental, AllParametersOptional = true, SingleRow = true>(dvdRental)
    Assert.Equal(Some( Some "test"), cmd.Execute(Some "test")) 
    Assert.Equal(Some( Some "Empty"), cmd.Execute()) 


