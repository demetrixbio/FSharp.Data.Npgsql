module NpgsqlCmdTests

open System
open Xunit

[<Literal>]
let dvdRental = "Host=localhost;Username=postgres;Database=dvdrental;Port=32768"

module Connection = 
    open Npgsql

    let get() = 
        let conn = new NpgsqlConnection(dvdRental)
        conn.Open()
        conn

    let getWithPostGis() = 
        let conn = get()
        conn.TypeMapper.UseLegacyPostgis() |> ignore
        conn

open FSharp.Data.Npgsql

[<Fact>]
let selectLiterals() =
    use cmd = new NpgsqlCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", dvdRental>(dvdRental)

    let x = cmd.Execute() |> Seq.exactlyOne
    Assert.Equal(Some 42, x.answer) 
    Assert.Equal(Some DateTime.UtcNow.Date, x.today)

[<Fact>]
let selectSingleRow() =
    use cmd = new NpgsqlCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", dvdRental, SingleRow = true>(dvdRental)

    Assert.Equal(
        Some( Some 42, Some DateTime.UtcNow.Date), 
        cmd.Execute() |> Option.map ( fun x ->  x.answer, x.today )
    )

[<Fact>]
let selectTuple() =
    use cmd = new NpgsqlCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", dvdRental, ResultType.Tuples>(dvdRental)

    Assert.Equal<_ list>(
        [ Some 42, Some DateTime.UtcNow.Date ],
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

type GetRentalReturnDateById = NpgsqlCommand<"SELECT return_date FROM rental WHERE rental_id = @id", dvdRental>

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
        rowsAffected := t.Update(dvdRental)
        Assert.Equal(1, !rowsAffected)

        use cmd = GetRentalReturnDateById.Create(dvdRental)
        Assert.Equal( new_return_date, cmd.Execute( rental_id) |> Seq.exactlyOne ) 

    finally
        if !rowsAffected = 1
        then 
            r.return_date <- return_date
            t.Update(dvdRental) |>  ignore      
            
[<Fact>]
let dateTableWithUpdateAndTx() =
    
    let rental_id = 2
    
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tx = conn.BeginTransaction()

    use cmd = new NpgsqlCommand<"
        SELECT * FROM rental WHERE rental_id = @rental_id
    ", dvdRental, ResultType.DataTable>(conn, tx)    
    let t = cmd.Execute(rental_id)
    Assert.Equal(1, t.Rows.Count)
    let r = t.Rows.[0]
    let return_date = r.return_date

    let new_return_date = Some DateTime.Now.Date
    r.return_date <- new_return_date
    Assert.Equal(1, t.Update(conn, tx))

    Assert.Equal( 
        new_return_date, 
        GetRentalReturnDateById.Create(conn, tx).Execute( rental_id) |> Seq.exactlyOne
    ) 

    tx.Rollback()
    conn.Close()

    Assert.Equal(
        return_date, 
        GetRentalReturnDateById.Create(dvdRental).Execute( rental_id) |> Seq.exactlyOne
    ) 

[<Fact>]
let dateTableWithUpdateWithConflictOptionCompareAllSearchableValues() =
    
    let rental_id = 2
    
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tran = conn.BeginTransaction()

    use cmd = new NpgsqlCommand<"
        SELECT * FROM rental WHERE rental_id = @rental_id
    ", dvdRental, ResultType.DataTable>(conn, tran)    
  
    let t = cmd.Execute(rental_id)
    Assert.Equal(1, t.Rows.Count)
    let r = t.Rows.[0]
    r.return_date <- Some DateTime.Now.Date
    Assert.Equal(1, t.Update(conn, tran, conflictOption = Data.ConflictOption.CompareAllSearchableValues ))

    Assert.Equal( 
        r.return_date, 
        GetRentalReturnDateById.Create(conn, tran).Execute( rental_id) |>  Seq.exactlyOne 
    ) 

[<Fact>]
let deleteWithTx() =
    let rental_id = 2

    use cmd = new GetRentalReturnDateById(dvdRental)
    Assert.Equal(1, cmd.Execute( rental_id) |> Seq.length) 

    do 
        use conn = Connection.get()
        use tran = conn.BeginTransaction()

        use del = new NpgsqlCommand<"
            DELETE FROM rental WHERE rental_id = @rental_id
        ", dvdRental>(conn, tran)  
        Assert.Equal(1, del.Execute(rental_id))
        Assert.Empty( GetRentalReturnDateById.Create(conn, tran).Execute( rental_id)) 


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
    use cmd = new NpgsqlCommand<"
        SELECT coalesce(@x, 'Empty') AS x
    ", dvdRental, AllParametersOptional = true, SingleRow = true>(dvdRental)
    Assert.Equal(Some( Some "test"), cmd.Execute(Some "test")) 
    Assert.Equal(Some( Some "Empty"), cmd.Execute()) 

[<Literal>]
let jsonConfig = __SOURCE_DIRECTORY__ + "/" + "development.settings.json"

[<Fact>]
let selectLiteralsConnStrFromJsonConfig() =
    use cmd = new NpgsqlCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", "dvdRental", Config = jsonConfig >(dvdRental)

    let x = cmd.Execute() |> Seq.exactlyOne
    Assert.Equal(Some 42, x.answer)
    Assert.Equal(Some DateTime.UtcNow.Date, x.today)

//[<Fact>]
//let selectLiteralsConnStrFromEnvironmentVariables() =
//    use cmd = new NpgsqlCommand<"        
//        SELECT 42 AS Answer, current_date as today
//    ", "dvdRental", ConfigType = ConfigType.Environment>(dvdRental)

//    let x = cmd.Execute() |> Seq.exactlyOne
//    Assert.Equal(Some 42, x.answer)
//    Assert.Equal(Some DateTime.UtcNow.Date, x.today)

//%APPDATA%\microsoft\UserSecrets\e0db9c78-0c59-4e4f-9d15-ed0c2848e94e\secrets.json
//[<Fact>]
//let selectLiteralsConnStrFromUserSecretStorePassingUserSecretsIdExplicitely() =
//    use cmd = new NpgsqlCommand<"        
//        SELECT 42 AS Answer, current_date as today
//    ", "dvdRental", ConfigType = ConfigType.UserStore, Config = "e0db9c78-0c59-4e4f-9d15-ed0c2848e94e">(dvdRental)

//    let x = cmd.Execute() |> Seq.exactlyOne
//    Assert.Equal(Some 42, x.answer)
//    Assert.Equal(Some DateTime.UtcNow.Date, x.today)

//[<Fact>]
//let selectLiteralsConnStrFromUserSecretStore() =
//    use cmd = new NpgsqlCommand<"        
//        SELECT 42 AS Answer, current_date as today
//    ", "dvdRental", ConfigType = ConfigType.UserStore>(dvdRental)

//    let x = cmd.Execute() |> Seq.exactlyOne
//    Assert.Equal(Some 42, x.answer)
//    Assert.Equal(Some DateTime.UtcNow.Date, x.today)

[<Fact>]
let selectLiteralsWithConnObject() =
    use cmd = new NpgsqlCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", dvdRental>(connection = Connection.get())

    let x = cmd.Execute() |> Seq.exactlyOne
    Assert.Equal(Some 42, x.answer) 
    Assert.Equal(Some DateTime.UtcNow.Date, x.today)

[<Fact>]
let fsx() =
    let why = Assert.Throws<exn>(fun()  -> 
        use cmd = new NpgsqlCommand<"SELECT 42 AS Answer", dvdRental, Fsx = true>()        
        cmd.Execute() |> ignore
    )
    Assert.Equal(
        "Design-time connection string re-use allowed at run-time only when executed inside FSI.",
        why.Message
    )


type EchoRatingsArrayWithVerification = NpgsqlCommand<"
        SELECT 42 , ARRAY[1, 2, 3];
    ", dvdRental, ResultType.Tuples>

[<Fact>]
let selectEnumWithArray2() =
    use cmd = new EchoRatingsArrayWithVerification(dvdRental)

    let actual = cmd.Execute() |> Seq.exactlyOne
    Assert.Equal( (Some 42, Some [| 1..3 |]), actual)

[<Fact>]
let ``AddRow/NewRow preserve order``() =
    use getActors = new NpgsqlCommand<"SELECT * FROM public.actor WHERE first_name = @firstName AND last_name = @lastName", dvdRental, ResultType.DataTable>(dvdRental)
    let actors = getActors.Execute("Tom", "Hankss")
    let r = actors.NewRow(Some 42, "Tom", "Hanks", Some DateTime.Now) 
    actors.Rows.Add(r); actors.Rows.Remove(r) |> ignore
    let r = actors.NewRow(actor_id = Some 42, first_name = "Tom", last_name = "Hanks", last_update = Some DateTime.Now) 
    actors.Rows.Add(r); actors.Rows.Remove(r) |> ignore

    actors.AddRow(None, first_name = "Tom", last_name = "Hanks", last_update = Some DateTime.Now) 
    actors.AddRow(last_update = Some DateTime.Now, first_name = "Tom", last_name = "Hanks")

    let getFilms = new NpgsqlCommand<"SELECT * FROM public.film WHERE title = @title", dvdRental, ResultType.DataTable>(dvdRental)
    let films = getFilms.Execute("Inception")
    films.AddRow(
        title = "Inception", 
        description = Some "A thief, who steals corporate secrets through the use of dream-sharing technology, is given the inverse task of planting an idea into the mind of a CEO.",
        language_id = 1s,
        fulltext = NpgsqlTypes.NpgsqlTsVector(ResizeArray())
    )


[<Fact>]
let asyncUpdateTable() =
    
    use conn = Connection.get()
    use tx = conn.BeginTransaction()

    use cmd =
        new NpgsqlCommand<"
            SELECT 
                actor_id, first_name, last_name, last_update
            FROM 
                public.actor
            WHERE 
                first_name = @firstName 
                AND last_name = @last_name
        ", dvdRental, ResultType.DataTable>(conn, tx)

    let (firstName, lastName) as name = "Tom", "Hanks"
    let actors = cmd.AsyncExecute name |> Async.RunSynchronously

    if actors.Rows.Count = 0 then
        actors.AddRow(first_name = firstName, last_name = lastName)
    else
        actors.Rows.[0].last_update <- DateTime.UtcNow
    
    Assert.Equal(1, actors.Update(conn, tx))

[<Fact>]
let postGisSimpleSelectPoint() =
    use conn = Connection.getWithPostGis()
    use cmd = new NpgsqlCommand<"SELECT 'SRID=4;POINT(0 0)'::geometry", dvdRental, SingleRow = true>(conn)
    let actual = cmd.Execute().Value.Value
    let expected = Npgsql.LegacyPostgis.PostgisPoint(x = 0., y = 0., SRID = 4u)
    Assert.Equal(expected, downcast actual)

[<Fact>]
let postGisSimpleSelectPointConnStr() =
    use cmd = new NpgsqlCommand<"SELECT 'SRID=4;POINT(0 0)'::geometry", dvdRental, SingleRow = true>(dvdRental)
    let actual = cmd.Execute().Value.Value
    let expected = Npgsql.LegacyPostgis.PostgisPoint(x = 0., y = 0., SRID = 4u)
    Assert.Equal(expected, downcast actual)
 
//[<Fact>]
//let npPkTable() =
//    use cmd =
//        new NpgsqlCommand<"select * from table_name limit 1", dvdRental, ResultType.DataTable>(dvdRental)
//    let t = cmd.Execute()
//    //t.Rows.[0].column_1 <- Some -1
//    //t.Update(dvdRental.Value) |> ignore
//    ()

//[<Fact>]
//let ``timestamp with time zone params``() =
//    do
//        let now = 
//            use cmd = new NpgsqlCommand<"SELECT current_timestamp", dvdRental, SingleRow = true>(dvdRental)
//            cmd.Execute().Value.Value
//        use cmd = new NpgsqlCommand<"SELECT current_timestamp < @p", dvdRental, SingleRow = true>(dvdRental)
//        Assert.True( cmd.Execute( now).Value.Value)
    
//    do
//        use cmd = new NpgsqlCommand<"SELECT current_timestamp > @p", dvdRental, SingleRow = true>(dvdRental)
//        Assert.True( cmd.Execute( DateTime.UtcNow.AddMinutes(-1.)).Value.Value)

