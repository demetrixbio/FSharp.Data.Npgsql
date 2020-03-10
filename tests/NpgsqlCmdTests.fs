module NpgsqlCmdTests

open System
open Xunit
open System.Reflection

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

let isStatementPrepared (connection: Npgsql.NpgsqlConnection) =
    let connector = typeof<Npgsql.NpgsqlConnection>.GetField("Connector", BindingFlags.NonPublic ||| BindingFlags.Instance).GetValue(connection)
    let psManager = connector.GetType().GetField("PreparedStatementManager", BindingFlags.NonPublic ||| BindingFlags.Instance).GetValue(connector)
    let preparedStatements = psManager.GetType().GetProperty("BySql", BindingFlags.NonPublic ||| BindingFlags.Instance).GetValue(psManager)
    preparedStatements.GetType().GetProperty("Count", BindingFlags.Public ||| BindingFlags.Instance).GetMethod.Invoke(preparedStatements, [||]) :?> int = 1

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
   
[<Literal>]
let selectFromPartitionedTable = "select * from logs where log_time between '2019-01-01' and '2019-12-31'"

[<Fact>]
let ``Select from partitioned table``() =
    use cmd = new NpgsqlCommand<selectFromPartitionedTable, dvdRental>(dvdRental)
    let actual = cmd.Execute()
    Assert.Equal(2, actual.Length)
    Assert.Equal<int[]>([|1;2;3|], actual.Head.some_data)

[<Literal>]
let selectFromSpecificPartition = "select * from logs_2019 where log_time between '2019-01-01' and '2019-12-31'"

[<Fact>]
let ``Select from specific partition``() =
    use cmd = new NpgsqlCommand<selectFromSpecificPartition, dvdRental>(dvdRental)
    let actual = cmd.Execute()
    Assert.Equal(2, actual.Length)
    Assert.Equal<int[]>([|1;2;3|], actual.Head.some_data)

type GetAllRatings = NpgsqlCommand<"
    SELECT * 
    FROM UNNEST( enum_range(NULL::mpaa_rating)) AS X 
    WHERE X <> @exclude;", dvdRental>

type Rating = GetAllRatings.``public.mpaa_rating``

[<Fact>]
let selectEnum() =
    use cmd = new GetAllRatings(dvdRental)
    Assert.Equal<_ list>(
        [ Rating.G; Rating.PG; Rating.R; Rating.``NC-17`` ],
        [ for x in cmd.Execute(exclude = Rating.``PG-13``) -> x.Value ]
    ) 

//ALTER TABLE public.country ADD ratings MPAA_RATING[] NULL;

type EchoRatingsArray = NpgsqlCommand<"SELECT @ratings::mpaa_rating[];", dvdRental, SingleRow = true>

[<Fact>]
let selectEnumWithArray() =
    use cmd = new EchoRatingsArray(dvdRental)

    let ratings = [| EchoRatingsArray.``public.mpaa_rating``.``PG-13``; EchoRatingsArray.``public.mpaa_rating``.R |]
        
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

(*[<Fact>]
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
        fulltext = NpgsqlTypes.NpgsqlTsVector.Parse("")
    )*)

[<Literal>]
let selectFromMaterializedView = "select some_data, title from long_films"

[<Fact>]
let ``Select from materialized view``() =
    use cmd = new NpgsqlCommand<selectFromMaterializedView, dvdRental, SingleRow = true>(dvdRental)
    let actual = cmd.Execute().Value
    Assert.Equal<int[]>([|1;2;3|], actual.some_data.Value)
    Assert.True(String.IsNullOrWhiteSpace actual.title.Value |> not)
 
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

type UpdateMovieRating = NpgsqlCommand<"UPDATE public.film SET rating = @rating WHERE rating = @oldRating AND title = @title", dvdRental>

[<Fact>]
let updateWithEnum() =
    use conn = Connection.get()
    use tx = conn.BeginTransaction()

    use cmd = new UpdateMovieRating(conn, tx)
    Assert.Equal( 
        1, 
        cmd.Execute( 
            rating = UpdateMovieRating.``public.mpaa_rating``.``PG-13``, 
            oldRating = UpdateMovieRating.``public.mpaa_rating``.PG,
            title = "Academy Dinosaur"
        )
    )


[<Fact>]
let selectBytea() =
    use cmd = new NpgsqlCommand<"SELECT picture FROM public.staff WHERE staff_id = 1", dvdRental, SingleRow = true>(dvdRental)
    let actual = cmd.Execute().Value.Value
    let expected = [|137uy; 80uy; 78uy; 71uy; 13uy; 10uy; 90uy; 10uy|]
    Assert.Equal<byte>(expected, actual)

[<Literal>]
let getActorByName = "
    SELECT first_name, last_name
    FROM public.actor 
    WHERE first_name = @firstName AND last_name = @lastName
"

[<Fact>]
let ``Command not prepared by default``() =
    use conn = Connection.get()
    conn.UnprepareAll()

    use cmd = new NpgsqlCommand<getActorByName, dvdRental>(conn)
    cmd.Execute("", "") |> ignore

    Assert.False(isStatementPrepared conn)

[<Fact>]
let ``Data table command prepared``() =
    use conn = Connection.get()
    conn.UnprepareAll()

    use cmd = new NpgsqlCommand<getActorByName, dvdRental, Prepare = true, ResultType = ResultType.DataTable>(conn)
    cmd.Execute("", "") |> ignore

    Assert.True(isStatementPrepared conn)

[<Fact>]
let ``Data reader command prepared``() =
    use conn = Connection.get()
    conn.UnprepareAll()

    use cmd = new NpgsqlCommand<getActorByName, dvdRental, Prepare = true, ResultType = ResultType.DataReader>(conn)
    cmd.Execute("", "") |> ignore

    Assert.True(isStatementPrepared conn)

[<Fact>]
let ``Records command prepared``() =
    use conn = Connection.get()
    conn.UnprepareAll()

    use cmd = new NpgsqlCommand<getActorByName, dvdRental, Prepare = true, ResultType = ResultType.Records>(conn)
    cmd.Execute("", "") |> ignore

    Assert.True(isStatementPrepared conn)

[<Fact>]
let ``Tuples command prepared``() =
    use conn = Connection.get()
    conn.UnprepareAll()

    use cmd = new NpgsqlCommand<getActorByName, dvdRental, Prepare = true, ResultType = ResultType.Tuples>(conn)
    cmd.Execute("", "") |> ignore

    Assert.True(isStatementPrepared conn)

[<Literal>]
let getActorsAndFilms = "select * from actor limit 5; select * from film limit 5"

[<Fact>]
let ``Two selects record``() =
    use cmd = new NpgsqlCommand<getActorsAndFilms, dvdRental>(dvdRental)
    let actual = cmd.Execute()

    Assert.Equal (5, actual.ResultSet1 |> List.map (fun x -> x.first_name) |> List.length)
    Assert.Equal (5, actual.ResultSet2 |> List.map (fun x -> x.title) |> List.length)

[<Fact>]
let ``Two selects tuple``() =
    use cmd = new NpgsqlCommand<getActorsAndFilms, dvdRental, ResultType = ResultType.Tuples>(dvdRental)
    let actual = cmd.Execute()

    Assert.Equal (5, actual.ResultSet1 |> List.length)
    Assert.Equal (5, actual.ResultSet2 |> List.length)

[<Fact>]
let ``Two selects data table``() =
    use cmd = new NpgsqlCommand<getActorsAndFilms, dvdRental, ResultType = ResultType.DataTable>(dvdRental)
    let actual = cmd.Execute()

    Assert.Equal (5, actual.ResultSet1.Rows |> Seq.map (fun x -> x.first_name) |> Seq.length)
    Assert.Equal (5, actual.ResultSet2.Rows |> Seq.map (fun x -> x.title) |> Seq.length)

[<Fact>]
let ``Two selects data reader``() =
    use cmd = new NpgsqlCommand<getActorsAndFilms, dvdRental, ResultType = ResultType.DataReader>(dvdRental)
    let actual = cmd.Execute()

    let mutable resultSets = 1

    while actual.NextResult () do
        resultSets <- resultSets + 1

    Assert.Equal (2, resultSets)

[<Literal>]
let getActorsUpdateActorsGetFilms = "select * from actor limit 5; update actor set actor_id = 1 where actor_id = 1; select * from film limit 5"

[<Fact>]
let ``Two selects and nonquery record``() =
    use cmd = new NpgsqlCommand<getActorsUpdateActorsGetFilms, dvdRental>(dvdRental)
    let actual = cmd.Execute()

    Assert.Equal (5, actual.ResultSet1 |> List.map (fun x -> x.first_name) |> List.length)
    Assert.Equal (1, actual.ResultSet2)
    Assert.Equal (5, actual.ResultSet3 |> List.map (fun x -> x.title) |> List.length)

[<Fact>]
let ``Two selects and nonquery tuple``() =
    use cmd = new NpgsqlCommand<getActorsUpdateActorsGetFilms, dvdRental, ResultType = ResultType.Tuples>(dvdRental)
    let actual = cmd.Execute()

    Assert.Equal (5, actual.ResultSet1 |> List.length)
    Assert.Equal (1, actual.ResultSet2)
    Assert.Equal (5, actual.ResultSet3 |> List.length)

[<Fact>]
let ``Two selects and nonquery data table``() =
    use cmd = new NpgsqlCommand<getActorsUpdateActorsGetFilms, dvdRental, ResultType = ResultType.DataTable>(dvdRental)
    let actual = cmd.Execute()

    Assert.Equal (5, actual.ResultSet1.Rows |> Seq.map (fun x -> x.first_name) |> Seq.length)
    Assert.Equal (1, actual.ResultSet2)
    Assert.Equal (5, actual.ResultSet3.Rows |> Seq.map (fun x -> x.title) |> Seq.length)

[<Fact>]
let ``Two selects and nonquery data reader``() =
    use cmd = new NpgsqlCommand<getActorsUpdateActorsGetFilms, dvdRental, ResultType = ResultType.DataReader>(dvdRental)
    let actual = cmd.Execute()

    let mutable resultSets = 1

    while actual.NextResult () do
        resultSets <- resultSets + 1

    Assert.Equal (2, resultSets)

[<Literal>]
let fourSelects = "SELECT * FROM generate_series(1, 10); SELECT * FROM generate_series(1, 10); SELECT * FROM generate_series(1, 10); SELECT * FROM generate_series(1, 10)"

[<Fact>]
let ``Four single-column selects async``() =
    use cmd = new NpgsqlCommand<fourSelects, dvdRental>(dvdRental)
    let actual = cmd.AsyncExecute() |> Async.RunSynchronously

    Assert.Equal (10, actual.ResultSet1 |> Seq.map (fun x -> x.Value) |> Seq.length)
    Assert.Equal (10, actual.ResultSet2 |> Seq.map (fun x -> x.Value) |> Seq.length)
    Assert.Equal (10, actual.ResultSet3 |> Seq.map (fun x -> x.Value) |> Seq.length)
    Assert.Equal (10, actual.ResultSet4 |> Seq.map (fun x -> x.Value) |> Seq.length)

[<Literal>]
let updateActorsUpdateSelectActorsUpdateFilms = "update actor set actor_id = actor_id where actor_id in (2, 3); select * from actor limit 5; update film set film_id = 1 where film_id = 1"

[<Fact>]
let ``One select and two updates record async``() =
    use cmd = new NpgsqlCommand<updateActorsUpdateSelectActorsUpdateFilms, dvdRental>(dvdRental)
    let actual = cmd.AsyncExecute() |> Async.RunSynchronously

    Assert.Equal (2, actual.ResultSet1)
    Assert.Equal (5, actual.ResultSet2 |> List.map (fun x -> x.first_name) |> List.length)
    Assert.Equal (1, actual.ResultSet3)

[<Fact>]
let ``One select and two updates tuple async``() =
    use cmd = new NpgsqlCommand<updateActorsUpdateSelectActorsUpdateFilms, dvdRental, ResultType = ResultType.Tuples>(dvdRental)
    let actual = cmd.AsyncExecute() |> Async.RunSynchronously

    Assert.Equal (2, actual.ResultSet1)
    Assert.Equal (5, actual.ResultSet2 |> List.length)
    Assert.Equal (1, actual.ResultSet3)

[<Fact>]
let ``One select and two updates data table async``() =
    use cmd = new NpgsqlCommand<updateActorsUpdateSelectActorsUpdateFilms, dvdRental, ResultType = ResultType.DataTable>(dvdRental)
    let actual = cmd.AsyncExecute() |> Async.RunSynchronously

    Assert.Equal (2, actual.ResultSet1)
    Assert.Equal (5, actual.ResultSet2.Rows |> Seq.map (fun x -> x.first_name) |> Seq.length)
    Assert.Equal (1, actual.ResultSet3)

[<Fact>]
let ``One select and two updates reader async``() =
    use cmd = new NpgsqlCommand<updateActorsUpdateSelectActorsUpdateFilms, dvdRental, ResultType = ResultType.DataReader>(dvdRental)
    let actual = cmd.AsyncExecute() |> Async.RunSynchronously

    let mutable resultSets = 1

    while actual.NextResult () do
        resultSets <- resultSets + 1

    Assert.Equal (1, resultSets)

[<Fact>]
let ``Queries against system catalogs work``() =
    use cmd = new NpgsqlCommand<"SELECT * FROM pg_timezone_names", dvdRental>(dvdRental)
    let actual = cmd.Execute()
    Assert.True(actual |> List.map (fun x -> x.name.Value) |> List.length > 0) 
 
[<Literal>]
let selectFromCompositesTableId5 = "select * from table_with_composites where id = 5"

[<Fact>]
let ``Composite type fields have correct values``() =
    use cmd = new NpgsqlCommand<selectFromCompositesTableId5, dvdRental>(dvdRental)
    let actual = cmd.Execute().Head
    
    Assert.Equal (Some 42L, actual.simple.some_number)
    Assert.Equal (None, actual.simple.some_text)
    Assert.Equal (Some [| 1; 2 |], actual.simple.some_array)

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

