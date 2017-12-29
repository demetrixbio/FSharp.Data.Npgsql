module Tests

open System
open Xunit

open Npgsql
open FSharp.Data

[<Literal>]
let connectionString = "Host=localhost;Username=postgres;Password=postgres;Database=dvdrental"

[<Fact>]
let selectLiterals() =
    use cmd = new NpgsqlCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", connectionString>(connectionString)

    let x = cmd.Execute() |> Seq.exactlyOne
    Assert.Equal(Some 42, x.answer)
    Assert.Equal(Some DateTime.Today, x.today)

[<Fact>]
let selectSingleRow() =
    use cmd = new NpgsqlCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", connectionString, SingleRow = true>(connectionString)

    Assert.Equal(
        Some( Some 42, Some DateTime.Today), 
        cmd.Execute() |> Option.map ( fun x ->  x.answer, x.today )
    )

[<Fact>]
let selectTuple() =
    use cmd = new NpgsqlCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", connectionString, ResultType.Tuples>(connectionString)

    Assert.Equal<_ list>(
        [ Some 42, Some DateTime.Today ],
        cmd.Execute() |>  Seq.toList
    )

[<Fact>]
let selectSingleNull() =
    use cmd = new NpgsqlCommand<"SELECT NULL", connectionString, SingleRow = true>(connectionString)
    Assert.Equal(Some None, cmd.Execute())

[<Fact>]
let selectSingleColumn() =
    use cmd = new NpgsqlCommand<"
        SELECT * FROM generate_series(0, 10)
    ", connectionString>(connectionString)

    Assert.Equal<_ seq>(
        { 0 .. 10 }, 
        cmd.Execute() |> Seq.choose id 
    )

[<Fact>]
let paramInFilter() =
    use cmd = new NpgsqlCommand<"
        SELECT * FROM generate_series(0, 10) AS xs(value) WHERE value % @div = 0
    ", connectionString>(connectionString)

    Assert.Equal<_ seq>(
        { 0 .. 2 .. 10 }, 
        cmd.Execute(div = 2) |> Seq.choose id 
    )

[<Fact>]
let paramInLimit() =
    use cmd = new NpgsqlCommand<"
        SELECT * FROM generate_series(0, 10) LIMIT @limit
    ", connectionString>(connectionString)

    let limit = 5
    Assert.Equal<_ seq>(
        { 0 .. 10 } |> Seq.take limit , 
        cmd.Execute(int64 limit) |> Seq.choose id
    )

type GetRentalById = NpgsqlCommand<"SELECT return_date FROM rental WHERE rental_id = @id", connectionString, SingleRow = true>

[<Fact>]
let dateTableWithUpdate() =
    use cmd = new NpgsqlCommand<"
        SELECT * FROM rental WHERE rental_id = @rental_id
    ", connectionString, ResultType.DataTable>(connectionString)    
    let t = cmd.Execute(rental_id = 2)
    Assert.Equal(1, t.Rows.Count)
    let r = t.Rows.[0]
    let return_date = r.return_date
    let rowsAffected = ref 0
    try
        let new_return_date = Some DateTime.Now.Date
        r.return_date <- new_return_date
        rowsAffected := t.Update()
        Assert.Equal(1, !rowsAffected)

        use cmd = GetRentalById.Create(connectionString)
        Assert.Equal(Some( new_return_date), cmd.Execute(2)) 

    finally
        if !rowsAffected = 1
        then 
            r.return_date <- return_date
            t.Update() |>  ignore      
            
[<Fact>]
let dateTableWithUpdateAndTx() =
        
    use conn = new NpgsqlConnection(connectionString)
    conn.Open()
    use tran = conn.BeginTransaction()

    use cmd = new NpgsqlCommand<"
        SELECT * FROM rental WHERE rental_id = @rental_id
    ", connectionString, ResultType.DataTable>(tran)    
    let t = cmd.Execute(rental_id = 2)
    Assert.Equal(1, t.Rows.Count)
    let r = t.Rows.[0]
    let return_date = r.return_date

    let new_return_date = Some DateTime.Now.Date
    r.return_date <- new_return_date
    Assert.Equal(1, t.Update(transaction = tran))

    Assert.Equal(Some( new_return_date), GetRentalById.Create(tran).Execute(2)) 

    tran.Rollback()

    Assert.Equal(Some( return_date), GetRentalById.Create(connectionString).Execute(2)) 
