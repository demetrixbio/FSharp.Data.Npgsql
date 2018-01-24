#I "../../src/Runtime/bin/Debug/netstandard2.0"
#r @"C:\Users\dmorozov\.nuget\packages\npgsql\3.2.6\lib\netstandard2.0\Npgsql.dll"
#r "FSharp.Data.Npgsql.dll"
#r "netstandard.dll"

open System.Data
open Npgsql

[<Literal>]
let dvdrental = "Host=localhost;Username=dvdrental;Password=postgres;Port=32768"

type DvdRental = FSharp.Data.NpgsqlConnection<dvdrental>

do
    let rental_id = 2
    
    use cmd = DvdRental.CreateCommand<"SELECT * FROM rental WHERE rental_id = @rental_id", SingleRow = true>(dvdrental)  
    let x = cmd.AsyncExecute(rental_id) |> Async.RunSynchronously |> Option.get
        
    use conn = new NpgsqlConnection(dvdrental)
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
    assert(1, t.Update(transaction = tran))
    let y = 
        use cmd = DvdRental.CreateCommand<"SELECT * FROM rental WHERE rental_id = @rental_id", SingleRow = true, Tx = true>(tran)
        cmd.Execute(r.rental_id) |> Option.get

    assert(x.staff_id, y.staff_id)
    assert(x.customer_id, y.customer_id)
    assert(x.inventory_id, y.inventory_id)
    assert(x.rental_date.AddDays(1.), y.rental_date)
    assert(x.return_date, y.return_date)

    tran.Rollback()

    assert(None, cmd.Execute(r.rental_id))