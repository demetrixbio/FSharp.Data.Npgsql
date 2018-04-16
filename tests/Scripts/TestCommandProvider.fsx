#r @"C:\Users\dmorozov\.nuget\packages\npgsql\3.2.7\lib\net451\Npgsql.dll"
#I "../../src/Runtime/bin/Debug/net461"
#r "FSharp.Data.Npgsql.dll"
#r "netstandard.dll"

open FSharp.Data.Npgsql
open System

[<Literal>]
let postgis_24_samples = "Host=localhost;Username=postgres;Password=postgres;Database=postgis_24_sample"
do
    use cmd = new NpgsqlCommand<"SELECT ST_GeomFromText('LINESTRING(50 50,150 150,150 50)')", postgis_24_samples, Fsx = true>()
    cmd.Execute() |> Seq.exactlyOne |> printfn "%A"

//[<Literal>]
//let dvdRental = "Host=localhost;Username=postgres;Database=dvdrental;Port=32768"

//do
//    use cmd = new NpgsqlCommand<"SELECT 42 AS Answer, current_date as today", dvdRental, Fsx = true>()

//    cmd.Execute() |> Seq.exactlyOne |> printfn "%A"

//type GetRentalReturnDateById = NpgsqlCommand<"SELECT return_date FROM rental WHERE rental_id = @id", dvdRental, Fsx = true>

//do
//    let rental_id = 2

//    use cmd = new NpgsqlCommand<"
//        SELECT * FROM rental WHERE rental_id = @rental_id
//    ", dvdRental, ResultType.DataTable, Fsx = true>(dvdRental)    
//    let t = cmd.Execute(rental_id)
//    printfn "1 = t.Rows.Count: %b" (1 = t.Rows.Count)

//    let r = t.Rows.[0]
//    let return_date = r.return_date
//    let rowsAffected = ref 0
//    try
//        let new_return_date = Some DateTime.Now.Date
//        r.return_date <- new_return_date
//        rowsAffected := t.Update()
//        printfn "1 = !rowsAffected: %b" (1 = !rowsAffected)

//        use cmd = GetRentalReturnDateById.Create(dvdRental)
//        printfn "new_return_date = (cmd.Execute( rental_id) |> Seq.exactlyOne ): %b" (new_return_date = (cmd.Execute( rental_id) |> Seq.exactlyOne ))

//    finally
//        if !rowsAffected = 1
//        then 
//            r.return_date <- return_date
//            t.Update(dvdRental) |>  ignore      
