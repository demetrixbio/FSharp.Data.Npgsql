module Program

open System
open NpgsqlConnectionTests
open Npgsql

[<EntryPoint>]
let main _ =
    NpgsqlConnection.GlobalTypeMapper.UseNetTopologySuite () |> ignore

    use cmd = DvdRental.CreateCommand<"begin;delete from film where film_id = -5000;end;">(connectionString)
    printfn "%A" (cmd.Execute())
  

    0

