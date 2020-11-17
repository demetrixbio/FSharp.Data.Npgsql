module Program

open System
open NpgsqlConnectionTests

[<EntryPoint>]
let main _ =
    use cmd = DvdRental.CreateCommand<"SELECT case when g % 2 = 0 then g else null end FROM generate_series(0, 10) g">(connectionString)
    printfn "%A" (cmd.Execute ())

    0

