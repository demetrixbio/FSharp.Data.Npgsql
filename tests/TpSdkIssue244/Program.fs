// Learn more about F# at http://fsharp.org

open FSharp.Data.Npgsql

[<Literal>]
let connection = "Host=localhost;Username=postgres;Password=postgres"

[<EntryPoint>]
let main _ =
    use cmd = new NpgsqlCommand<"SELECT now() as now, @echo::int as value", connection>(connection)
    let x = cmd.Execute(42) |> Seq.exactlyOne
    printfn "Today: %A, echo value: %A" x.now x.value
    0 
