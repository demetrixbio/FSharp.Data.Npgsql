module Program

open System
open NpgsqlConnectionTests
open Npgsql
open System.Data
open System.Data.Common

let statementIndexGetter =
    typeof<NpgsqlDataReader>.GetProperty("StatementIndex", Reflection.BindingFlags.Instance ||| Reflection.BindingFlags.NonPublic).GetMethod

let GetStatementIndex(cursor: DbDataReader) =
    statementIndexGetter.Invoke(cursor, null) :?> int
let controlCommandRegex = System.Text.RegularExpressions.Regex ("^\\s*\\b(?:begin|end)\\b\\s*$", Text.RegularExpressions.RegexOptions.IgnoreCase)

[<EntryPoint>]
let main _ =
    use cmd = DvdRental.CreateCommand<"begin;delete from film where film_id = -5000;end;">(connectionString)
    printfn "%A" (cmd.Execute())

    let conn = Npgsql.NpgsqlConnection (connectionString)
    conn.Open()
    let cmd = NpgsqlCommand ("begin;delete from film where film_id = -5000;end;", conn)
    use cursor = cmd.ExecuteReader CommandBehavior.SchemaOnly
    

    0

