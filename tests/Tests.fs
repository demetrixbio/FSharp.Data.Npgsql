module Tests

open System
open Xunit

open FSharp.Data

[<Literal>]
let connectionString = "Host=localhost;Username=postgres;Password=Leningrad1;Database=lims"

[<Fact>]
let BasicQuery() =
    let cmd = new NpgsqlCommand<"        
        SELECT table_schema || '.' || table_name as name, table_type, row_number() over() as c
        FROM information_schema.tables
        WHERE table_type = @tableType
        LIMIT 10;
    ", connectionString>(connectionString)

    let xs = [ for x in cmd.Execute(tableType = "VIEW") -> x.name, x.table_type, x.c ] 
    Assert.Equal(10, xs.Length)

type Db = Npgsql<connectionString>

[<Fact>]
let CreateCommand() =
    let cmd = Db.CreateCommand<"select 42, current_date", ResultType.Tuples>(connectionString)
    Assert.Equal<_ list>( [ 42, DateTime.Today.Date ], [ for num, date in cmd.Execute() -> num.Value, date.Value.Date ] )
    
[<Fact>]
let SingleRow() =
    use cmd = new NpgsqlCommand<"SELECT NULL", connectionString, SingleRow = true>(connectionString)
    Assert.Equal( Some None, cmd.Execute())
     
