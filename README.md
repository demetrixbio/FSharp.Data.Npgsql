## Description
FSharp.Data.Npgsql is F# type providers library on a top of well-known [Npgsql ADO.NET client library]( http://www.npgsql.org/doc/index.html). 

The library includes two type providers: NpgsqlConnection and NpgsqlCommand. It's recommended to use NpgsqlConnection by default.  NpgsqlCommand exists mainly for flexibility.

## Getting started

All examples based on [DVD rental sample database](http://www.postgresqltutorial.com/download/dvd-rental-sample-database/) and assume following connection string literal defined.
```fsharp
[<Literal>]
let dvdRental = "Host=localhost;Username=postgres;Database=dvdrental;Port=32768"
```

To retrieve set of records 
```fsharp

```

## Configuration

## Scripting

## Data modifications

## Transactions

## Limitations

  ### One unfortunate PostgreSQL limitation is that column nullability cannot be inferred for derived columns. A command 
  ```fsharp
  use cmd = new NpgsqlCommand<"SELECT 42 AS Answer", dvdRental>(dvdRental)
  assert( cmd.Execute() |> Seq.exactlyOne = Some 42)
  ```

## Running tests
