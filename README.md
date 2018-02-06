## Description
FSharp.Data.Npgsql is F# type providers library on a top of well-known [Npgsql ADO.NET client library]( http://www.npgsql.org/doc/index.html). 

The library includes two type providers: NpgsqlConnection and NpgsqlCommand. 

## Target platforms: 
  - netstandard2.0
  - net461
    
  To compile on Linux/Mac F# project consuming the type provider make sure to run on [Mono 5.8.0](http://www.mono-project.com/docs/about-mono/releases/5.8.0/) or later. Mono 5.4.1 was failing for me with mysterious errors. Also apply [this fix](https://github.com/Microsoft/visualfsharp/issues/3303#issuecomment-331426983) to your project file. See examples [here](https://github.com/fsprojects/FSharp.TypeProviders.SDK/tree/master/examples). 
  
## Setup

All examples based on [DVD rental sample database](http://www.postgresqltutorial.com/download/dvd-rental-sample-database/) and assume following definitions to exist:
```fsharp
[<Literal>]
let dvdRental = "Host=localhost;Username=postgres;Database=dvdrental"

open FSharp.Data.Npgsql

type DvdRental = NpgsqlConnection<dvdRental>
```

## Basic query

```fsharp
do
    use cmd = DvdRental.CreateCommand<"SELECT title, release_year FROM public.film LIMIT 3">(dvdRental)

    for x in cmd.Execute() do   
        printfn "Movie '%s' released in %i." x.title x.release_year.Value
```
Alternatevly using inline ```NpgsqlCommand``` definition.
```fsharp
do
    use cmd = new NpgsqlCommand<"SELECT title, release_year FROM public.film LIMIT 3", dvdRental>(dvdRental)
    //...
```
Or using ```NpgsqlCommand``` with explicit type alias. `Create` factory method can be used in addition to traditional constructor. It mainly exists to work around [Intellisense deficiency]().

```fsharp
type BasicQuery = NpgsqlCommand<"SELECT title, release_year FROM public.film LIMIT 3", dvdRental>
do 
    use cmd = BasicQuery.Create(dvdRental)
    //...
```

## Parameterized query

```fsharp
do
    use cmd = DvdRental.CreateCommand<"SELECT title FROM public.film WHERE length > @longer_than">(dvdRental)
    let longerThan = TimeSpan.FromHours(3.)
    let xs: string list = cmd.Execute(longer_than = int16 longerThan.TotalMinutes) |> Seq.toList 
    printfn "Movies longer than %A:\n%A" longerThan xs
```
```NpgsqlCommand``` version:
```fsharp
do
  use cmd = DvdRental.CreateCommand<"SELECT title FROM public.film WHERE length > @longer_than">(dvdRental)
  let longerThan = TimeSpan.FromHours(3.)
  let xs: string list = cmd.Execute(longer_than = int16 longerThan.TotalMinutes) |> Seq.toList 
  printfn "Movies longer than %A:\n%A" longerThan xs
```

## NpgsqlConnection or NpgsqlCommand?

It's recommended to use NpgsqlConnection by default.  NpgsqlCommand exists mainly for flexibility.

## Async execution

## Naming 

## Configuration

## Data modifications

## Scripting

## Transactions

## Limitations

  - One unfortunate PostgreSQL limitation is that column nullability cannot be inferred for derived columns. A command 
  ```fsharp
  use cmd = new NpgsqlCommand<"SELECT 42 AS Answer", dvdRental>(dvdRental)
  assert( cmd.Execute() |> Seq.exactlyOne = Some 42)
  ```
  will infer ```seq<Option<int>>``` as result although it's cleary should be ```seq<int>```. 
  - Custom enums and array types are supported but composite types not yet.

## Running tests

## Implemenation details
