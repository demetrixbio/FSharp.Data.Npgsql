## Description
FSharp.Data.Npgsql is F# type providers library on a top of well-known [Npgsql ADO.NET client library]( http://www.npgsql.org/doc/index.html). 

The library includes two type providers: NpgsqlConnection and NpgsqlCommand. 

## Nuget package

https://www.nuget.org/packages/FSharp.Data.Npgsql/

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
    use cmd = new NpgsqlCommand<"SELECT title FROM public.film WHERE length > @longer_than", dvdRental>(dvdRental)
    let longerThan = System.TimeSpan.FromHours(3.)
    cmd.Execute(longer_than = int16 longerThan.TotalMinutes)
    |> Seq.toList 
    |> printfn "Movies longer than %A:\n%A" longerThan 
```

## Retrieve singleton record
Specify "SingleRow = true" to retrieve single row result. Command execution throws an exception if result set contains more than one row.

```fsharp
do
    use cmd = DvdRental.CreateCommand<"SELECT current_date as today", SingleRow = true>(dvdRental)
    cmd.Execute() |> printfn "Today is: %A"
```

```fsharp
do 
    use cmd = new NpgsqlCommand<"SELECT current_date as today", dvdRental, SingleRow = true>(dvdRental)
    cmd.Execute() |> printfn "Today is: %A"
```

## Result types

There 4 result types:
 - `ResultType.Record` (default) - returns F# record like class with read-only properties.  See see examples above. 
 - `ResultType.Tuples` - In practice it's rarely useful but why not? 
 ```fsharp
 do
     use cmd = DvdRental.CreateCommand<"SELECT title, release_year FROM public.film LIMIT 3", ResultType.Tuples>(dvdRental)
    for title, releaseYear in cmd.Execute() do   
        printfn "Movie '%s' released in %i." title releaseYear.Value
 ```
 - `ResultType.DataTable` comes handy when you need to do updates, deletes or upserts. For insert only ETL-like workloads use statically typed data tables. See [Data modifications](#data-modifications) section for details. 
 - `ResultType.DataReader` returns plain NpgsqlDataReader. I think passing it as a parameter to [DataTable.Load](https://docs.microsoft.com/en-us/dotnet/api/system.data.datatable.load?view=netstandard-2.0) for merge/upsert 
is the only useful scenario. 

## NpgsqlConnection or NpgsqlCommand?

It's recommended to use ```NpgsqlConnection``` by default. ```NpgsqlCommand``` exists mainly for flexibility.
```NpgsqlConnection``` reduces design-time configuration bloat by having it all in one place. 

But, but ... because ```NpgsqlConnection``` relies on fairly new F# compiler feature [statically parametrized TP methods](https://github.com/fsharp/fslang-design/blob/master/FSharp-4.0/StaticMethodArgumentsDesignAndSpec.md) Intellisense often fails. It  shows red squiggles even though code compiles just fine. Hopefully it will be fixed soon. Pick you poison: better code or better development experience. 

## Naming 

Both type providers have local type names that collide with types from Npgsql library. I admit it's slightly controversial decision but naming is too important to be compromised on. I believe both names best communicate the intent. 
If you'll end up having following error message :
```
...
FS0033	The non-generic type 'Npgsql.NpgsqlCommand' does not expect any type arguments, but here is given 3 type argument(s)
...
```
or
```
...
FS0033	The non-generic type 'Npgsql.NpgsqlConnection' does not expect any type arguments, but here is given 2 type argument(s)	
...
```

It means that types from Npgsql shadowed the type providers because ```open FSharp.Data.Npgsql``` statement was followed by ```open Npgsql```

There are several ways to work around the issue:

 - Use fully qualified names for type providers. For example:

```fsharp
type DvdRental = FSharp.Data.Npgsql.NpgsqlConnection<connectionStringName, Config = config>

type BasicQuery = FSharp.Data.Npgsql.NpgsqlCommand<"SELECT title, release_year FROM public.film LIMIT 3", dvdRental>
//or
do
    use cmd = new FSharp.Data.Npgsql.NpgsqlCommand<"SELECT title, release_year FROM public.film LIMIT 3", dvdRental>(dvdRental)
```
 
It's good solution for ```NpgsqlConnection``` provider but for ```NpgsqlCommand``` provider it will cause a lot of extra typing and reduce readability a little. 

 - Use fully qualified names for 'Npgsql.NpgsqlConnection and Npgsql.NpgsqlCommand

 - Use type alias for 'Npgsql.NpgsqlConnection and Npgsql.NpgsqlCommand
```fsharp
type PgConnectoin = Npgsql.NpgsqlConnection
type PgCommand = Npgsql.NpgsqlCommand
```

- Isolate usage by module or file  

I expect once you commit to the ```NpgsqlCommand``` type provider usage of ```Npgsql.NpgsqlCommand``` type will be very limited so name collision is not an issue.  

```Npgsql.NpgsqlConnection``` collision can be solved by a simple helper function:
```fsharp
let openConnection(connectionString) = 
    let conn = new Npgsql.NpgsqlConnection(connectionString)
    conn.Open()
    conn
```

## Async execution
Every instance of generated command has async counterpart of ```Execute``` method - ```AsyncExecute```.

```fsharp
do
    use cmd = DvdRental.CreateCommand<"SELECT title, release_year FROM public.film LIMIT 3">(dvdRental)
    for x in cmd.AsyncExecute() |> Async.RunSynchronously do   
        printfn "Movie '%s' released in %i." x.title x.release_year.Value
```

```fsharp
do
    use cmd = new NpgsqlCommand<"SELECT title, release_year FROM public.film LIMIT 3", dvdRental>(dvdRental)
    for x in cmd.AsyncExecute() |> Async.RunSynchronously do   
        printfn "Movie '%s' released in %i." x.title x.release_year.Value
```

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
