## Description
FSharp.Data.Npgsql is F# type providers library on a top of well-known [Npgsql ADO.NET client library]( http://www.npgsql.org/doc/index.html). 

## Nuget package

https://www.nuget.org/packages/FSharp.Data.Npgsql/

## Target platforms: 
  - netstandard2.0
 
## Setup

All examples based on [DVD rental sample database](http://www.postgresqltutorial.com/download/dvd-rental-sample-database/) and assume following definitions exist:
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

## Parameterized query

```fsharp
do
    use cmd = DvdRental.CreateCommand<"SELECT title FROM public.film WHERE length > @longer_than">(dvdRental)
    let longerThan = TimeSpan.FromHours(3.)
    let xs: string list = cmd.Execute(longer_than = int16 longerThan.TotalMinutes) |> Seq.toList 
    printfn "Movies longer than %A:\n%A" longerThan xs
```

## Retrieve singleton record
Specify "SingleRow = true" to retrieve single row result. Command execution throws an exception if result set contains more than one row.

```fsharp
do
    use cmd = DvdRental.CreateCommand<"SELECT current_date as today", SingleRow = true>(dvdRental)
    cmd.Execute() |> printfn "Today is: %A"
```

## Result types

There are 4 result types:
 - `ResultType.Record` (default) - returns F# record like class with read-only properties.  See examples above. 
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

## Reuse of provided records

By default, every `CreateCommand` generates a completely seperate type when using `ResultType.Record`. This can be annoying when you have similar queries that return the same data structurally and you cannot, for instance, use one function to map the results onto your domain model.
`NpgsqlConnection` exposes the static parameter `ReuseProvidedTypes` to alleviate this issue. When set to true, all commands that return the same columns (**column names and types must match exactly**, while select order does not matter) end up sharing the same provided record type too.
The following snippet illustrates how you could reuse a single function to map the result of 2 distinct queries onto the `Film` type:

```fsharp
type DvdRental = NpgsqlConnection<dvdRental, ReuseProvidedTypes = true>

type Film = { Title: string; Rating: DvdRental.``public``.Types.mpaa_rating option }

// CreateCommand returning a type we want to refer to in a function signature has to be 'mentioned' first
let getAllFilmsWithRatingsCommand = DvdRental.CreateCommand<"select title, rating from film">

// The type with title and rating is now generated and accessible
let mapFilm (x: DvdRental.``rating:Option<public.mpaa_rating>, title:String``) =
    { Title = x.title; Rating = x.rating }
	
let getAllFilmsWithRatings () =
    use cmd = getAllFilmsWithRatingsCommand dvdRental
    let res = cmd.Execute()
    res |> List.map mapFilm
	
let getFilmWithRatingById id =
    use cmd = DvdRental.CreateCommand<"select title, rating from film where film_id = @id", SingleRow = true>(dvdRental)
    let res = cmd.Execute(id)
    res |> Option.map mapFilm
```

## Naming 

The type provider's local type names collide with types from Npgsql library. I admit it's a slightly controversial decision but naming is too important to be compromised on. 
If you end up having following error message:

```
...
FS0033	The non-generic type 'Npgsql.NpgsqlConnection' does not expect any type arguments, but here is given 2 type argument(s)	
...
```

It means that types from Npgsql shadowed the type providers because ```open FSharp.Data.Npgsql``` statement was followed by ```open Npgsql```

There are several ways to work around the issue:

 - Use fully qualified names for the type provider. For example:

```fsharp
type DvdRental = FSharp.Data.Npgsql.NpgsqlConnection<connectionString>
```

 - Use a fully qualified name for `Npgsql.NpgsqlConnection`

 - Use a type alias for `Npgsql.NpgsqlConnection`
```fsharp
type PgConnectoin = Npgsql.NpgsqlConnection
```

- Isolate usage by module or file  

`Npgsql.NpgsqlConnection` collision can be solved by a simple helper function:
```fsharp
let openConnection(connectionString) = 
    let conn = new Npgsql.NpgsqlConnection(connectionString)
    conn.Open()
    conn
```

## Async execution
Every instance of a generated command has async counterpart of `Execute` method - `AsyncExecute`.

```fsharp
do
    use cmd = DvdRental.CreateCommand<"SELECT title, release_year FROM public.film LIMIT 3">(dvdRental)
    for x in cmd.AsyncExecute() |> Async.RunSynchronously do   
        printfn "Movie '%s' released in %i." x.title x.release_year.Value
```

## Prepared statements
[Prepared statements](https://www.npgsql.org/doc/prepare.html) are supported by setting the static parameter `Prepare` to `true`. For `NpgsqlConnection` this can be set when defining the type itself and also overriden when calling `CreateCommand`.

```fsharp
// All commands created from this type will be prepared
type DvdRental = NpgsqlConnection<dvdRental, Prepare = true>

do
    // Will be prepared
    use cmd = DvdRental.CreateCommand<"SELECT title, release_year FROM public.film LIMIT 3">(dvdRental)
    for x in cmd.Execute() do   
        printfn "Movie '%s' released in %i." x.title x.release_year.Value

do
    // Overrides the DvdRental setting and thus won't be prepared
    use cmd = DvdRental.CreateCommand<"SELECT title, release_year FROM public.film LIMIT 3", Prepare = false>(dvdRental)
    for x in cmd.Execute() do   
        printfn "Movie '%s' released in %i." x.title x.release_year.Value

```

## Configuration
_Design-time type providers configuration is never passed to run-time._ `CreateCommand` expects a run-time connection parameter.

## Data modifications
- Hand-written statements
```fsharp
    //deactivate customer if exists and active
    let email = "mary.smith@sakilacustomer.org"

    use cmd = DvdRental.CreateCommand<" 
            UPDATE public.customer 
            SET activebool = false 
            WHERE email = @email 
                AND activebool
    ", SingleRow = true>(dvdRental)

    let recordsAffected = cmd.Execute(email)
    if recordsAffected = 0 
    then
        printfn "Could not deactivate customer %s" email
    elif recordsAffected = 1
    then 
        use restore = 
            DvdRental.CreateCommand<" 
                UPDATE public.customer 
                SET activebool = true
                WHERE email = @email 
            ">(dvdRental)
        assert( restore.Execute(email) = 1)    
```
- `ResultType.DataTable` - good to handle updates, deletes, upserts or inserts mixed with any above. 

```fsharp
    //Deactivate customer if found and active 
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tx = conn.BeginTransaction()    
    use cmd = 
        DvdRental.CreateCommand<"
            SELECT customer_id, activebool
            FROM public.customer 
            WHERE email = @email  
        ", ResultType.DataTable>(conn, tx)
    let t = cmd.Execute(email = "mary.smith@sakilacustomer.org")
    if t.Rows.Count > 0 && t.Rows.[0].activebool
    then 
        t.Rows.[0].activebool <- true
        assert( t.Update(conn, tx) = 1)

    //Commit to persist changes
    //tx.Commit() 
```

- Statically-typed data tables for inserts-only scenarios (for example ETL). Avalable only on ```NpgsqlConnection``` type provider.
```fsharp
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tx = conn.BeginTransaction()
    let t = new DvdRental.``public``.Tables.actor()

    let r = t.NewRow(first_name = "Tom", last_name = "Hanks")
    t.Rows.Add(r)

    //or
    //t.AddRow(first_name = "Tom", last_name = "Hanks")
    //let r = t.Rows.[0] 

    assert( t.Update(conn, tx) = 1)
    printfn "Identity 'actor_id' %i and column with default 'last update': %A auto-fetched." r.actor_id r.last_update
```
Worth noting that statically typed tables know to auto-fetch generated ids and default values after insert but only if updateBatchSize parameter set 1 (which is default). 

## Transactions
  
In order to use transactions across commands, pass the `XCtor = true` static parameter to `CreateCommand` so that the method signature accepts a connection + optional transaction. `XCtor` stands for extended constructor.

```fsharp
do
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tx = conn.BeginTransaction()
    use cmd = 
        DvdRental.CreateCommand<"        
            INSERT INTO public.actor (first_name, last_name)
            VALUES(@firstName, @lastName)`
        ", XCtor = true>(conn, tx)
    assert(cmd.Execute(firstName = "Tom", lastName = "Hanks") = 1)
    //Commit to persist changes
    //tx.Commit()
```
`XCtor` also can be set on top level effectively making all `CreateCommand` methods to accept connection + transaction combination. 
```fsharp
type DvdRentalXCtor = NpgsqlConnection<dvdRental, XCtor = true>
do
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tx = conn.BeginTransaction()
    use cmd = 
        DvdRentalXCtor.CreateCommand<"        
            INSERT INTO public.actor (first_name, last_name)
            VALUES(@firstName, @lastName)
        ">(conn, tx)
    assert(cmd.Execute(firstName = "Tom", lastName = "Hanks") = 1)
    //Commit to persist changes
    //tx.Commit()
  ```

## Optional input parameters
By default all input parameters of `Execute`/`AsyncExecute` methods generated by the type providers are mandatory. There are rare cases when you prefer to handle NULL input values inside SQL script. `AllParametersOptional` set to true makes all parameters optional.
```fsharp
do
    use cmd = new NpgsqlCommand<"
        SELECT coalesce(@x, 'Empty') AS x
    ", dvdRental, AllParametersOptional = true, SingleRow = true>(dvdRental)
    
    assert( cmd.Execute(Some "test") = Some( Some "test")) 
    assert( cmd.Execute() = Some( Some "Empty")) 
```

## Bulk Copy
To upload fast large amount of data use `BinaryImport` method on statically typed data tables:
```fsharp
    let firstName, lastName = "Tom", "Hanks"
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tx = conn.BeginTransaction()

    let actors = new DvdRental.``public``.Tables.actor()
        
    let actor_id = 
        use cmd = DvdRental.CreateCommand<"select nextval('actor_actor_id_seq' :: regclass)::int", SingleRow = true, XCtor = true>(conn, tx)
        cmd.Execute() |> Option.flatten 
    
    //Binary copy operation expects all columns including auto-generated and having defaults to be populated. 
    //Therefore we must provide values for actor_id and last_update columns which are optional for plain Update method. 
    actors.AddRow(actor_id, first_name = "Tom", last_name = "Hanks", last_update = Some DateTime.Now)
    
    actors.BinaryImport(conn)

    use cmd = 
        DvdRental.CreateCommand<
            "SELECT COUNT(*) FROM public.actor WHERE first_name = @firstName AND last_name = @lastName", 
            SingleRow = true, 
            XCtor = true>(conn, tx)

    assert(Some( Some 1L) = cmd.Execute(firstName, lastName))
```
Worth noting that `BinaryImport` operation expects _all_ columns including auto-generated and having defaults to be populated with values. 
`BinaryImport` implementation uses Npgsql [Binary COPY](http://www.npgsql.org/doc/copy.html#binary-copy).

## Limitations

  - One unfortunate PostgreSQL limitation is that column nullability cannot be inferred for derived columns. A command 
  ```fsharp
  use cmd = DvdRental.CreateCommand<"SELECT 42 AS Answer">(dvdRental)
  assert( cmd.Execute() |> Seq.exactlyOne = Some 42)
  ```
  will infer ```seq<Option<int>>``` as result although it cleary should be ```seq<int>```. 
  - Data modification batch processing is [not supported](https://github.com/npgsql/npgsql/issues/1830).

## Running tests
From the repo root folder
- dotnet build .\src\DesignTime\
- dotnet build .\src\Runtime\
- docker build -t pg_dvdrental .\tests\
- docker run -d -p 5432:5432 --name dvdrental pg_dvdrental
- dotnet test .\tests\

## Releasing package

(Notes to self)
```
cd src/Design
dotnet build -c Release
cd ../Runtime
dotnet build -c Release
vi paket.template 
bump version number
paket pack
paket push FSharp.Data.Npgsql.<versionNumber>.nupkg --api-key <insert api key>
```

