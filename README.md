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

## NpgsqlConnection or NpgsqlCommand?

It's recommended to use ```NpgsqlConnection``` type provider by default. ```NpgsqlCommand``` type provider exists mainly for flexibility.
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

 - Use fully qualified names for `Npgsql.NpgsqlConnection` and `Npgsql.NpgsqlCommand`

 - Use type alias for `Npgsql.NpgsqlConnection` and `Npgsql.NpgsqlCommand`
```fsharp
type PgConnectoin = Npgsql.NpgsqlConnection
type PgCommand = Npgsql.NpgsqlCommand
```

- Isolate usage by module or file  

I expect once you commit to the `NpgsqlCommand` type provider usage of `Npgsql.NpgsqlCommand` type will be very limited so name collision is not an issue.  

`Npgsql.NpgsqlConnection` collision can be solved by a simple helper function:
```fsharp
let openConnection(connectionString) = 
    let conn = new Npgsql.NpgsqlConnection(connectionString)
    conn.Open()
    conn
```

## Async execution
Every instance of generated command has async counterpart of `Execute` method - `AsyncExecute`.

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
_Design-time type providers configuration is never passed to run-time._

Command constructor/factory method expects run-time connection parameter. 
A notable exception is [Fsx](#scripting) flag.
Library doesn't have any support to simplify run-time confirmation but there is machinery to share design-time configuration.  

Configuring instance of `NpgsqlConnection` type provider is simple but configuring numerous instances of `NpgsqlCommand` can be tedious. `Config` and `ConfigFile` properties allow to externalize and therefore share configuration. It also helps to avoid exposing sensitive information in connection string literals. 

- `ConfigType.JsonFile`
```fsharp
do
    use cmd = new NpgsqlCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", "dvdRental", Config = jsonConfig >(dvdRental)  
    //...
```
The type provider will look for connection string named `dvdRental` in file that should have content like:
```json
{
  "ConnectionStrings": {
    "dvdRental": "Host=localhost;Username=postgres;Database=dvdrental;Port=32768"
  }
}
```
- `ConfigType.Environment`

Reads configuration from `ConnectionStrings:dvdRental` environment variable.
```fsharp
do
    use cmd = new NpgsqlCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", "dvdRental", ConfigType = ConfigType.Environment>(dvdRental)
```
- `ConfigType.UserStore`

Reads design time connection string from user store. 
```fsharp
    use cmd = new NpgsqlCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", "dvdRental", ConfigType = ConfigType.UserStore>(dvdRental)
```

For the code above the type provider will try to find _single_ F# project in resolution folder and parse it to extract value of <UserSecretsId> element. This approach relies on several assumptions. Unfortunately more robust way via reading [UserSecretsIdAttribute](https://docs.microsoft.com/en-us/dotnet/api/microsoft.extensions.configuration.usersecrets.usersecretsidattribute?view=aspnetcore-2.0) is not available for the type provider because final assembly is not generated yet. To address this UserSecretsId can be supplied via Config parameter.  
 
```fsharp
do
    use cmd = new NpgsqlCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", "dvdRental", ConfigType = ConfigType.UserStore, Config = "e0db9c78-0c59-4e4f-9d15-ed0c2848e94e">(dvdRental)
    //...
```
User store id is just file name so it can be practically any text.

More on .NET Core configuration is [here](https://docs.microsoft.com/en-us/aspnet/core/fundamentals/configuration/?tabs=basicconfiguration).

## Data modifications
- Hand-written statements
```fsharp
    //deactivate customer if exists and active
    let email = "mary.smith@sakilacustomer.org"

    use cmd = new NpgsqlCommand<" 
            UPDATE public.customer 
            SET activebool = false 
            WHERE email = @email 
                AND activebool
    ", dvdRental, SingleRow = true>(dvdRental)

    let recordsAffected = cmd.Execute(email)
    if recordsAffected = 0 
    then
        printfn "Could not deactivate customer %s" email
    elif recordsAffected = 1
    then 
        use restore = 
            new NpgsqlCommand<" 
                UPDATE public.customer 
                SET activebool = true
                WHERE email = @email 
            ", dvdRental>(dvdRental)
        assert( restore.Execute(email) = 1)    
```
- `ResultType.DataTable` - good to handle updates, deletes, upserts or inserts mixed with any above. 

```fsharp

```

- Statically-typed for inserts-only scenario for example ETL data upload.
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

## Transactions
Every instance of generated by `NpgsqlCoammnd` type provider command has constructor overload that accepts mandatory connection instance and optional transaction instance. Use it to executed commands inside transaction. 
```fsharp
do
    use conn = new Npgsql.NpgsqlConnection(dvdRental)
    conn.Open()
    use tx = conn.BeginTransaction()
    use cmd = new NpgsqlCommand<"        
        INSERT INTO public.actor (first_name, last_name)
        VALUES(@firstName, @lastName)
    ", dvdRental>(conn, tx)
    assert(cmd.Execute(firstName = "Tom", lastName = "Hanks") = 1)
    //Commit to persist changes
    //tx.Commit()    
```
`NpgsqlConnection` type provider handles transaction object diffrerently because [statically parametrized TP methods](https://github.com/fsharp/fslang-design/blob/master/FSharp-4.0/StaticMethodArgumentsDesignAndSpec.md) cannot have overloads by design. Pass extra `XCtor = true` parameter to have `CreateCommand` method signature that accepts connection + optional transaction. `XCtor` stands for extended constructor.
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

## Scripting
To make scripting experience more palatable the type providers accept boolean flag called Fsx. When set it makes run-time connection string parameter optional with default set to design time connection string.
```
type DvdRentalForScripting = NpgsqlConnection<NpgsqlCmdTests.dvdRental, Fsx = true>
do
    use cmd = DvdRentalForScripting.CreateCommand<"SELECT 42 AS Answer">()        
    //...
```

```fsharp
do 
    use cmd = new NpgsqlCommand<"SELECT 42 AS Answer", dvdRental, Fsx = true>()    
    //...
```
Re-using design time connection string allowed only for types evaluated in FSI. Attempt to create command that re-uses design time connection string outside FSI will throw an exception. 

## Limitations

  - One unfortunate PostgreSQL limitation is that column nullability cannot be inferred for derived columns. A command 
  ```fsharp
  use cmd = new NpgsqlCommand<"SELECT 42 AS Answer", dvdRental>(dvdRental)
  assert( cmd.Execute() |> Seq.exactlyOne = Some 42)
  ```
  will infer ```seq<Option<int>>``` as result although it's cleary should be ```seq<int>```. 
  - Custom enums and array types are supported but composite types not yet.

## Running tests
- git clone _--recurse-submodules_ https://github.com/demetrixbio/FSharp.Data.Npgsql.git

From the repo root folder
- dotnet build .\src\DesignTime\
- dotnet build .\src\Runtime\
- docker build -t pg_dvdrental .\tests\
- docker run -d -p 32768:5432 --name dvdrental pg_dvdrental
- dotnet test .\tests\

