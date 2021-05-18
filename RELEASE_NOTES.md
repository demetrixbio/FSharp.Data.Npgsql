### 0.1.45-beta - March 12th, 2019
* Eager evaluation of query execution results returned from DbReader
* closing/disposing DbReader after evaluation.
* Return type for Record|Tuple switched to list<_> instead of seq<_> enabling multiple reads of materialized data.

### 0.1.46-beta - March 12th, 2019
* Prepared statements / automatic preparation support for NpgsqlCommand and NpgsqlConnection providers.
* Bytea column type support.
* Schema caching for type inference, performance improvements.

### 0.1.47-beta - October 28th, 2019
* Fixed timestamptz Npgsql parameter handling by DataTable/DbCommand api. Insertions of DateTime into timestamptz column always delegate conversion handling to underlying Npgsql library.

### 0.1.48-beta - November 6th, 2019
* Added json/jsonb Npgsql parameter handling for DataTable/DbCommand api.

### 0.1.49-beta - January 13th, 2020
* DataTable.Update wrong DbParameter type fix in case of multiple update statements.

### 0.1.50-beta - February 27th, 2020
* Materialized views support by @kerams
* Partitioned table support by @kerams
* Bug fixes to enable enum support in NpgsqlCommand type provider, querying against system catalogs

### 0.2.0-beta - March 5th, 2020
* Quotation optimizations by @kerams
* Inferring rowmapping/nullability in runtime to remove redundant quotation passing. Modified provided types SDK for better performance.

### 0.2.3-beta - March 10th, 2020
* Provided record reuse support by @kerams

### 0.2.4-beta - March 28th, 2020
* Interval insert/update fix for DataTable api. Correct schema inferred for user defined types.

### 0.2.5-beta - April 3rd, 2020
* Switch from netcore2.0 to netstandard2.0. Updated dependencies, switched tests to netcore3.1

### 0.2.6-beta - April 3rd, 2020
* Removed net461 framework dependency.

### 0.2.7-beta - April 6th, 2020
* Use paket for builds / nuget publish. Github CI actions by @swoorup
* Timescaledb fix by @swoorup

### 0.2.8-beta - April 7th, 2020
* Switch from LegacyPostgis to NetTopologySuite by @swoorup

### 0.2.9-beta - May 30th, 2020
* Fix postgis params issue by @swoorup
* Generated column support for DataTable api

### 0.2.10-beta - Jun 26th, 2020
* #93 - @sandeepc24 fix for geometry column

### 1.0.0 - February 18th, 2021
* Fixed SingleRow for multiple result sets

### 1.0.1 - March 9th, 2021
* Minor performance optimizations

### 1.1.0 - March 17th, 2021
* Rows for `ResultType.Record` are now erased to a tuple instead of `obj[]`. This results in faster property access and makes it possible to read value types from Npgsql with fewer allocations.
* Users of PostGIS are now required to set up a global type mapper for NetTopologySuite in their startup code.
  ```fsharp
  open type Npgsql.NpgsqlNetTopologySuiteExtensions

  Npgsql.NpgsqlConnection.GlobalTypeMapper.UseNetTopologySuite () |> ignore
  ```
  
### 1.2.0 March 23rd, 2021
* Merge keram's fork into FSharp.Data.Npgsql repo.
* BREAKING - Bumped Npgsql dependency to v5
* BREAKING - Removed `NpgsqlCommand`
  - Create one root `NpgsqlConnection` type, and use `CreateCommand` to generate commands you need from that.
* BREAKING - Removed the `Config` static parameter on `NpgsqlConnection` (primarily because of its limited utility and a [bug](https://github.com/dotnet/fsharp/issues/9265) in VS tooling that causes IntelliSense errors and will be fixed who knows when)
  - Use an inline connection string in `NpgsqlConnection` or define a literal for it.
* BREAKING - Removed the `Fsx` static parameter
  - Always pass a connection string to `CreateCommand` in F# Interactive.
* BREAKING - Renamed `ResultSetX` to `RowsAffectedX` for statements that are non-queries and return the number of affected rows
  - For a command like `DvdRental.CreateCommand<"select * from film; delete from actor">(cs)` use `RowsAffected2` instead of `ResultSet2` to get the number of deleted rows.
* BREAKING - Removed provided constructors for result set types
* BREAKING - Made [`BinaryImport`](#Bulk-Copy) aware of identity columns
* `BinaryImport` returns the number of imported rows as `uint64`
* Added `TaskAsyncExecute` that returns `System.Threading.Tasks.Task<_>`
* Added the [`CollectionType`](#Collection-types) static parameter
* Added the [`MethodTypes`](#Method-types) static parameter
* Added XML docs to result set properties, containing the corresponding SQL statements
* Made a bunch of design- and runtime performance improvements

### 1.2.1 March 24th, 2021
* Fix missing type definition error due to cache invalidation / different name

### 1.2.2 May 18th, 2021
* Default command timeout for data table and command taken from connection string. Connection/command level override for timeout variable exposed