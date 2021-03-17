### 1.1.0 - March 17th, 2021
- Rows for `ResultType.Record` are now erased to a tuple instead of `obj[]`. This results in faster property access and makes it possible to read value types from Npgsql with fewer allocations.
- Users of PostGIS are now required to set up a global type mapper for NetTopologySuite in their startup code.
  ```fsharp
  open type Npgsql.NpgsqlNetTopologySuiteExtensions

  Npgsql.NpgsqlConnection.GlobalTypeMapper.UseNetTopologySuite () |> ignore
  ```

### 1.0.1 - March 9th, 2021
- Minor performance optimizations

### 1.0.0 - February 18th, 2021
- Fixed SingleRow for multiple result sets