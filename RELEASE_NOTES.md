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