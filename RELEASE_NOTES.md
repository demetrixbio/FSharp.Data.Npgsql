### 0.1.45-beta - March 12th, 2019
* Eager evaluation of query execution results returned from DbReader
* closing/disposing DbReader after evaluation. 
* Return type for Record|Tuple switched to list<_> instead of seq<_> enabling multiple reads of materialized data.
