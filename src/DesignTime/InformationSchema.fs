module internal FSharp.Data.Npgsql.DesignTime.InformationSchema 

open System
open System.Data
open System.Data.Common
open System.Collections.Generic

open FSharp.Quotations

open Npgsql
open Npgsql.PostgresTypes
open NpgsqlTypes

open FSharp.Data.Npgsql
open ProviderImplementation.ProvidedTypes
open System.Collections
open System.Net
open System.Reflection

type internal DbDataReader with

    member cursor.GetValueOrDefault(name: string, defaultValue) =    
        let i = cursor.GetOrdinal(name)
        if cursor.IsDBNull( i) then defaultValue else cursor.GetFieldValue( i)
        
    member cursor.GetOptionalValue(name: string) =    
        let i = cursor.GetOrdinal(name)
        if cursor.IsDBNull( i) then None else Some <| cursor.GetValue( i)

type internal Type with
    member this.PartiallyQualifiedName = 
        sprintf "%s, %s" this.FullName (this.Assembly.GetName().Name)

//https://www.postgresql.org/docs/current/static/datatype.html#DATATYPE-TABLE
let private builtins = [
    "boolean", typeof<bool>; "bool", typeof<bool>

    "smallint", typeof<int16>; "int2", typeof<int16>
    "integer", typeof<int32>; "int", typeof<int32>; "int4", typeof<int32>
    "bigint", typeof<int64>; "int8", typeof<int64>

    "real", typeof<single>; "float4", typeof<single>
    "double precision", typeof<double>; "float8", typeof<double>

    "numeric", typeof<decimal>; "decimal", typeof<decimal>
    "money", typeof<decimal>
    "text", typeof<string>

    "character varying", typeof<string>; "varchar", typeof<string>
    "character", typeof<string>; "char", typeof<string>

    "citext", typeof<string>
    "jsonb", typeof<string>
    "json", typeof<string>
    "xml", typeof<string>
    "point", typeof<NpgsqlPoint>
    "lseg", typeof<NpgsqlLSeg>
    "path", typeof<NpgsqlPath>
    "polygon", typeof<NpgsqlPolygon>
    "line", typeof<NpgsqlLine>
    "circle", typeof<NpgsqlCircle>
    "box", typeof<bool>

    "bit", typeof<BitArray>; "bit(n)", typeof<BitArray>; "bit varying", typeof<BitArray>; "varbit", typeof<BitArray>

    "hstore", typeof<IDictionary>
    "uuid", typeof<Guid>
    "cidr", typeof<ValueTuple<IPAddress, int>>
    "inet", typeof<IPAddress>
    "macaddr", typeof<NetworkInformation.PhysicalAddress>
    "tsquery", typeof<NpgsqlTsQuery>
    "tsvector", typeof<NpgsqlTsVector>

    "date", typeof<DateTime>
    "interval", typeof<TimeSpan>
    "timestamp without time zone", typeof<DateTime>; "timestamp", typeof<DateTime>   
    "timestamp with time zone", typeof<DateTime>; "timestamptz", typeof<DateTime>
    "time without time zone", typeof<TimeSpan>; "time", typeof<TimeSpan>
    "time with time zone", typeof<DateTimeOffset>; "timetz", typeof<DateTimeOffset>

    "bytea", typeof<byte[]>
    "oid", typeof<UInt32>
    "xid", typeof<UInt32>
    "cid", typeof<UInt32>
    "oidvector", typeof<UInt32[]>
    "name", typeof<string>
    "char", typeof<string>
    
    "regtype", typeof<UInt32>
    "regclass", typeof<UInt32>
    //"range", typeof<NpgsqlRange>, NpgsqlDbType.Range)
]

let mutable private spatialTypesMapping = [
    "geometry", typeof<NetTopologySuite.Geometries.Geometry>
    "geography", typeof<NetTopologySuite.Geometries.Geometry>
]

let getTypeMapping = 
    let allMappings = dict (builtins @ spatialTypesMapping)
    fun datatype -> 
        let exists, value = allMappings.TryGetValue(datatype)
        if exists then value else typeof<obj> 

type PostgresType with    
    member this.ToClrType() = 
        match this with
        | :? PostgresBaseType as x -> 
            getTypeMapping x.Name
        | :? PostgresEnumType ->
            typeof<string>
        | :? PostgresDomainType as x -> 
            x.BaseType.ToClrType()
        | :? PostgresTypes.PostgresArrayType as arr ->
            arr.Element.ToClrType().MakeArrayType()
        | _ -> 
            typeof<obj>
        
type DataType = {
    Name: string
    Schema: string
    ClrType: Type
}   with    
    member this.FullName = sprintf "%s.%s" this.Schema this.Name
    member this.IsUserDefinedType = this.Schema <> "pg_catalog"
    member this.IsFixedLength = this.ClrType.IsValueType
    
    member this.UdtTypeShortName = 
        if this.ClrType.IsArray 
        then this.Name.Substring(0, this.Name.Length - 2) // my_enum[] -> my_enum
        else this.Name
    
    member this.UdtTypeName = 
        sprintf "%s.%s" this.Schema this.UdtTypeShortName

    static member Create(x: PostgresTypes.PostgresType) = 
        { 
            Name = x.Name
            Schema = x.Namespace
            ClrType = x.ToClrType()
        }

type Schema =
    { OID : uint32
      Name : string }
    
type Table =
    { OID : uint32
      Name : string
      Description : string option }
    
type Enum =
    { Schema : string
      Name : string
      Values : string [] }
    
type Column =
    { ColumnAttributeNumber : int16
      Name: string
      DataType: DataType
      Nullable: bool
      MaxLength: int
      ReadOnly: bool
      AutoIncrement: bool
      DefaultConstraint: string
      Description: string
      PartOfPrimaryKey: bool
      BaseSchemaName: string
      BaseTableName: string }
    with

    member this.ClrType = this.DataType.ClrType

    member this.ClrTypeConsideringNullability = 
        if this.Nullable
        then typedefof<_ option>.MakeGenericType this.DataType.ClrType
        else this.DataType.ClrType

    member this.HasDefaultConstraint = string this.DefaultConstraint <> ""
    member this.OptionalForInsert = this.Nullable || this.HasDefaultConstraint || this.AutoIncrement

    member this.MakeProvidedType (customTypes : Map<string, ProvidedTypeDefinition>, ?forceNullability: bool) =
        let nullable = defaultArg forceNullability this.Nullable
        if this.DataType.IsUserDefinedType && customTypes.ContainsKey(this.DataType.UdtTypeName) then 
            let providedType = customTypes.[this.DataType.UdtTypeName]
            let t = if this.DataType.ClrType.IsArray then providedType.MakeArrayType() else upcast providedType
            if nullable then ProvidedTypeBuilder.MakeGenericType(typedefof<_ option>, [ t ]) else t
        else
            if nullable then typedefof<_ option>.MakeGenericType this.ClrType else this.ClrType

    member this.ToDataColumnExpr () =
        let mi = typeof<Utils>.GetMethod ("ToDataColumn", BindingFlags.Static ||| BindingFlags.Public)
        let typeName = 
            let clrType = if this.ClrType.IsArray then typeof<Array> else this.ClrType
            clrType.PartiallyQualifiedName

        Expr.Call (mi, [
            Expr.Value this.Name
            Expr.Value typeName
            Expr.Value (this.DataType.Name = "timestamptz" && this.ClrType = typeof<DateTime>)
            Expr.Value (this.DataType.Name = "timestampt" && this.ClrType = typeof<DateTime>)
            Expr.Value (this.DataType.Name = "json")
            Expr.Value (this.DataType.Name = "jsonb")
            Expr.Value (not this.ClrType.IsArray && this.DataType.IsUserDefinedType)
            Expr.Value this.AutoIncrement
            Expr.Value (this.Nullable || this.HasDefaultConstraint)
            Expr.Value this.ReadOnly
            Expr.Value (if this.ClrType = typeof<string> then this.MaxLength else -1)
            Expr.Value this.PartOfPrimaryKey
            Expr.Value this.Nullable
            Expr.Value this.ClrType.PartiallyQualifiedName
            Expr.Value this.BaseSchemaName
            Expr.Value this.BaseTableName
        ])

type StatementType =
    | Query of Column list
    | NonQuery
    | Control

type DbSchemaLookupItem =
    { Schema : Schema
      Tables : Dictionary<Table, HashSet<Column>>
      Enums : Map<string, Enum> }
    
type ColumnLookupKey = { TableOID : uint32; ColumnAttributeNumber : int16 }
    
type DbSchemaLookups =
    { Schemas : Dictionary<string, DbSchemaLookupItem>
      Columns : Dictionary<ColumnLookupKey, Column> }
    
type Parameter =
    { Name: string
      NpgsqlDbType: NpgsqlTypes.NpgsqlDbType
      Direction: ParameterDirection 
      MaxLength: int
      Precision: byte
      Scale : byte
      Optional: bool
      DataType: DataType }

let inline openConnection connectionString =  
    let conn = new NpgsqlConnection(connectionString)
    conn.Open()
    conn

let controlCommandRegex = System.Text.RegularExpressions.Regex ("^\\s*\\b(begin|end)\\b\\s*$", Text.RegularExpressions.RegexOptions.IgnoreCase)

let extractParametersAndOutputColumns(connectionString, commandText, resultType, allParametersOptional, dbSchemaLookups : DbSchemaLookups) =
    use conn = openConnection(connectionString)
    
    use cmd = new NpgsqlCommand(commandText, conn)
    NpgsqlCommandBuilder.DeriveParameters(cmd)
    for p in cmd.Parameters do p.Value <- DBNull.Value

    let resultSets =
        if resultType = ResultType.DataReader then
            []
        else
            use cursor = cmd.ExecuteReader CommandBehavior.SchemaOnly
            
            let resultSetSchemasFromNpgsql = [
                if cursor.FieldCount > 0 then
                    cursor.GetStatementIndex (), cursor.GetColumnSchema () |> Seq.toList

                    while cursor.NextResult () do
                        cursor.GetStatementIndex (), cursor.GetColumnSchema () |> Seq.toList
                ]

            [ 0 .. cursor.Statements.Count - 1 ]
            |> List.map (fun i ->
                let sql = cursor.Statements.[i].SQL
                match List.tryFind (fun (index, _) -> index = i) resultSetSchemasFromNpgsql with
                | Some (_, columns) ->
                    sql, columns |> List.map (fun column -> 
                        let columnAttributeNumber = column.ColumnAttributeNumber.GetValueOrDefault(-1s)
                        
                        let lookupKey = { TableOID = column.TableOID; ColumnAttributeNumber = columnAttributeNumber }

                        match dbSchemaLookups.Columns.TryGetValue lookupKey with
                        | true, col -> { col with Name = column.ColumnName }
                        | _ ->
                            {
                                ColumnAttributeNumber = columnAttributeNumber
                                Name = column.ColumnName
                                DataType = DataType.Create column.PostgresType
                                Nullable = column.AllowDBNull.GetValueOrDefault(true)
                                MaxLength = column.ColumnSize.GetValueOrDefault(-1)
                                ReadOnly = true
                                AutoIncrement = column.IsIdentity.GetValueOrDefault(false)
                                DefaultConstraint = column.DefaultValue
                                Description = ""
                                PartOfPrimaryKey = column.IsKey.GetValueOrDefault(false)
                                BaseSchemaName = column.BaseSchemaName
                                BaseTableName = column.BaseTableName
                            }) |> Query
                | _ ->
                    if controlCommandRegex.IsMatch sql then
                        sql, Control
                    else
                        sql, NonQuery)

    let parameters = 
        [ for p in cmd.Parameters ->
            assert (p.Direction = ParameterDirection.Input)
            let npgsqlDbType =
                match p.PostgresType with
                | :? PostgresArrayType as x when (x.Element :? PostgresEnumType) -> 
                    //probably array of custom type (enum or composite)
                    NpgsqlDbType.Array ||| NpgsqlDbType.Text
                | _ -> p.NpgsqlDbType
            { Name = p.ParameterName
              NpgsqlDbType = npgsqlDbType
              Direction = p.Direction
              MaxLength = p.Size
              Precision = p.Precision
              Scale = p.Scale
              Optional = allParametersOptional 
              DataType = DataType.Create(p.PostgresType) } ]

    parameters, resultSets

let getDbSchemaLookups(connectionString) =
    use conn = openConnection(connectionString)
    
    use cmd = conn.CreateCommand()
    cmd.CommandText <- """
        SELECT
          n.nspname              AS schema,
          t.typname              AS name,
          array_agg(e.enumlabel) AS values
        FROM pg_type t
          JOIN pg_enum e ON t.oid = e.enumtypid
          JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        GROUP BY
          schema, name
    """
    
    let enumsLookup =
        [
            use cursor = cmd.ExecuteReader()
            while cursor.Read() do
                let schema = cursor.GetString(0)
                let name = cursor.GetString(1)
                let values = cursor.GetValue(2) :?> _
                yield { Schema = schema; Name = name; Values = values }
        ]
        |> Seq.groupBy (fun e -> e.Schema)
        |> Seq.map (fun (schema, types) -> schema, types |> Seq.map (fun e -> e.Name, e) |> Map.ofSeq)
        |> Map.ofSeq
    
    //https://stackoverflow.com/questions/12445608/psql-list-all-tables#12455382
    use cmd = conn.CreateCommand()
    cmd.CommandText <- """
        SELECT
             ns.OID AS schema_oid,
             ns.nspname AS schema_name,
             attr.attrelid AS table_oid,
             cls.relname AS table_name,
             pg_catalog.obj_description(attr.attrelid) AS table_description,
             attr.attnum AS col_number,
             attr.attname AS col_name,
             coalesce(col.udt_name, typ.typname) AS col_udt_name,
             typ_ns.nspname AS col_data_type_ns,
             col.data_type AS col_data_type,
             attr.attnotnull AS col_not_null,
             col.character_maximum_length AS col_max_length,
             CASE WHEN col.is_updatable = 'YES' AND col.is_generated <> 'ALWAYS' THEN false ELSE true END AS col_is_readonly,
             CASE WHEN col.is_identity = 'YES' THEN true else false END AS col_is_identity,
             CASE WHEN attr.atthasdef THEN (SELECT pg_get_expr(adbin, cls.oid) FROM pg_attrdef WHERE adrelid = cls.oid AND adnum = attr.attnum) ELSE NULL END AS col_default,
             pg_catalog.col_description(attr.attrelid, attr.attnum) AS col_description,
             typ.oid AS col_typoid,
             EXISTS (
               SELECT * FROM pg_index
               WHERE pg_index.indrelid = cls.oid AND
                     pg_index.indisprimary AND
                     attnum = ANY (indkey)
             ) AS col_part_of_primary_key

        FROM pg_class AS cls
        INNER JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace

        LEFT JOIN pg_attribute AS attr ON attr.attrelid = cls.oid AND attr.atttypid <> 0 AND attr.attnum > 0 AND NOT attr.attisdropped
        LEFT JOIN pg_type AS typ ON typ.oid = attr.atttypid
        LEFT JOIN pg_namespace AS typ_ns ON typ_ns.oid = typ.typnamespace
        
        LEFT JOIN information_schema.columns AS col ON col.table_schema = ns.nspname AND
           col.table_name = relname AND
           col.column_name = attname
        WHERE
           cls.relkind IN ('r', 'v', 'm', 'p') AND
           ns.nspname !~ '^pg_' AND
           ns.nspname <> 'information_schema'
        ORDER BY ns.nspname, relname;
    """
    
    let schemas = Dictionary<string, DbSchemaLookupItem>()
    let columns = Dictionary<ColumnLookupKey, Column>()

    use row = cmd.ExecuteReader()
    while row.Read() do
        let schema = { OID = row.["schema_oid"] :?> _; Name = row.["schema_name"] :?> _ }
        
        let schemaItem =
            match schemas.TryGetValue schema.Name with
            | true, item -> item
            | _ ->
                let item = { Schema = schema; Tables = Dictionary(); Enums = enumsLookup.TryFind schema.Name |> Option.defaultValue Map.empty }
                schemas.[schema.Name] <- item
                item
        
        match row.GetOptionalValue("table_oid") with
        | None -> ()
        | Some oid ->
            let table = { OID = oid :?> _; Name = row.["table_name"] :?> _; Description = row.["table_description"] |> Option.ofObj |> Option.map string }
            
            let tableColumns =
                match schemaItem.Tables.TryGetValue table with
                | true, hashSet -> hashSet
                | _ ->
                    let hashSet = HashSet ()
                    schemaItem.Tables.[table] <- hashSet
                    hashSet
            
            match row.GetValueOrDefault("col_number", -1s) with
            | -1s -> ()
            | attnum ->
                let udtName = row.["col_udt_name"] :?> _
                // column data type namespace is not the same as table schema.
                let typeSchema = row.["col_data_type_ns"] :?> _
                let isUdt = schemas.ContainsKey(typeSchema) && schemas.[typeSchema].Enums.ContainsKey(udtName)

                let clrType =
                    match string row.["col_data_type"] with
                    | "ARRAY" ->
                        let elemType = getTypeMapping(udtName.TrimStart('_') ) 
                        elemType.MakeArrayType()
                    | "" when udtName.StartsWith "_" -> // possibly a column of a materialized view
                        let elemType = getTypeMapping(udtName.TrimStart('_') ) 
                        elemType.MakeArrayType()
                    | "USER-DEFINED" ->
                        if isUdt then typeof<string> else typeof<obj>
                    | "" -> // possibly a column of a materialized view
                        if isUdt then typeof<string> else getTypeMapping(udtName)
                    | dataType -> 
                        getTypeMapping(dataType)
                
                let column =
                    { ColumnAttributeNumber = attnum
                      Name = row.["col_name"] :?> _
                      DataType = { Name = udtName; Schema = typeSchema; ClrType = clrType }
                      Nullable = row.["col_not_null"] :?> _ |> not
                      MaxLength = row.GetValueOrDefault("col_max_length", -1)
                      ReadOnly = row.["col_is_readonly"] :?> _
                      AutoIncrement = row.["col_is_identity"] :?> _
                      DefaultConstraint = row.GetValueOrDefault("col_default", "")
                      Description = row.GetValueOrDefault("col_description", "")
                      PartOfPrimaryKey = row.["col_part_of_primary_key"] :?> _
                      BaseSchemaName = schema.Name
                      BaseTableName = row.GetValueOrDefault("table_name", "") }
                 
                tableColumns.Add column |> ignore
                
                let lookupKey = { TableOID = table.OID; ColumnAttributeNumber = column.ColumnAttributeNumber }
                if columns.ContainsKey lookupKey |> not then
                    columns.[lookupKey] <- column

    { Schemas = schemas; Columns = columns }
