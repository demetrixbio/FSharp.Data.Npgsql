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

type internal NpgsqlDataReader with

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
        if exists then value else failwithf "Unsupported datatype %s." datatype 

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
    
type UDT =
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

    member this.MakeProvidedType(customTypes : Map<string, ProvidedTypeDefinition>, ?forceNullability: bool) =
        let nullable = defaultArg forceNullability this.Nullable
        if this.DataType.IsUserDefinedType && customTypes.ContainsKey(this.DataType.UdtTypeName) then 
            let providedType = customTypes.[this.DataType.UdtTypeName]
            let t = if this.DataType.ClrType.IsArray then providedType.MakeArrayType() else upcast providedType
            if nullable then ProvidedTypeBuilder.MakeGenericType(typedefof<_ option>, [ t ]) else t
        else
            if nullable then typedefof<_ option>.MakeGenericType this.ClrType else this.ClrType

    member this.ToDataColumnExpr() =
        let typeName = 
            let clrType = if this.ClrType.IsArray then typeof<Array> else this.ClrType
            clrType.PartiallyQualifiedName
        
        let isTimestampTz = this.DataType.Name = "timestamptz" && this.ClrType = typeof<DateTime>
        let isTimestamp = this.DataType.Name = "timestamp" && this.ClrType = typeof<DateTime>
        let isJson = this.DataType.Name = "json"
        let isJsonb = this.DataType.Name = "jsonb"
        let isEnum = (not this.ClrType.IsArray) && this.DataType.IsUserDefinedType
        
        <@@
            let x = new DataColumn( %%Expr.Value(this.Name), Type.GetType(typeName, throwOnError = true))

            x.AutoIncrement <- %%Expr.Value(this.AutoIncrement)
            x.AllowDBNull <- %%Expr.Value(this.Nullable || this.HasDefaultConstraint)
            x.ReadOnly <- %%Expr.Value(this.ReadOnly)
            
            if x.DataType = typeof<string> then x.MaxLength <- %%Expr.Value(this.MaxLength)
            
            // control flow must be specified via simple bool switches as we are inside of quotation expression.
            // Expr.Value can contain only boxed primitive types (not variables)
            if isTimestampTz then
                //https://github.com/npgsql/npgsql/issues/1076#issuecomment-355400785
                x.DateTimeMode <- DataSetDateTime.Local
                //https://www.npgsql.org/doc/types/datetime.html#detailed-behavior-sending-values-to-the-database
                x.ExtendedProperties.Add(%%Expr.Value(box SchemaTableColumn.ProviderType), %%Expr.Value(box NpgsqlDbType.TimestampTz))
            elif isTimestamp then
                //https://www.npgsql.org/doc/types/datetime.html#detailed-behavior-sending-values-to-the-database
                x.ExtendedProperties.Add(%%Expr.Value(box SchemaTableColumn.ProviderType), %%Expr.Value(box NpgsqlDbType.Timestamp))
            elif isEnum then
                // value is an enum and should be sent to npgsql as unknown (auto conversion from string to appropriate enum type)
                x.ExtendedProperties.Add(%%Expr.Value(box SchemaTableColumn.ProviderType), %%Expr.Value(box NpgsqlDbType.Unknown))
            elif isJson then
                x.ExtendedProperties.Add(%%Expr.Value(box SchemaTableColumn.ProviderType), %%Expr.Value(box NpgsqlDbType.Json))
            elif isJsonb then
                x.ExtendedProperties.Add(%%Expr.Value(box SchemaTableColumn.ProviderType), %%Expr.Value(box NpgsqlDbType.Jsonb))
            
            x.ExtendedProperties.Add(%%Expr.Value(box SchemaTableColumn.IsKey), %%Expr.Value(box this.PartOfPrimaryKey))
            x.ExtendedProperties.Add(%%Expr.Value(box SchemaTableColumn.AllowDBNull), %%Expr.Value(box this.Nullable))
            x.ExtendedProperties.Add(%%Expr.Value(box "ClrType.PartiallyQualifiedName"), %%Expr.Value(box this.ClrType.PartiallyQualifiedName))
            x.ExtendedProperties.Add(%%Expr.Value(box SchemaTableColumn.BaseSchemaName), %%Expr.Value(box this.BaseSchemaName))
            x.ExtendedProperties.Add(%%Expr.Value(box SchemaTableColumn.BaseTableName), %%Expr.Value(box this.BaseTableName))
            x
        @@>
    
type DbSchemaLookupItem =
    { Schema : Schema
      Tables : Dictionary<Table, HashSet<Column>>
      Enums : Map<string, UDT> }
    
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
    with
   
    member this.Size = this.MaxLength
        //match this.TypeInfo.DbType with
        //| DbType.NChar | SqlDbType.NText | SqlDbType.NVarChar -> this.MaxLength / 2
        //| _ -> this.MaxLength

let inline openConnection connectionString =  
    let conn = new NpgsqlConnection(connectionString)
    conn.Open()
    conn

let extractParametersAndOutputColumns(connectionString, commandText, resultType, allParametersOptional, dbSchemaLookups : DbSchemaLookups) =
    use conn = openConnection(connectionString)
    
    use cmd = new NpgsqlCommand(commandText, conn)
    NpgsqlCommandBuilder.DeriveParameters(cmd)
    for p in cmd.Parameters do p.Value <- DBNull.Value

    let resultSets =
        if resultType = ResultType.DataReader then
            []
        else
            use cursor = cmd.ExecuteReader(CommandBehavior.SchemaOnly)
            
            let resultSetSchemasFromNpgsql = [
                if cursor.FieldCount = 0 then
                    // Command consists of a single non-query
                    yield 0, []
                else
                    yield cursor.GetStatementIndex(), [ for c in cursor.GetColumnSchema() -> c ]

                    while cursor.NextResult () do
                        yield cursor.GetStatementIndex(), [ for c in cursor.GetColumnSchema() -> c ]
                ]

            // Account for non-queries, which Npgsql neglects when there are multiple statements 
            [ 0 .. cursor.Statements.Count - 1 ]
            |> List.map (fun i ->
                match List.tryFind (fun (index, _) -> index = i) resultSetSchemasFromNpgsql with
                | Some (_, columns) -> columns
                | _ -> [])
    
    let outputColumns =
        if resultType <> ResultType.DataReader then
            resultSets |> List.map (List.map (fun column -> 
                let columnAttributeNumber = column.ColumnAttributeNumber.GetValueOrDefault(-1s)
                
                let lookupKey = { TableOID = column.TableOID; ColumnAttributeNumber = columnAttributeNumber }

                match dbSchemaLookups.Columns.TryGetValue lookupKey with
                | (true, col) ->
                    { col with Name = column.ColumnName }
                | _ ->
                    let dataType = DataType.Create(column.PostgresType)
                    {
                        ColumnAttributeNumber = columnAttributeNumber
                        Name = column.ColumnName
                        DataType = dataType
                        Nullable = column.AllowDBNull.GetValueOrDefault(true)
                        MaxLength = column.ColumnSize.GetValueOrDefault(-1)
                        ReadOnly = true
                        AutoIncrement = column.IsIdentity.GetValueOrDefault(false)
                        DefaultConstraint = column.DefaultValue
                        Description = ""
                        PartOfPrimaryKey = column.IsKey.GetValueOrDefault(false)
                        BaseSchemaName = column.BaseSchemaName
                        BaseTableName = column.BaseTableName
                    }))
        else
            [[]]

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
    
    let enums =  
        outputColumns 
        |> List.concat
        |> List.choose (fun c ->
            if c.DataType.IsUserDefinedType && dbSchemaLookups.Schemas.[c.DataType.Schema].Enums.ContainsKey(c.DataType.UdtTypeShortName) then
                Some dbSchemaLookups.Schemas.[c.DataType.Schema].Enums.[c.DataType.UdtTypeShortName]
            else
                None)
        |> List.append [ 
            for p in parameters do
                if p.DataType.IsUserDefinedType && dbSchemaLookups.Schemas.[p.DataType.Schema].Enums.ContainsKey(p.DataType.UdtTypeShortName) then
                    yield dbSchemaLookups.Schemas.[p.DataType.Schema].Enums.[p.DataType.UdtTypeShortName]
        ]
        |> List.distinct

    parameters, outputColumns, enums

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
                let values: string[] = cursor.GetValue(2) :?> _
                yield schema, name, { Schema = schema; Name = name; Values = values }
        ]
        |> Seq.groupBy (fun (schema, _, _) -> schema)
        |> Seq.map (fun (schema, types) ->
            schema, types |> Seq.map (fun (_, name, t) -> name, t) |> Map.ofSeq
        )
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
        let schema : Schema =
            { OID = row.["schema_oid"] :?> uint32
              Name = string row.["schema_name"] }
        
        if not <| schemas.ContainsKey(schema.Name) then
            
            schemas.Add(schema.Name, { Schema = schema
                                       Tables = Dictionary();
                                       Enums = enumsLookup.TryFind schema.Name |> Option.defaultValue Map.empty })
        
        match row.GetOptionalValue("table_oid") with
        | None -> ()
        | Some oid ->
            let table =
                { OID = oid :?> uint32
                  Name = string row.["table_name"]
                  Description = row.["table_description"] |> Option.ofObj |> Option.map string }
            
            if not <| schemas.[schema.Name].Tables.ContainsKey(table) then
                schemas.[schema.Name].Tables.Add(table, HashSet())
            
            match row.GetValueOrDefault("col_number", -1s) with
            | -1s -> ()
            | attnum ->
                let udtName = string row.["col_udt_name"]
                // column data type namespace is not the same as table schema.
                let typeSchema = string row.["col_data_type_ns"]
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
                      Name = string row.["col_name"]
                      DataType = { Name = udtName
                                   Schema = typeSchema
                                   ClrType = clrType }
                      Nullable = row.["col_not_null"] |> unbox |> not
                      MaxLength = row.GetValueOrDefault("col_max_length", -1)
                      ReadOnly = row.["col_is_readonly"] |> unbox
                      AutoIncrement = unbox row.["col_is_identity"]
                      DefaultConstraint = row.GetValueOrDefault("col_default", "")
                      Description = row.GetValueOrDefault("col_description", "")
                      PartOfPrimaryKey = unbox row.["col_part_of_primary_key"]
                      BaseSchemaName = schema.Name
                      BaseTableName = string row.["table_name"] }
                 
                if not <| schemas.[schema.Name].Tables.[table].Contains(column) then
                    schemas.[schema.Name].Tables.[table].Add(column) |> ignore
                
                let lookupKey = { TableOID = table.OID
                                  ColumnAttributeNumber = column.ColumnAttributeNumber }
                if not <| columns.ContainsKey(lookupKey) then
                    columns.Add(lookupKey, column)

    { Schemas = schemas
      Columns = columns }
