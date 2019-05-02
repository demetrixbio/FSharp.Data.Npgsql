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
open System.Collections.ObjectModel
open System.Net

type internal NpgsqlDataReader with

    member cursor.GetValueOrDefault(name: string, defaultValue) =    
        let i = cursor.GetOrdinal(name)
        if cursor.IsDBNull( i) then defaultValue else cursor.GetFieldValue( i)

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
    //"range", typeof<NpgsqlRange>, NpgsqlDbType.Range)
]

let mutable private spatialTypesMapping = [
    "geometry", typeof<LegacyPostgis.PostgisGeometry>
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
    member this.UdtTypeName = 
        if this.ClrType.IsArray 
        then
            let withoutTrailingBrackets = this.Name.Substring(0, this.Name.Length - 2) // my_enum[] -> my_enum
            sprintf "%s.%s" this.Schema withoutTrailingBrackets
        else this.FullName

    static member Create(x: PostgresTypes.PostgresType) = 
        { 
            Name = x.Name
            Schema = x.Namespace
            ClrType = x.ToClrType()
        }

type Schema =
    { OID : string
      Name : string }
    
type Table =
    { OID : string
      Name : string
      Description : string option }
    
type ColumnLookupKey = { TableOID : string; ColumnAttributeNumber : int16 }

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
      UDT: Type option
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

    member this.MakeProvidedType(?forceNullability: bool) = 
        let nullable = defaultArg forceNullability this.Nullable
        match this.UDT with
        | Some t -> 
            let t = if this.DataType.ClrType.IsArray then t.MakeArrayType() else t
            if nullable then ProvidedTypeBuilder.MakeGenericType(typedefof<_ option>, [ t ]) else t
        | None ->   
            if nullable
            then typedefof<_ option>.MakeGenericType this.ClrType
            else this.ClrType

    member this.ToDataColumnExpr() =
        let typeName = 
            let clrType = if this.ClrType.IsArray then typeof<Array> else this.ClrType
            clrType.PartiallyQualifiedName

        let localDateTimeMode = this.DataType.Name = "timestamptz" && this.ClrType = typeof<DateTime>
        let isEnum = this.UDT |> Option.exists (fun x -> not x.IsArray)

        <@@ 
            let x = new DataColumn( %%Expr.Value(this.Name), Type.GetType( typeName, throwOnError = true))

            x.AutoIncrement <- %%Expr.Value(this.AutoIncrement)
            x.AllowDBNull <- %%Expr.Value(this.Nullable || this.HasDefaultConstraint)
            x.ReadOnly <- %%Expr.Value(this.ReadOnly)

            if x.DataType = typeof<string> then x.MaxLength <- %%Expr.Value(this.MaxLength)
            if localDateTimeMode then x.DateTimeMode <- DataSetDateTime.Local

            x.ExtendedProperties.Add(%%Expr.Value(box SchemaTableColumn.IsKey), %%Expr.Value(box this.PartOfPrimaryKey))
            x.ExtendedProperties.Add(%%Expr.Value(box SchemaTableColumn.AllowDBNull), %%Expr.Value(box this.Nullable))
            x.ExtendedProperties.Add(%%Expr.Value(box SchemaTableColumn.BaseSchemaName), %%Expr.Value(box this.BaseSchemaName))
            x.ExtendedProperties.Add(%%Expr.Value(box SchemaTableColumn.BaseTableName), %%Expr.Value(box this.BaseTableName))
            if isEnum
            then
                x.ExtendedProperties.Add(%%Expr.Value(box SchemaTableColumn.ProviderType), %%Expr.Value(box NpgsqlDbType.Unknown))

            x
        @@>
    
type DbSchemaLookupItem =
    { Tables : Dictionary<Table, HashSet<Column>>
      Enums : Map<string, ProvidedTypeDefinition> }
    
type DbSchemaLookups =
    { Schemas : Dictionary<Schema, DbSchemaLookupItem>
      Columns : Dictionary<ColumnLookupKey, Column>
      Enums : Map<string, ProvidedTypeDefinition> }
    
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

let extractParametersAndOutputColumns(connectionString, commandText, resultType, allParametersOptional, customTypes: ref<IDictionary<string, ProvidedTypeDefinition>>) =  
    use conn = openConnection(connectionString)
    
    use cmd = new NpgsqlCommand(commandText, conn)
    let cols =
        if resultType <> ResultType.DataReader then
            NpgsqlCommandBuilder.DeriveInputParametersAndOutputTypes(cmd)
        else
            ReadOnlyCollection [||]

    let parameters = 
        [
            for p in cmd.Parameters do
                assert (p.Direction = ParameterDirection.Input)

                yield { 
                    Name = p.ParameterName
                    NpgsqlDbType = 
                        match p.PostgresType with
                        | :? PostgresArrayType as x when (x.Element :? PostgresEnumType) -> 
                            //probably array of custom type (enum or composite)
                            NpgsqlDbType.Array ||| NpgsqlDbType.Text
                        | _ -> p.NpgsqlDbType
                    Direction = p.Direction
                    MaxLength = p.Size
                    Precision = p.Precision
                    Scale = p.Scale
                    Optional = allParametersOptional 
                    DataType = DataType.Create(p.PostgresType)
                }
        ]
    
    let getEnums() =  
            
        let enumTypes = 
            cols 
            |> Seq.choose (fun c -> 
                if c.PostgresType :? PostgresTypes.PostgresEnumType
                then Some( c.PostgresType.FullName)
                else None
            )
            |> Seq.append [ 
                for p in parameters do 
                    if p.DataType.IsUserDefinedType 
                    then 
                        yield p.DataType.UdtTypeName 
            ]
            |> Seq.distinct
            |> List.ofSeq

        if enumTypes.IsEmpty
        then dict []
        else
            use getEnums = conn.CreateCommand()
            getEnums.CommandText <- 
                enumTypes
                |> List.map (fun x -> sprintf "enum_range(NULL::%s) AS \"%s\"" x x)
                |> String.concat ","
                |> sprintf "SELECT %s"

            [
                use cursor = getEnums.ExecuteReader()
                cursor.Read() |> ignore
                for i = 0 to cursor.FieldCount - 1 do 
                    let t = new ProvidedTypeDefinition(cursor.GetName(i), Some typeof<string>, hideObjectMethods = true, nonNullable = true)
                    let values = cursor.GetValue(i) :?> string[]
                    t.AddMembers [ for value in values -> ProvidedField.Literal(value, t, value) ]
                    yield t.Name, t 
            ]
            |> dict

    if customTypes.Value.Count = 0 
    then customTypes := getEnums()

    let outputColumns =
        cols
        |> Seq.map ( fun c -> 
            let dataType = DataType.Create(c.PostgresType)

            {
                ColumnAttributeNumber = c.ColumnAttributeNumber.GetValueOrDefault(-1s)
                Name = c.ColumnName
                DataType = dataType
                Nullable = c.AllowDBNull.GetValueOrDefault(true)
                MaxLength = c.ColumnSize.GetValueOrDefault(-1)
                ReadOnly = c.IsReadOnly.GetValueOrDefault(false)
                AutoIncrement = c.IsAutoIncrement.GetValueOrDefault(false)
                DefaultConstraint = c.DefaultValue
                Description = ""
                UDT = 
                    match customTypes.Value.TryGetValue(dataType.UdtTypeName) with 
                    | true, x ->
                        Some( if c.DataType.IsArray then x.MakeArrayType() else upcast x)
                    | false, _ -> None 
                PartOfPrimaryKey = c.IsKey.GetValueOrDefault(false)
                BaseSchemaName = c.BaseSchemaName
                BaseTableName = c.BaseTableName
            } 
        )
        |> List.ofSeq
        
    parameters, outputColumns
    
let getEnums connectionString = 
    use conn = openConnection(connectionString)
    use cmd = conn.CreateCommand()
    cmd.CommandText <- "
        SELECT
          n.nspname              AS schema,
          t.typname              AS name,
          array_agg(e.enumlabel) AS values
        FROM pg_type t
          JOIN pg_enum e ON t.oid = e.enumtypid
          JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        GROUP BY
          schema, name
    "
    seq {
        use cursor = cmd.ExecuteReader()
        while cursor.Read() do
            let schema = cursor.GetString(0)
            let name = cursor.GetString(1)
            let values: string[] = cursor.GetValue(2) :?> _
            let t = new ProvidedTypeDefinition(name, Some typeof<string>, hideObjectMethods = true, nonNullable = true)
            for value in values do
                t.AddMember( ProvidedField.Literal(value, t, value))

            //let valuesFieldType = ProvidedTypeBuilder.MakeGenericType(typedefof<_ list>, [ t ])
            //let valuesField = ProvidedField("Values", valuesFieldType) 
            //valuesField.SetFieldAttributes( FieldAttributes.Public ||| FieldAttributes.InitOnly ||| FieldAttributes.Static)
            //t.AddMember( valuesField)

            //let typeInit = 
            //    let valuesExpr = Expr.NewArray(typeof<string>, [ for v in values -> Expr.Value(v)])
            //    ProvidedConstructor(
            //        [], 
            //        invokeCode = (fun _ -> Expr.FieldSet(valuesField, Expr.Coerce(valuesExpr, valuesFieldType))),
            //        IsTypeInitializer = true
            //    )

            //t.AddMember typeInit 

            yield schema, name, t
    }
    |> Seq.groupBy (fun (schema, _, _) -> schema)
    |> Seq.map (fun (schema, types) ->
        schema, types |> Seq.map (fun (_, name, t) -> name, t) |> Map.ofSeq
    )
    |> Map.ofSeq

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
        seq {
            use cursor = cmd.ExecuteReader()
            while cursor.Read() do
                let schema = cursor.GetString(0)
                let name = cursor.GetString(1)
                let values: string[] = cursor.GetValue(2) :?> _
                let t = new ProvidedTypeDefinition(name, Some typeof<string>, hideObjectMethods = true, nonNullable = true)
                for value in values do
                    t.AddMember( ProvidedField.Literal(value, t, value))
                yield schema, name, t
        }
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
             col.udt_name AS col_udt_name,
             col.data_type AS col_data_type,
             attr.attnotnull AS col_not_null,
             col.character_maximum_length AS col_max_length,
             CASE WHEN col.is_updatable = 'YES' THEN true ELSE false END AS col_is_updatable,
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
        LEFT JOIN information_schema.columns AS col ON col.table_schema = ns.nspname AND
           col.table_name = relname AND
           col.column_name = attname
        WHERE
           cls.relkind IN ('r', 'v', 'm') AND
           ns.nspname !~ '^pg_' AND
           ns.nspname <> 'information_schema'
        ORDER BY nspname, relname;
    """
    
    let schemas = Dictionary<Schema, DbSchemaLookupItem>()
    let columns = Dictionary<ColumnLookupKey, Column>()
    
    use row = cmd.ExecuteReader()
    while row.Read() do
        let schema : Schema =
            { OID = unbox row.["schema_oid"]
              Name = unbox row.["schema_name"] }
        
        if not <| schemas.ContainsKey(schema) then
            
            schemas.Add(schema, { Tables = Dictionary();
                                  Enums = enumsLookup.TryFind schema.Name |> Option.defaultValue Map.empty })
        
        match row.["table_oid"] |> Option.ofObj with
        | None -> ()
        | Some oid ->
            let table =
                { OID = unbox oid
                  Name = unbox row.["table_name"]
                  Description = row.["table_description"] |> Option.ofObj |> Option.map string }
            
            if not <| schemas.[schema].Tables.ContainsKey(table) then
                schemas.[schema].Tables.Add(table, HashSet())
            
            match row.["col_number"] |> Option.ofObj with
            | None -> ()
            | Some attnum ->
                let udtName = string row.["col_udt_name"]
                let udt =
                    schemas.[schema].Enums
                    |> Map.tryFind udtName
                    |> Option.map (fun x -> x :> Type)
                
                let clrType =
                    match unbox row.["col_data_type"] with
                    | "ARRAY" ->
                        let elemType = getTypeMapping(udtName.TrimStart('_') ) 
                        elemType.MakeArrayType()
                    | "USER-DEFINED" ->
                        if udt.IsSome then typeof<string> else typeof<obj>
                    | dataType -> 
                        getTypeMapping(dataType)
                
                let column =
                    { ColumnAttributeNumber = unbox attnum
                      Name = unbox row.["col_name"]
                      DataType = { Name = udtName
                                   Schema = schema.Name
                                   ClrType = clrType }
                      Nullable = row.["col_not_null"] |> unbox |> not
                      MaxLength = row.GetValueOrDefault("col_max_length", -1)
                      ReadOnly = row.["col_is_updatable"] |> unbox |> not
                      AutoIncrement = unbox row.["col_is_identity"]
                      DefaultConstraint = row.GetValueOrDefault("col_default", "")
                      Description = row.GetValueOrDefault("col_description", "")
                      UDT = udt
                      PartOfPrimaryKey = unbox row.["col_part_of_primary_key"]
                      BaseSchemaName = schema.Name
                      BaseTableName = unbox row.["table_name"] }
                 
                if not <| schemas.[schema].Tables.[table].Contains(column) then
                    schemas.[schema].Tables.[table].Add(column) |> ignore
                
                let lookupKey = { TableOID = table.OID
                                  ColumnAttributeNumber = column.ColumnAttributeNumber }
                if not <| columns.ContainsKey(lookupKey) then
                    columns.Add(lookupKey, column)

    { Schemas = schemas
      Columns = columns
      Enums = enumsLookup
              |> Seq.map (fun s -> s.Value
                                   |> Seq.map (fun x -> x.Key, x.Value))
              |> Seq.concat
              |> Map.ofSeq }