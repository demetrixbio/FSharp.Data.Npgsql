module internal FSharp.Data.Npgsql.DesignTime.InformationSchema 

open System
open System.Data
open System.Data.Common
open System.Collections.Generic

open FSharp.Quotations

open Npgsql
open Npgsql.PostgresTypes
open NpgsqlTypes

open ProviderImplementation.ProvidedTypes
open System.Collections
open System.Net

type internal NpgsqlDataReader with

    member cursor.GetValueOrDefault(name: string, defaultValue) =    
        let i = cursor.GetOrdinal(name)
        if cursor.IsDBNull( i) then defaultValue else cursor.GetFieldValue( i)

type internal Type with
    member this.PartiallyQualifiedName = 
        sprintf "%s, %s" this.FullName (this.Assembly.GetName().Name)

//https://www.postgresql.org/docs/current/static/datatype.html#DATATYPE-TABLE
let typesMapping = 
    Map.ofList [
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
        "geometry", typeof<LegacyPostgis.PostgisGeometry>
        //"range", typeof<NpgsqlRange>, NpgsqlDbType.Range)
    ]

type PostgresType with    
    member this.ToClrType() = 
        match this with
        | :? PostgresBaseType as x -> 
            typesMapping.[x.Name]
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

type Column = {
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
    BaseTableName: string

}   with

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
    
type Parameter = {
    Name: string
    NpgsqlDbType: NpgsqlTypes.NpgsqlDbType
    Direction: ParameterDirection 
    MaxLength: int
    Precision: byte
    Scale : byte
    Optional: bool
    DataType: DataType
}   with
   
    member this.Size = this.MaxLength
        //match this.TypeInfo.DbType with
        //| DbType.NChar | SqlDbType.NText | SqlDbType.NVarChar -> this.MaxLength / 2
        //| _ -> this.MaxLength

let inline openConnection connectionString =  
    let conn = new NpgsqlConnection(connectionString)
    conn.Open()
    conn

let extractParameters(connectionString, commandText, allParametersOptional) =  
    use conn = openConnection(connectionString)
    use cmd = new NpgsqlCommand(commandText, conn)
    NpgsqlCommandBuilder.DeriveParameters(cmd)

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

let getOutputColumns(connectionString, commandText, commandType, parameters: Parameter list, customTypes: ref<IDictionary<string, ProvidedTypeDefinition>>): Column list = 
    use conn = openConnection connectionString
        
    use cmd = new NpgsqlCommand(commandText, conn, CommandType = commandType)
    for p in parameters do
        cmd.Parameters.Add(p.Name, p.NpgsqlDbType).Value <- DBNull.Value
        
    let cols = 
        use cursor = cmd.ExecuteReader(CommandBehavior.KeyInfo ||| CommandBehavior.SchemaOnly)
        if cursor.FieldCount = 0 then [] else [ for c in cursor.GetColumnSchema() -> c ]

    let getEnums() =  
            
        let enumTypes = 
            cols 
            |> List.choose (fun c -> 
                if c.PostgresType :? PostgresTypes.PostgresEnumType
                then Some( c.PostgresType.FullName)
                else None
            )
            |> List.append [ 
                for p in parameters do 
                    if p.DataType.IsUserDefinedType 
                    then 
                        yield p.DataType.UdtTypeName 
            ]
            |> List.distinct

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

    cols
    |> List.map ( fun c -> 
        let dataType = DataType.Create(c.PostgresType)

        { 
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

let getTables(connectionString, schema) = 
    use conn = openConnection(connectionString)
    use cmd = conn.CreateCommand()
    cmd.CommandText <- sprintf "
        SELECT 
            table_name
            --,obj_description('myschema.mytable'::regclass)
        FROM 
            information_schema.tables 
        WHERE 
            table_schema = '%s' 
            AND table_type = 'BASE TABLE'
    " schema

    [ 
        use cursor = cmd.ExecuteReader()
        while cursor.Read() do
            let tableName = unbox cursor.[0]
            yield  tableName, None
    ]

let getTableColumns(connectionString, schema, tableName, customTypes: Map<_, ProvidedTypeDefinition list>) = 
    use conn = openConnection(connectionString)
    let cmd = conn.CreateCommand()
    cmd.CommandText <- sprintf """
        SELECT
            c.table_schema
            ,c.column_name
            ,c.data_type
            ,c.udt_name
            ,c.is_nullable
            ,c.character_maximum_length
            ,c.is_updatable
            ,c.is_identity
            ,c.column_default
            ,pgd.description
            ,constraint_column_usage.column_name IS NOT NULL AS part_of_primary_key
        FROM 
            information_schema.columns c
            LEFT JOIN pg_catalog.pg_statio_all_tables as st 
            ON c.table_schema = st.schemaname AND c.table_name = st.relname
            LEFT JOIN pg_catalog.pg_description pgd 
            ON pgd.objsubid = c.ordinal_position AND pgd.objoid = st.relid
            LEFT JOIN information_schema.table_constraints tc 
            ON c.table_schema = tc.table_schema AND c.table_name = tc.table_name AND tc.constraint_type = 'PRIMARY KEY'
            LEFT JOIN information_schema.constraint_column_usage USING (constraint_schema, constraint_name, column_name)
        WHERE 
            c.table_schema = '%s' 
            AND c.table_name = '%s' 
        ORDER BY 
            ordinal_position
    """ schema tableName

    [
        use row = cmd.ExecuteReader()
        while row.Read() do

            let udtName = string row.["udt_name"]
            let schema = unbox row.["table_schema"] 

            let udt =                             
                customTypes
                |> Map.tryFind schema
                |> Option.bind (List.tryFind (fun t -> t.Name = udtName))
                |> Option.map (fun x -> x :> Type)

            let values = [|  for i = 0 to row.FieldCount-1 do yield row.GetName(i), row.GetValue(i) |]

            yield {
                Name = unbox row.["column_name"]
                DataType = 
                    {
                        Name = udtName
                        Schema = schema
                        ClrType = 
                            match unbox row.["data_type"] with 
                            | "ARRAY" -> 
                                let elemType = typesMapping.[udtName.TrimStart('_')] 
                                elemType.MakeArrayType()
                            | "USER-DEFINED" ->
                                if udt.IsSome 
                                then 
                                    typeof<string> 
                                else 
                                    typeof<obj>
                            | dataType -> 
                                typesMapping.[dataType] 
                    }

                Nullable = unbox row.["is_nullable"] = "YES"
                MaxLength = row.GetValueOrDefault("character_maximum_length", -1)
                ReadOnly = unbox row.["is_updatable"] = "NO"
                AutoIncrement = unbox row.["is_identity"] = "YES"
                DefaultConstraint = row.GetValueOrDefault("column_default", "")
                Description = row.GetValueOrDefault("description", "")
                UDT = udt
                PartOfPrimaryKey = unbox row.["part_of_primary_key"]
                BaseSchemaName = schema
                BaseTableName = tableName
            }
    ]

