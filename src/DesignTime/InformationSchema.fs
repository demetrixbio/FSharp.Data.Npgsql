module internal FSharp.Data.Npgsql.DesignTime.InformationSchema 

open System.Data
open System
open System.Collections.Generic

open FSharp.Quotations

open NpgsqlTypes
open Npgsql
open Npgsql.PostgresTypes

open ProviderImplementation.ProvidedTypes
open System.Collections

type internal NpgsqlDataReader with

    member cursor.GetValueOrDefault(name: string, defaultValue) =    
        let i = cursor.GetOrdinal(name)
        if cursor.IsDBNull( i) then defaultValue else cursor.GetFieldValue( i)

let typesMapping = 
    Map.ofList [
        "bool", (typeof<bool>, NpgsqlDbType.Boolean)
        "int2", (typeof<int16>, NpgsqlDbType.Smallint)
        "int4", (typeof<int32>, NpgsqlDbType.Integer)
        "int8", (typeof<int64>, NpgsqlDbType.Bigint)
        "float4", (typeof<single>, NpgsqlDbType.Real)
        "float8", (typeof<double>, NpgsqlDbType.Double)
        "numeric", (typeof<decimal>, NpgsqlDbType.Numeric)
        "money", (typeof<decimal>, NpgsqlDbType.Money)
        "text", (typeof<string>, NpgsqlDbType.Text)
        "varchar", (typeof<string>, NpgsqlDbType.Varchar)
        "bpchar", (typeof<string>, NpgsqlDbType.Unknown)
        "citext", (typeof<string>, NpgsqlDbType.Citext)
        "jsonb", (typeof<string>, NpgsqlDbType.Jsonb)
        "json", (typeof<string>, NpgsqlDbType.Json)
        "xml", (typeof<string>, NpgsqlDbType.Xml)
        "point", (typeof<NpgsqlPoint>, NpgsqlDbType.Point)
        "lseg", (typeof<NpgsqlLSeg>, NpgsqlDbType.LSeg)
        "path",  (typeof<NpgsqlPath>, NpgsqlDbType.Path)
        "polygon", (typeof<NpgsqlPolygon>, NpgsqlDbType.Polygon)
        "line", (typeof<NpgsqlLine>, NpgsqlDbType.Line)
        "circle", (typeof<NpgsqlCircle>, NpgsqlDbType.Circle)
        "box", (typeof<NpgsqlBox>, NpgsqlDbType.Box)
        "bit", (typeof<BitArray>, NpgsqlDbType.Bit)
        "varbit", (typeof<BitArray>, NpgsqlDbType.Bit)
        "hstore", (typeof<IDictionary>, NpgsqlDbType.Hstore)
        "uuid", (typeof<Guid>, NpgsqlDbType.Uuid)
        "cidr", (typeof<NpgsqlInet>, NpgsqlDbType.Inet)        
        "inet", (typeof<NpgsqlInet>, NpgsqlDbType.Inet)
        "macaddr", (typeof<System.Net.NetworkInformation.PhysicalAddress>, NpgsqlDbType.MacAddr)
        "tsquery", (typeof<NpgsqlTsQuery>, NpgsqlDbType.TsQuery)
        "tsvector", (typeof<NpgsqlTsVector>, NpgsqlDbType.TsVector)

        "date", (typeof<DateTime>, NpgsqlDbType.Date)
        "interval", (typeof<TimeSpan>, NpgsqlDbType.Interval)
        "timestamp", (typeof<DateTime>, NpgsqlDbType.Timestamp)
        "timestamptz", (typeof<DateTime>, NpgsqlDbType.TimestampTz)
        "time", (typeof<TimeSpan>, NpgsqlDbType.Time)
        "timetz", (typeof<DateTimeOffset>, NpgsqlDbType.TimeTz)

        "bytea", (typeof<byte[]>, NpgsqlDbType.Bytea)
        "oid", (typeof<UInt32>, NpgsqlDbType.Oid)
        "xid", (typeof<UInt32>, NpgsqlDbType.Oid)
        "cid", (typeof<UInt32>, NpgsqlDbType.Cid)
        "oidvector",(typeof<UInt32[]>, NpgsqlDbType.Int2Vector)
        "name", (typeof<string>, NpgsqlDbType.Name)
        "char", (typeof<string>, NpgsqlDbType.Char)
        "geometry",(typeof<PostgisGeometry>, NpgsqlDbType.Geometry)        
        //"range", (typeof<NpgsqlRange>, NpgsqlDbType.Range)
    ]

type PostgresType with    
    member this.ToClrType() = 
        match this with
        | :? PostgresBaseType as x when typesMapping.ContainsKey(x.Name) -> 
            typesMapping |> Map.find x.Name |> fst
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
        then sprintf "%s.%s" this.Schema (this.Name.TrimStart('_'))
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
    Identity: bool
    DefaultConstraint: string
    Description: string
    UDT: Type option
}   with

    member this.ClrType = this.DataType.ClrType

    member this.ClrTypeConsideringNullability = 
        if this.Nullable
        then typedefof<_ option>.MakeGenericType this.DataType.ClrType
        else this.DataType.ClrType

    member this.HasDefaultConstraint = this.DefaultConstraint <> ""
    member this.OptionalForInsert = this.Nullable || this.HasDefaultConstraint

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
        let columnName = this.Name
        let typeName = this.ClrType.AssemblyQualifiedName.Split(',') |> Array.take 2 |> String.concat ","
        let allowDBNull = this.Nullable || this.HasDefaultConstraint
        let localDateTimeMode = this.DataType.Name = "timestamptz" && this.ClrType = typeof<DateTime>

        <@@ 
            let x = new DataColumn( columnName, Type.GetType( typeName, throwOnError = true))
            x.AllowDBNull <- allowDBNull
            if x.DataType = typeof<string>
            then 
                x.MaxLength <- %%Expr.Value(this.MaxLength)
            x.ReadOnly <- %%Expr.Value(this.ReadOnly)
            x.AutoIncrement <- %%Expr.Value(this.Identity)

            if localDateTimeMode
            then 
                x.DateTimeMode <- DataSetDateTime.Local

            x
        @@>
    
    override this.ToString() = 
        sprintf "%s\t%s\t%b\t%i\t%b\t%b" 
            this.Name 
            this.ClrType.FullName 
            this.Nullable 
            this.MaxLength
            this.ReadOnly
            this.Identity

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
        let nullable = not c.AllowDBNull.HasValue || c.AllowDBNull.Value
        let dataType = DataType.Create(c.PostgresType)

        { 
            Name = c.ColumnName
            DataType = dataType
            Nullable = nullable
            MaxLength = c.ColumnSize.GetValueOrDefault()
            ReadOnly = c.IsAutoIncrement.GetValueOrDefault() || c.IsReadOnly.GetValueOrDefault()
            Identity = c.IsIdentity.GetValueOrDefault()
            DefaultConstraint = c.DefaultValue
            Description = ""
            UDT = 
                match customTypes.Value.TryGetValue(dataType.UdtTypeName) with 
                | true, x ->
                    Some( if c.DataType.IsArray then x.MakeArrayType() else upcast x)
                | false, _ -> None 
        } 
    )
 

let getTables(connectionString, schema) = 
    use conn = openConnection(connectionString)
    use cmd = conn.CreateCommand()
    cmd.CommandText <- sprintf "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_type = 'BASE TABLE'" schema
    [ 
        use cursor = cmd.ExecuteReader()
        while cursor.Read() do
            let tableName = unbox cursor.[0]
            yield  tableName, tableName, schema, None
    ]