module internal FSharp.Data.InformationSchema 

open System.Data
open System
open System.Collections.Generic

open FSharp.Quotations

open NpgsqlTypes
open Npgsql
open Npgsql.PostgresTypes

open ProviderImplementation.ProvidedTypes

type internal NpgsqlDataReader with

    member this.GetValueOrDefault(name: string, defaultValue) =    
        let i = this.GetOrdinal(name)
        if this.IsDBNull( i) then defaultValue else this.GetFieldValue( i)

let typesMapping = 
    [
        "bool", typeof<bool>, Some NpgsqlDbType.Boolean, Some DbType.Boolean
        "int2", typeof<int16>, Some NpgsqlDbType.Smallint, Some DbType.Int16
        "int4", typeof<int32>, Some NpgsqlDbType.Integer, Some DbType.Int32
        "int8", typeof<int64>, Some NpgsqlDbType.Bigint, Some DbType.Int64
        "float4", typeof<single>, Some NpgsqlDbType.Real, Some DbType.Single
        "float8", typeof<double>, Some NpgsqlDbType.Double, Some DbType.Double
        "numeric", typeof<decimal>, Some NpgsqlDbType.Numeric, Some DbType.Decimal
        "money", typeof<decimal>, Some NpgsqlDbType.Money, Some DbType.Currency
        "text", typeof<string>, Some NpgsqlDbType.Text, Some DbType.String
        "varchar", typeof<string>, Some NpgsqlDbType.Varchar, None
        "bpchar", typeof<string>, None, None
        "citext", typeof<string>, Some NpgsqlDbType.Citext, None
        "jsonb", typeof<string>, Some NpgsqlDbType.Jsonb, None
        "json", typeof<string>, Some NpgsqlDbType.Json, None
        "xml", typeof<string>, Some NpgsqlDbType.Xml, None
        "point", typeof<NpgsqlPoint>, Some NpgsqlDbType.Point, None
        "lseg", typeof<NpgsqlLSeg>, Some NpgsqlDbType.LSeg, None
    //path	NpgsqlPath		
    //polygon	NpgsqlPolygon		
    //line	NpgsqlLine		string
    //circle	NpgsqlCircle		string
    //box	NpgsqlBox		string
        //bit(1)	bool		BitArray
    //bit(n)	BitArray		
    //varbit	BitArray		
    //hstore	IDictionary		string
        "uuid", typeof<Guid>, Some NpgsqlDbType.Uuid, None
    //cidr	NpgsqlInet		string
        "inet", typeof<NpgsqlInet>, Some NpgsqlDbType.Inet, None
    //macaddr	PhysicalAddress		string
        "tsquery",  typeof<NpgsqlTsQuery>, Some NpgsqlDbType.TsQuery, None
        "tsvector", typeof<NpgsqlTsVector>, Some NpgsqlDbType.TsVector, None

        "date", typeof<DateTime>, Some NpgsqlDbType.Date, Some DbType.Date
        "interval", typeof<TimeSpan>, Some NpgsqlDbType.Interval, None
        "timestamp", typeof<DateTime>, Some NpgsqlDbType.Timestamp, Some DbType.DateTime
        "timestamptz", typeof<DateTime>, Some NpgsqlDbType.TimestampTz, Some DbType.DateTime
        "time", typeof<TimeSpan>, Some NpgsqlDbType.Time, Some DbType.Time
        "timetz", typeof<DateTimeOffset>, Some NpgsqlDbType.TimeTz, Some DbType.DateTimeOffset

        "bytea", typeof<byte[]>, Some NpgsqlDbType.Bytea, Some DbType.Binary
        "oid", typeof<UInt32>, Some NpgsqlDbType.Oid, Some DbType.UInt32
        "xid", typeof<UInt32>, Some NpgsqlDbType.Oid, Some DbType.UInt32
    //cid	uint		
    //oidvector	uint[]		
        "name", typeof<string>, Some NpgsqlDbType.Name, Some DbType.String
        "char", typeof<string>, Some NpgsqlDbType.Char, Some DbType.String
    //geometry (PostGIS)	PostgisGeometry		
    //record	object[]		
    //composite types	T		
    //range subtypes	NpgsqlRange		
    //enum types	TEnum		
    //array types	Array (of child element type)		
    ]

let postresTypeToClrType = 
    typesMapping 
    |> List.map (fun (postgres, clr, _, _) -> postgres, clr) 
    |> dict

let npgsqlDbTypeToClrType = 
    typesMapping 
    |> List.choose (fun (_, clr, maybeNpgsqlDbType, _) -> maybeNpgsqlDbType |> Option.map (fun t -> t, clr)) 
    |> dict

type PostgresType with    
    member this.ToClrType() = 
        match this with
        | :? PostgresBaseType as x when postresTypeToClrType.ContainsKey(x.Name) -> 
            postresTypeToClrType.[x.Name]
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

    member this.GetProvidedType(?forceNullability: bool) = 
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
        let typeName = this.ClrType.FullName
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

let inline asyncOpenConnection connectionString =  
    async {
        let conn = new NpgsqlConnection(connectionString)
        do! conn.OpenAsync() |> Async.AwaitTask
        return conn
    }

let extractParameters(connectionString, commandText: string, allParametersOptional) =  
    use conn = openConnection(connectionString)
    use cmd = new NpgsqlCommand(commandText, conn)
    NpgsqlCommandBuilder.DeriveParameters(cmd)

    [
        for p in cmd.Parameters do
            assert (p.Direction = ParameterDirection.Input)

            yield { 
                Name = p.ParameterName
                NpgsqlDbType = 
                    //if p.NpgsqlDbType.HasFlag( NpgsqlDbType.Enum) then NpgsqlDbType.Unknown else p.NpgsqlDbType
                    match p.NpgsqlDbType with 
                    | NpgsqlDbType.Text when p.PostgresType.GetType() = typeof<PostgresEnumType> -> NpgsqlDbType.Unknown 
                    | as_is -> as_is
                    
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
                if typeof<PostgresTypes.PostgresEnumType> = c.PostgresType.GetType()
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