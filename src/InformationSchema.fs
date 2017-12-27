module internal FSharp.Data.InformationSchema 

open System.Data
open System
open NpgsqlTypes
open Npgsql
open ProviderImplementation.ProvidedTypes

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
    //tsquery	NpgsqlTsQuery		
    //tsvector	NpgsqlTsVector		

        "date", typeof<DateTime>, Some NpgsqlDbType.Date, Some DbType.Date
        "interval", typeof<TimeSpan>, Some NpgsqlDbType.Interval, None
        "timestamp", typeof<DateTime>, Some NpgsqlDbType.Timestamp, Some DbType.DateTime
        "timestamptz", typeof<DateTime>, Some NpgsqlDbType.TimestampTz, Some DbType.DateTime
        "time", typeof<TimeSpan>, Some NpgsqlDbType.Time, Some DbType.Time
        "timetz", typeof<DateTimeOffset>, Some NpgsqlDbType.TimeTz, Some DbType.DateTimeOffset

    //bytea	byte[]		
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

type DataType = {
    Name: string
    Schema: string
    ClrType: Type
}   with    
    member this.FullName = sprintf "%s.%s" this.Schema this.Name
        

type Column = {
    Name: string
    DataType: DataType
    Nullable: bool
    MaxLength: int
    ReadOnly: bool
    Identity: bool
    DefaultConstraint: string
    Description: string
    UDT: ProvidedTypeDefinition option
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
            if nullable then ProvidedTypeBuilder.MakeGenericType(typedefof<_ option>, [ t ]) else upcast t
        | None ->   
            if nullable
            then typedefof<_ option>.MakeGenericType this.ClrType
            else this.ClrType
    
    override this.ToString() = 
        sprintf "%s\t%s\t%b\t%i\t%b\t%b" 
            this.Name 
            this.ClrType.FullName 
            this.Nullable 
            this.MaxLength
            this.ReadOnly
            this.Identity

type TypeInfo = {
    DbType: DbType
    ClrType: Type
}   with
    member this.IsValueType = this.ClrType.IsValueType
    member this.IsFixedLength = this.IsValueType

type Parameter = {
    Name: string
    NpgsqlDbType: NpgsqlTypes.NpgsqlDbType
    ClrType: Type
    Direction: ParameterDirection 
    MaxLength: int
    Precision: byte
    Scale : byte
    Optional: bool
    DataTypeName: string 
}   with

    member this.IsUserDefinedType = not (this.DataTypeName.StartsWith( "pg_catalog."))
    member this.IsFixedLength = this.ClrType.IsValueType
    member this.Size = this.MaxLength
        //match this.TypeInfo.DbType with
        //| DbType.NChar | SqlDbType.NText | SqlDbType.NVarChar -> this.MaxLength / 2
        //| _ -> this.MaxLength

