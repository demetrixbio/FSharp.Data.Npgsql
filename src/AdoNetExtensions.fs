[<AutoOpen>]
module FSharp.Data.Extensions

open System
open System.Data
open System.Data.Common

open Npgsql
open FSharp.Data.InformationSchema
open ProviderImplementation.ProvidedTypes
open System.Collections.Generic

let defaultCommandTimeout = (new NpgsqlCommand()).CommandTimeout

type internal DbDataReader with
    member this.MapRowValues<'TItem>( rowMapping) = 
        seq {
            use _ = this
            let values = Array.zeroCreate this.FieldCount
            while this.Read() do
                this.GetValues(values) |> ignore
                yield values |> rowMapping |> unbox<'TItem>
        }
    member this.TryGetValue name = 
        let i = this.GetOrdinal(name)
        if this.IsDBNull( i) then None else Some(this.GetFieldValue( i))
    member this.GetValueOrDefault(name: string, defaultValue) = 
        this.TryGetValue(name) |> Option.defaultValue defaultValue

let DbNull = box DBNull.Value


type NpgsqlConnection with

 //address an issue when regular Dispose on SqlConnection needed for async computation 
 //wipes out all properties like ConnectionString in addition to closing connection to db
    member this.UseLocally(?privateConnection) =
        if this.State = ConnectionState.Closed 
            && defaultArg privateConnection true
        then 
            this.Open()
            { new IDisposable with member __.Dispose() = this.Close() }
        else { new IDisposable with member __.Dispose() = () }
    
    member internal this.GetOutputColumns(commandText, commandType, parameters: InformationSchema.Parameter list, customTypes: ref<IDictionary<string, ProvidedTypeDefinition>>): InformationSchema.Column list = 
        assert (this.State = ConnectionState.Open)
        
        use cmd = new NpgsqlCommand(commandText, this, CommandType = commandType)
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
                use getEnums = this.CreateCommand()
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
       
    member internal this.GetTables( schema) = 
        use __ = this.UseLocally()
        use cmd = this.CreateCommand()
        cmd.CommandText <- sprintf "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_type = 'BASE TABLE'" schema
        [ 
            use cursor = cmd.ExecuteReader()
            while cursor.Read() do
                let tableName = unbox cursor.[0]
                yield  tableName, tableName, schema, None
        ]
