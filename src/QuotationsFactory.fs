namespace FSharp.Data

open System
open System.Data
open System.Reflection
open Npgsql

open Microsoft.FSharp.Quotations

open FSharp.Data
open InformationSchema

[<CompilerMessageAttribute("This API supports the FSharp.Data.Npgsql infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
type QuotationsFactory private() = 
    
    static member internal GetBody(methodName, specialization, [<ParamArray>] bodyFactoryArgs : obj[]) =
        
        let bodyFactory =   
            let mi = typeof<QuotationsFactory>.GetMethod(methodName, BindingFlags.NonPublic ||| BindingFlags.Static)
            assert(mi <> null)
            mi.MakeGenericMethod([| specialization |])

        fun(args : Expr list) -> 
            let parameters = Array.append [| box args |] bodyFactoryArgs
            bodyFactory.Invoke(null, parameters) |> unbox

    static member internal ToSqlParam(p : Parameter) = 

        let name = p.Name
        let dbType = p.NpgsqlDbType
        let isFixedLength = p.DataType.IsFixedLength

        <@@ 
            let x = NpgsqlParameter(name, dbType, Direction = %%Expr.Value p.Direction)

            if not isFixedLength then x.Size <- %%Expr.Value p.Size 

            x.Precision <- %%Expr.Value p.Precision
            x.Scale <- %%Expr.Value p.Scale

            x
        @@>

    static member internal OptionToObj<'T> value = <@@ match %%value with Some (x : 'T) -> box x | None -> Extensions.DbNull @@>    
        
    static member internal MapArrayOptionItemToObj<'T>(arr, index) =
        <@
            let values : obj[] = %%arr
            values.[index] <- match unbox values.[index] with Some (x : 'T) -> box x | None -> null 
        @> 

    static member internal MapArrayObjItemToOption<'T>(arr, index) =
        <@
            let values : obj[] = %%arr
            values.[index] <- box <| if Convert.IsDBNull(values.[index]) then None else Some(unbox<'T> values.[index])
        @> 

    static member internal MapArrayNullableItems(outputColumns : Column list, mapper : string) = 
        let columnTypes, isNullableColumn = outputColumns |> List.map (fun c -> c.ClrType.FullName, c.Nullable) |> List.unzip

        QuotationsFactory.MapArrayNullableItems(columnTypes, isNullableColumn, mapper)            

    static member internal MapArrayNullableItems(columnTypes : string list, isNullableColumn : bool list, mapper : string) = 
        assert(columnTypes.Length = isNullableColumn.Length)
        let arr = Var("_", typeof<obj[]>)
        let body =
            (columnTypes, isNullableColumn) 
            ||> List.zip
            |> List.mapi(fun index (typeName, isNullableColumn) ->
                if isNullableColumn 
                then 
                    typeof<QuotationsFactory>
                        .GetMethod(mapper, BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod( Type.GetType( typeName, throwOnError = true))
                        .Invoke(null, [| box(Expr.Var arr); box index |])
                        |> unbox
                        |> Some
                else 
                    None
            ) 
            |> List.choose id
            |> List.fold (fun acc x ->
                Expr.Sequential(acc, x)
            ) <@@ () @@>
        Expr.Lambda(arr, body)

    static member internal GetNullableValueFromDataRow<'T>(exprArgs : Expr list, name : string) =
        <@
            let row : DataRow = %%exprArgs.[0]
            if row.IsNull name then None else Some(unbox<'T> row.[name])
        @> 

    static member internal SetNullableValueInDataRow<'T>(exprArgs : Expr list, name : string) =
        <@
            (%%exprArgs.[0] : DataRow).[name] <- match (%%exprArgs.[1] : option<'T>) with None -> DbNull | Some value -> box value
        @> 

    static member GetMapperWithNullsToOptions(nullsToOptions, mapper: obj[] -> obj) = 
        fun values -> 
            nullsToOptions values
            mapper values

    static member private GetNonNullableValueFromDataRow<'T>(exprArgs : Expr list, name: string) =
        <@ (%%exprArgs.[0] : DataRow).[name] @>

    static member private SetNonNullableValueInDataRow<'T>(exprArgs : Expr list, name : string) =
        <@ (%%exprArgs.[0] : DataRow).[name] <- %%Expr.Coerce(exprArgs.[1], typeof<obj>) @>
