[<AutoOpen>]
module FSharp.Data.Extensions

open System
open System.Data
open System.Data.Common

open Npgsql
open FSharp.Data.InformationSchema


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
    
       

