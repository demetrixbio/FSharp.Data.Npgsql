[<AutoOpen>]
module FSharp.Data.Extensions

open System
open System.Data.Common

open Npgsql

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


    
       

