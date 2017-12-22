[<CompilerMessageAttribute("This API supports the FSharp.Data.Npgsql infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
module FSharp.Data.DbCommand

open System.Data
open System.Data.Common

let execute (cmd: DbCommand) (paramValues: obj[]) = 
    seq {
        let mutable ownConnection = false
        try
            cmd.Parameters |> Seq.cast<DbParameter> |> Seq.iter2 (fun p v -> p.Value <- v) <| paramValues

            if cmd.Connection.State = ConnectionState.Closed 
            then 
                cmd.Connection.Open()
                ownConnection <- true

            use cursor = cmd.ExecuteReader()
            let columns =                 
                if cursor.HasRows  
                then [| for i = 0 to cursor.VisibleFieldCount - 1 do yield cursor.GetName(i) |] 
                else Array.empty
            let rowValues = Array.zeroCreate cursor.VisibleFieldCount
            while cursor.Read() do
                cursor.GetValues( rowValues) |> ignore
                yield (columns, rowValues) ||> Array.zip |> Map.ofArray
        finally 
            if ownConnection 
            then 
                cmd.Connection.Close()
    }

//let mapRowValues<'TItem> rowMapping (cursor: DbDataReader) = 
//    seq {
//        try
//            let values = Array.zeroCreate cursor.FieldCount
//            while cursor.Read() do
//                cursor.GetValues(values) |> ignore
//                yield values |> rowMapping |> unbox<'TItem>
//        finally
//            cursor.Close()
//    }

