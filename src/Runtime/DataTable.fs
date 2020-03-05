namespace FSharp.Data.Npgsql

open System.Data
open System.Collections.Generic
open System.ComponentModel
open Npgsql

///<summary>Enum describing output type</summary>
type ResultType =
    ///<summary>Sequence of custom records with properties matching column names and types</summary>
    | Records = 0
    ///<summary>Sequence of tuples matching column types with the same order</summary>
    | Tuples = 1
    ///<summary>Typed DataTable <see cref='T:FSharp.Data.DataTable`1'/></summary>
    | DataTable = 2
    ///<summary>raw DataReader</summary>
    | DataReader = 3

type ConfigType =
    | JsonFile = 1
    | Environment = 2
    | UserStore = 3
    
[<EditorBrowsable(EditorBrowsableState.Never)>]
type ResultSetDefinition = {
    //Row2ItemMapping: obj[] -> obj
    SeqItemTypeName: string
    ExpectedColumns: DataColumn[]
}

[<Sealed>]
[<EditorBrowsable(EditorBrowsableState.Never)>]
type DataTable<'T when 'T :> DataRow>(selectCommand: NpgsqlCommand) = 
    inherit DataTable() 

    do assert (selectCommand <> null)

    let typedRows = 
        let rows = base.Rows 
        {
            new IList<'T> with
                member __.GetEnumerator() = rows.GetEnumerator()
                member __.GetEnumerator() : IEnumerator<'T> = (Seq.cast<'T> rows).GetEnumerator() 

                member __.Count = rows.Count
                member __.IsReadOnly = rows.IsReadOnly
                member __.Item 
                    with get index = downcast rows.[index]
                    and set index row = 
                        rows.RemoveAt(index)
                        rows.InsertAt(row, index)

                member __.Add row = rows.Add row
                member __.Clear() = rows.Clear()
                member __.Contains row = rows.Contains row
                member __.CopyTo(dest, index) = rows.CopyTo(dest, index)
                member __.IndexOf row = rows.IndexOf row
                member __.Insert(index, row) = rows.InsertAt(row, index)
                member __.Remove row = rows.Remove(row); true
                member __.RemoveAt index = rows.RemoveAt(index)
        }

    member __.Rows: IList<'T> = typedRows

    member internal __.SelectCommand = selectCommand

