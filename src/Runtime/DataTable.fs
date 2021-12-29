namespace FSharp.Data.Npgsql

open System.Data
open System.Collections.Generic
open System.ComponentModel
open Npgsql

///Enum describing output type
type ResultType =
    ///Sequence of custom records with properties matching column names and types
    | Records = 0
    ///Sequence of tuples matching column types with the same order
    | Tuples = 1
    ///Typed DataTable <see cref='T:FSharp.Data.DataTable`1'/>
    | DataTable = 2
    ///<summary>raw DataReader</summary>
    | DataReader = 3

/// Specifies the combination of `Execute`, `AsyncExecute` and `TaskAsyncExecute` methods to provide on commands.
/// Select only those that you need for the best design-time performance.
[<System.Flags>]
type MethodTypes =
    | Sync = 1
    | Async = 2
    | Task = 4

///Specifies the type of collection commands will return.
type CollectionType =
    | List = 0
    | Array = 1
    | ResizeArray = 2
    | LazySeq = 3

[<EditorBrowsable(EditorBrowsableState.Never); NoEquality; NoComparison>]
type ResultSetDefinition = {
    ErasedRowType: System.Type
    ExpectedColumns: DataColumn[] }

type LazySeq<'a> (s: 'a seq, reader: Common.DbDataReader, cmd: NpgsqlCommand) =
    member val Seq = s

    interface System.IDisposable with
        member _.Dispose () =
            reader.Dispose ()
            cmd.Dispose ()

[<Sealed>]
[<EditorBrowsable(EditorBrowsableState.Never)>]
type DataTable<'T when 'T :> DataRow>(selectCommand: NpgsqlCommand) = 
    inherit DataTable() 

    do assert (selectCommand <> null)

    let typedRows = 
        let rows = base.Rows 
        {
            new IList<'T> with
                member _.GetEnumerator() = rows.GetEnumerator()
                member _.GetEnumerator() : IEnumerator<'T> = (Seq.cast<'T> rows).GetEnumerator() 

                member _.Count = rows.Count
                member _.IsReadOnly = rows.IsReadOnly
                member _.Item 
                    with get index = downcast rows.[index]
                    and set index row = 
                        rows.RemoveAt(index)
                        rows.InsertAt(row, index)

                member _.Add row = rows.Add row
                member _.Clear() = rows.Clear()
                member _.Contains row = rows.Contains row
                member _.CopyTo(dest, index) = rows.CopyTo(dest, index)
                member _.IndexOf row = rows.IndexOf row
                member _.Insert(index, row) = rows.InsertAt(row, index)
                member _.Remove row = rows.Remove(row); true
                member _.RemoveAt index = rows.RemoveAt(index)
        }

    member _.Rows: IList<'T> = typedRows

    member internal _.SelectCommand = selectCommand

