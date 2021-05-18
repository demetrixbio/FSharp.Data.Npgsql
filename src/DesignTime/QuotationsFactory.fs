namespace FSharp.Data.Npgsql.DesignTime

open System
open System.Data
open System.Reflection
open FSharp.Quotations
open ProviderImplementation.ProvidedTypes
open Npgsql
open FSharp.Data.Npgsql
open InformationSchema
open System.Collections.Concurrent
open System.Threading.Tasks

type internal ReturnType = {
    Single: Type
    RowProvidedType: Type option
}

type internal Statement = {
    Type: StatementType
    ReturnType: ReturnType option
    Sql: string
}

type internal ProvidedTypeReuse =
    | WithCache of ConcurrentDictionary<string, ProvidedTypeDefinition>
    | NoReuse

type internal QuotationsFactory () = 
    static let (|Arg3|) xs = 
        assert (List.length xs = 3)
        Arg3(xs.[0], xs.[1], xs.[2])

    static let (|Arg6|) xs = 
        assert (List.length xs = 6)
        Arg6(xs.[0], xs.[1], xs.[2], xs.[3], xs.[4], xs.[5])

    static let (|Arg7|) xs = 
        assert (List.length xs = 7)
        Arg7(xs.[0], xs.[1], xs.[2], xs.[3], xs.[4], xs.[5], xs.[6])

    static member val GetValueAtIndexExpr: (Expr * int) -> Expr =
        let mi = typeof<Unit>.Assembly.GetType("Microsoft.FSharp.Core.LanguagePrimitives+IntrinsicFunctions").GetMethod("GetArray").MakeGenericMethod typeof<obj>
        fun (arrayExpr, index) -> Expr.Call (mi, [ arrayExpr; Expr.Value index ])

    static member val ToSqlParamsExpr =
        let mi = typeof<Utils>.GetMethod (nameof Utils.ToSqlParam, BindingFlags.Static ||| BindingFlags.Public)
        let miEmpty = typeof<Array>.GetMethod(nameof Array.Empty, BindingFlags.Static ||| BindingFlags.Public).MakeGenericMethod typeof<NpgsqlParameter>
        fun (ps: Parameter list) ->
            if ps.IsEmpty then
                Expr.Call (miEmpty, [])
            else
                Expr.NewArray (typeof<NpgsqlParameter>, ps |> List.map (fun p ->
                    Expr.Call (mi,
                        [ Expr.Value p.Name; Expr.Value p.NpgsqlDbType; Expr.Value (if p.DataType.IsFixedLength then 0 else p.MaxLength); Expr.Value p.Scale; Expr.Value p.Precision ])))

    static member val ParamArrayEmptyExpr =
        let mi = typeof<Array>.GetMethod(nameof Array.Empty, BindingFlags.Static ||| BindingFlags.Public).MakeGenericMethod typeof<string * obj>
        Expr.Call (mi, [])

    static member val DataColumnArrayEmptyExpr =
        let mi = typeof<Array>.GetMethod(nameof Array.Empty, BindingFlags.Static ||| BindingFlags.Public).MakeGenericMethod typeof<DataColumn>
        Expr.Call (mi, [])

    static member GetNullableValueFromDataRow (t: Type, name: string) (exprArgs: Expr list) =
        Expr.Call (typeof<Utils>.GetMethod(nameof Utils.GetNullableValueFromDataRow).MakeGenericMethod t, [
            exprArgs.[0]
            Expr.Value name ])

    static member SetNullableValueInDataRow (t: Type, name: string) (exprArgs: Expr list) =
        Expr.Call (typeof<Utils>.GetMethod(nameof Utils.SetNullableValueInDataRow).MakeGenericMethod t, [
            exprArgs.[0]
            Expr.Value name
            Expr.Coerce (exprArgs.[1], typeof<obj>) ])

    static member GetNonNullableValueFromDataRow (name: string) (exprArgs: Expr list) =
        Expr.Call (exprArgs.Head, typeof<DataRow>.GetMethod ("get_Item", [| typeof<string> |]), [ Expr.Value name ])

    static member SetNonNullableValueInDataRow (name: string) (exprArgs: Expr list) =
        Expr.Call (exprArgs.Head, typeof<DataRow>.GetMethod ("set_Item", [| typeof<string>; typeof<obj> |]), [ Expr.Value name; Expr.Coerce (exprArgs.[1], typeof<obj>) ])
    
    static member GetMapperFromOptionToObj (t: Type, value: Expr) =
        Expr.Call (typeof<Utils>.GetMethod(nameof Utils.OptionToObj).MakeGenericMethod t, [ Expr.Coerce (value, typeof<obj>) ])

    static member AddGeneratedMethod (sqlParameters: Parameter list, executeArgs: ProvidedParameter list, erasedType, providedOutputType, name) =
        let mappedInputParamValues (exprArgs: Expr list) = 
            (exprArgs.Tail, sqlParameters)
            ||> List.map2 (fun expr param ->
                let value = 
                    if param.Direction = ParameterDirection.Input then 
                        if param.Optional then 
                            QuotationsFactory.GetMapperFromOptionToObj(param.DataType.ClrType, expr)
                        else
                            expr
                    else
                        let t = param.DataType.ClrType

                        if t.IsArray then
                            Expr.Value(Array.CreateInstance(t.GetElementType(), param.MaxLength))
                        else
                            Expr.Value(Activator.CreateInstance(t), t)

                Expr.NewTuple [ Expr.Value param.Name; Expr.Coerce (value, typeof<obj>) ])

        let invokeCode exprArgs =
            let vals = mappedInputParamValues exprArgs
            let paramValues = if vals.IsEmpty then QuotationsFactory.ParamArrayEmptyExpr else Expr.NewArray (typeof<string * obj>, vals)
            Expr.Call (Expr.Coerce (exprArgs.[0], erasedType), typeof<ISqlCommand>.GetMethod name, [ paramValues ])    

        ProvidedMethod(name, executeArgs, providedOutputType, invokeCode)

    static member GetRecordType (rootTypeName, columns: Column list, customTypes: Map<string, ProvidedTypeDefinition>, typeNameSuffix, providedTypeReuse) =
        columns 
        |> List.groupBy (fun x -> x.Name)
        |> List.iter (fun (name, xs) ->
            if not xs.Tail.IsEmpty then
                failwithf "Non-unique column name %s is not supported for ResultType.Records." name
            if String.IsNullOrEmpty name then
                failwithf "One or more columns do not have a name. Please give the columns an explicit alias.")
        
        let createType typeName =
            let baseType = ProvidedTypeBuilder.MakeTupleType (columns |> List.sortBy (fun x -> x.Name) |> List.map (fun x -> x.MakeProvidedType customTypes))
            let recordType = ProvidedTypeDefinition (typeName, baseType = Some baseType, hideObjectMethods = true)

            columns
            |> List.sortBy (fun x -> x.Name)
            |> List.iteri (fun i col ->
                let rec accessor instance (baseType: Type) tupleIndex coerce =
                    if tupleIndex < 7 then
                        Expr.PropertyGet (
                            (if coerce then Expr.Coerce (instance, baseType) else instance),
                            baseType.GetProperty (sprintf "Item%d" (tupleIndex + 1))
                        )
                    else
                        let constituentTuple = baseType.GetGenericArguments().[7]
                        let rest =
                            Expr.PropertyGet (
                                (if coerce then Expr.Coerce (instance, baseType) else instance),
                                baseType.GetProperty "Rest")
                        accessor rest constituentTuple (tupleIndex - 7) false

                ProvidedProperty (col.Name, col.MakeProvidedType customTypes, fun args -> accessor args.[0] baseType i true) |> recordType.AddMember)

            recordType

        match providedTypeReuse with
        | WithCache cache ->
            let typeName = columns |> List.map (fun x ->
                let t = if Map.containsKey x.DataType.FullName customTypes then x.DataType.FullName else x.ClrType.Name
                if x.Nullable then sprintf "%s:Option<%s>" x.Name t else sprintf "%s:%s" x.Name t) |> List.sort |> String.concat ", "

            cache.GetOrAdd (rootTypeName + typeName, fun _ -> createType typeName)
        | NoReuse ->
            createType ("Record" + typeNameSuffix)

    static member GetDataRowPropertyGetterAndSetterCode (column: Column) =
        let name = column.Name
        if column.Nullable then
            let setter = if column.ReadOnly then None else Some (QuotationsFactory.SetNullableValueInDataRow (column.ClrType, name))
            QuotationsFactory.GetNullableValueFromDataRow (column.ClrType, name), setter
        else
            let setter = if column.ReadOnly then None else Some (QuotationsFactory.SetNonNullableValueInDataRow name)
            QuotationsFactory.GetNonNullableValueFromDataRow name, setter

    static member GetDataRowType (customTypes: Map<string, ProvidedTypeDefinition>, columns: Column list) = 
        let rowType = ProvidedTypeDefinition("Row", Some typeof<DataRow>)
            
        columns 
        |> List.mapi(fun i col ->

            if col.Name = "" then failwithf "Column #%i doesn't have a name. Please use an explicit alias." (i + 1)

            let propertyType = col.MakeProvidedType(customTypes)

            let getter, setter = QuotationsFactory.GetDataRowPropertyGetterAndSetterCode col

            let p = ProvidedProperty(col.Name, propertyType, getter, ?setterCode = setter)

            if col.Description <> "" then p.AddXmlDoc col.Description

            p
        )
        |> rowType.AddMembers

        rowType

    static member GetDataTableType
        (
            typeName, 
            dataRowType: ProvidedTypeDefinition,
            customTypes: Map<string, ProvidedTypeDefinition>,
            outputColumns: Column list
        ) =

        let tableType = ProvidedTypeDefinition(typeName, Some( ProvidedTypeBuilder.MakeGenericType(typedefof<_ DataTable>, [ dataRowType ])))
      
        do //Columns
            let columnsType = ProvidedTypeDefinition("Columns", Some typeof<DataColumnCollection>)
            tableType.AddMember columnsType
            let columns = ProvidedProperty("Columns", columnsType, getterCode = fun args -> <@@ (%%args.Head: DataTable<DataRow>).Columns @@>)
            tableType.AddMember columns
      
            for column in outputColumns do
                let propertyType = ProvidedTypeDefinition(column.Name, Some typeof<DataColumn>)
                columnsType.AddMember propertyType

                let property = 
                    let columnName = column.Name
                    ProvidedProperty(column.Name, propertyType, getterCode = fun args -> <@@ (%%args.Head: DataColumnCollection).[columnName] @@>)
            
                columnsType.AddMember property

        do
            let parameters, updateableColumns = 
                [ 
                    for c in outputColumns do 
                        if not c.ReadOnly
                        then 
                            let dataType = c.MakeProvidedType(customTypes, forceNullability = c.OptionalForInsert)
                            let parameter = 
                                if c.OptionalForInsert
                                then ProvidedParameter(c.Name, parameterType = dataType, optionalValue = null)
                                else ProvidedParameter(c.Name, dataType)

                            yield parameter, c
                ] 
                |> List.unzip

            let methodXmlDoc = 
                String.concat "\n" [
                    for c in updateableColumns do
                        if c.Description <> "" 
                        then 
                            let defaultConstrain = 
                                if c.HasDefaultConstraint 
                                then sprintf " Default constraint: %s." c.DefaultConstraint
                                else ""
                            yield sprintf "<param name='%s'>%s%s</param>" c.Name c.Description defaultConstrain
                ]


            let invokeCode = fun (args: _ list)-> 

                let argsValuesConverted = 
                    (args.Tail, updateableColumns)
                    ||> List.map2 (fun valueExpr c ->
                        if c.OptionalForInsert
                        then 
                            QuotationsFactory.GetMapperFromOptionToObj(c.ClrType, valueExpr)
                        else
                            valueExpr
                    )

                <@@ 
                    let table: DataTable<DataRow> = %%args.[0]
                    let row = table.NewRow()

                    let values: obj[] = %%Expr.NewArray(typeof<obj>, [ for x in argsValuesConverted -> Expr.Coerce(x, typeof<obj>) ])
                    let namesOfUpdateableColumns: string[] = %%Expr.NewArray(typeof<string>, [ for c in updateableColumns -> Expr.Value(c.Name) ])

                    for name, value in Array.zip namesOfUpdateableColumns values do 
                        if not(Convert.IsDBNull(value)) 
                        then 
                            row.[name] <- value
                    row
                @@>

            do 
                let newRowMethod = ProvidedMethod("NewRow", parameters, dataRowType, invokeCode)
                if methodXmlDoc <> "" then newRowMethod.AddXmlDoc methodXmlDoc
                tableType.AddMember newRowMethod

                let addRowMethod =
                    ProvidedMethod(
                        "AddRow", 
                        parameters, 
                        typeof<Void>, 
                        invokeCode = fun args ->
                            let newRow = invokeCode args
                            <@@ (%%args.[0]: DataTable<DataRow>).Rows.Add(%%newRow) @@>
                    )

                if methodXmlDoc <> "" then addRowMethod.AddXmlDoc methodXmlDoc
                tableType.AddMember addRowMethod

        do
            let commonParams = [
                ProvidedParameter("batchSize", typeof<int>, optionalValue = 1)
                ProvidedParameter("continueUpdateOnError", typeof<bool>, optionalValue = false) 
                ProvidedParameter("conflictOption", typeof<ConflictOption>, optionalValue = ConflictOption.OverwriteChanges)
                // if invoked with default - timeout is taken from NpgsqlConnection constructed from connection string
                ProvidedParameter("batchTimeout", typeof<int>, optionalValue = 0)
            ]

            tableType.AddMembers [

                ProvidedMethod(
                    "Update", 
                    ProvidedParameter("connectionString", typeof<string>) :: commonParams, 
                    typeof<int>,
                    fun (Arg6(table, connectionString, batchSize, continueUpdateOnError, conflictOption, batchTimeout)) -> 
                        <@@ 
                            let conn = new NpgsqlConnection(%%connectionString)
                            Utils.UpdateDataTable(%%table, conn, null, %%batchSize, %%continueUpdateOnError, %%conflictOption, %%batchTimeout)
                        @@>
                )

                ProvidedMethod(
                    "Update", 
                    ProvidedParameter("connection", typeof<NpgsqlConnection> ) 
                    :: ProvidedParameter("transaction", typeof<NpgsqlTransaction>, optionalValue = null) 
                    :: commonParams, 
                    typeof<int>,
                    fun (Arg7(table, conn, tx, batchSize, continueUpdateOnError, conflictOption, batchTimeout)) -> 
                        <@@ 
                            Utils.UpdateDataTable(%%table, %%conn, %%tx, %%batchSize, %%continueUpdateOnError, %%conflictOption, %%batchTimeout)
                        @@>
                )

            ]

        tableType

    static member GetOutputTypes (rootTypeName, sql, statementType, customTypes: Map<string, ProvidedTypeDefinition>, resultType, collectionType, singleRow, typeNameSuffix, providedTypeReuse) =    
        let returnType =
            match resultType, statementType with
            | ResultType.DataReader, _
            | _, Control ->
                None
            | _, NonQuery ->
                Some { Single = typeof<int>; RowProvidedType = None }
            | ResultType.DataTable, Query columns ->
                let dataRowType = QuotationsFactory.GetDataRowType (customTypes, columns)
                let dataTableType =
                    QuotationsFactory.GetDataTableType(
                        "Table" + typeNameSuffix,
                        dataRowType,
                        customTypes,
                        columns)

                dataTableType.AddMember dataRowType

                Some { Single = dataTableType; RowProvidedType = None }
            | _, Query columns ->
                let providedRowType =
                    if List.length columns = 1 then
                        columns.Head.MakeProvidedType customTypes
                    elif resultType = ResultType.Records then 
                        QuotationsFactory.GetRecordType (rootTypeName, columns, customTypes, typeNameSuffix, providedTypeReuse) :> Type
                    else
                        match columns with
                        | [ x ] -> x.MakeProvidedType customTypes
                        | xs -> Reflection.FSharpType.MakeTupleType [| for x in xs -> x.MakeProvidedType customTypes |]

                Some {
                    Single =
                        if singleRow then
                            ProvidedTypeBuilder.MakeGenericType (typedefof<_ option>, [ providedRowType ])
                        elif collectionType = CollectionType.ResizeArray then
                            ProvidedTypeBuilder.MakeGenericType (typedefof<ResizeArray<_>>, [ providedRowType ])
                        elif collectionType = CollectionType.Array then
                            providedRowType.MakeArrayType ()
                        elif collectionType = CollectionType.LazySeq then
                            ProvidedTypeBuilder.MakeGenericType (typedefof<LazySeq<_>>, [ providedRowType ])
                        else
                            ProvidedTypeBuilder.MakeGenericType (typedefof<_ list>, [ providedRowType ])
                    RowProvidedType = Some providedRowType }

        { Type = statementType; Sql = sql; ReturnType = returnType }

    static member GetExecuteArgs(sqlParameters: Parameter list, customTypes: Map<string, ProvidedTypeDefinition>) = 
        [
            for p in sqlParameters do
                let parameterName = p.Name

                let t =
                    let customType = customTypes.TryFind p.DataType.UdtTypeName
                    
                    match p.DataType.IsUserDefinedType, customType with
                    | true, Some t ->
                        if p.DataType.ClrType.IsArray then t.MakeArrayType() else upcast t
                    | _ -> p.DataType.ClrType

                if p.Optional
                then
                    assert(p.Direction = ParameterDirection.Input)
                    yield ProvidedParameter(
                        parameterName,
                        parameterType = ProvidedTypeBuilder.MakeGenericType( typedefof<_ option>, [ t ]),
                        optionalValue = null
                    )
                else
                    if p.Direction.HasFlag(ParameterDirection.Output)
                    then
                        yield ProvidedParameter(parameterName, parameterType = t.MakeByRefType(), isOut = true)
                    else                                 
                        yield ProvidedParameter(parameterName, parameterType = t)
        ]

    static member val ConnectionUcis = Reflection.FSharpType.GetUnionCases typeof<Choice<string, NpgsqlConnection * NpgsqlTransaction>>

    static member GetCommandFactoryMethod (cmdProvidedType: ProvidedTypeDefinition, designTimeConfig, isExtended, methodName) = 
        let ctorImpl = typeof<ISqlCommandImplementation>.GetConstructors() |> Array.exactlyOne

        if isExtended then
            let body (Arg3(connection, transaction, commandTimeout)) =
                let arguments = [ Expr.Value (cmdProvidedType.Name.GetHashCode()); designTimeConfig; Expr.NewUnionCase (QuotationsFactory.ConnectionUcis.[1], [ Expr.NewTuple [ connection; transaction ] ]); commandTimeout ]
                Expr.NewObject (ctorImpl, arguments)

            let parameters = [
                ProvidedParameter("connection", typeof<NpgsqlConnection>) 
                ProvidedParameter("transaction", typeof<NpgsqlTransaction>, optionalValue = null)
                ProvidedParameter("commandTimeout", typeof<int>, optionalValue = 0) ]

            ProvidedMethod (methodName, parameters, cmdProvidedType, body, true)
        else
            let body (args: _ list) =
                Expr.NewObject (ctorImpl, [ Expr.Value (cmdProvidedType.Name.GetHashCode()); designTimeConfig; Expr.NewUnionCase (QuotationsFactory.ConnectionUcis.[0], [ args.Head ]) ] @ args.Tail)

            let parameters = [
                ProvidedParameter("connectionString", typeof<string>)
                ProvidedParameter("commandTimeout", typeof<int>, optionalValue = 0) ]

            ProvidedMethod (methodName, parameters, cmdProvidedType, body, true)

    static member AddProvidedTypeToDeclaring resultType returnType (declaringType: ProvidedTypeDefinition) =
        if resultType = ResultType.Records then
            returnType.RowProvidedType
            |> Option.iter (fun x -> if x :? ProvidedTypeDefinition then declaringType.AddMember x)
        elif resultType = ResultType.DataTable && not returnType.Single.IsPrimitive then
            returnType.Single |> declaringType.AddMember

    static member BuildDataColumnsExpr (statements, slimDataColumns) =
        Expr.NewArray (typeof<DataColumn[]>,
            statements
            |> List.map (fun x ->
                match x.Type with
                | Query columns ->
                    Expr.NewArray (typeof<DataColumn>, columns |> List.map (fun x -> x.ToDataColumnExpr slimDataColumns))
                | _ ->
                    QuotationsFactory.DataColumnArrayEmptyExpr))

    static member AddTopLevelTypes (cmdProvidedType: ProvidedTypeDefinition) parameters resultType (methodTypes: MethodTypes) customTypes statements typeToAttachTo =
        let executeArgs = QuotationsFactory.GetExecuteArgs (parameters, customTypes)
        
        let addRedirectToISqlCommandMethods outputType xmlDoc =
            let add outputType name xmlDoc =
                let m = QuotationsFactory.AddGeneratedMethod (parameters, executeArgs, cmdProvidedType.BaseType, outputType, name) 
                Option.iter m.AddXmlDoc xmlDoc
                cmdProvidedType.AddMember m

            if methodTypes.HasFlag MethodTypes.Sync then
                add outputType "Execute" xmlDoc
            if methodTypes.HasFlag MethodTypes.Async then
                add (typedefof<Async<_>>.MakeGenericType outputType) "AsyncExecute" xmlDoc
            if methodTypes.HasFlag MethodTypes.Task then
                add (typedefof<Task<_>>.MakeGenericType outputType) "TaskAsyncExecute" xmlDoc

        match statements with
        | _ when resultType = ResultType.DataReader ->
            addRedirectToISqlCommandMethods typeof<NpgsqlDataReader> None
        | [ { ReturnType = Some returnType; Sql = sql } ] ->
            let xmlDoc = if returnType.Single = typeof<int> then sprintf "Number of rows affected by \"%s\"." sql |> Some else None
            addRedirectToISqlCommandMethods returnType.Single xmlDoc
            QuotationsFactory.AddProvidedTypeToDeclaring resultType returnType typeToAttachTo
        | _ ->
            let resultSetsType = ProvidedTypeDefinition ("ResultSets", baseType = Some typeof<obj[]>, hideObjectMethods = true)

            statements
            |> List.iteri (fun i statement ->
                match statement.Type, statement.ReturnType with
                | NonQuery, _ ->
                    let prop = ProvidedProperty (sprintf "RowsAffected%d" (i + 1), typeof<int>, fun args -> QuotationsFactory.GetValueAtIndexExpr (Expr.Coerce (args.[0], typeof<obj[]>), i))
                    sprintf "Number of rows affected by \"%s\"." statement.Sql |> prop.AddXmlDoc
                    resultSetsType.AddMember prop
                | Query _, Some rt ->
                    let prop = ProvidedProperty (sprintf "ResultSet%d" (i + 1), rt.Single, fun args -> QuotationsFactory.GetValueAtIndexExpr (Expr.Coerce (args.[0], typeof<obj[]>), i))
                    sprintf "Rows returned for query \"%s\"." statement.Sql |> prop.AddXmlDoc
                    resultSetsType.AddMember prop
                | _ -> ()

                statement.ReturnType |> Option.iter (fun rt -> QuotationsFactory.AddProvidedTypeToDeclaring resultType rt typeToAttachTo))

            addRedirectToISqlCommandMethods resultSetsType None
            cmdProvidedType.AddMember resultSetsType


