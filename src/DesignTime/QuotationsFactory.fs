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

type internal RowType = {
    Provided: Type
    ErasedTo: Type
}

type internal ReturnType = {
    Single: Type
    PerRow: RowType option
}  with 

    member this.SeqItemTypeName = 
        match this.PerRow with
        | Some x -> x.ErasedTo.PartiallyQualifiedName
        | None -> null

type internal Statement = {
    Type: StatementType
    ReturnType: ReturnType option
    Sql: string
}

type internal ProvidedTypeReuse =
    | WithCache of ConcurrentDictionary<string, ProvidedTypeDefinition>
    | NoReuse

type internal QuotationsFactory private() = 

    static let (|Arg3|) xs = 
        assert (List.length xs = 3)
        Arg3(xs.[0], xs.[1], xs.[2])

    static let (|Arg4|) xs = 
        assert (List.length xs = 4)
        Arg4(xs.[0], xs.[1], xs.[2], xs.[3])

    static let (|Arg5|) xs = 
        assert (List.length xs = 5)
        Arg5(xs.[0], xs.[1], xs.[2], xs.[3], xs.[4])

    static let (|Arg6|) xs = 
        assert (List.length xs = 6)
        Arg6(xs.[0], xs.[1], xs.[2], xs.[3], xs.[4], xs.[5])

    static let (|Arg7|) xs = 
        assert (List.length xs = 7)
        Arg7(xs.[0], xs.[1], xs.[2], xs.[3], xs.[4], xs.[5], xs.[6])

    static let defaultCommandTimeout =
        use cmd = new NpgsqlCommand ()
        cmd.CommandTimeout

    static member internal GetValueAtIndexExpr: (Expr * int) -> Expr =
        let mi = typeof<Unit>.Assembly.GetType("Microsoft.FSharp.Collections.ArrayModule").GetMethod("Get").MakeGenericMethod typeof<obj>
        fun (arrayExpr, index) -> Expr.Call (mi, [ arrayExpr; Expr.Value index ])

    static member internal ToSqlParamsExpr =
        let mi = typeof<Utils>.GetMethod ("ToSqlParam", BindingFlags.Static ||| BindingFlags.Public)
        fun (ps: Parameter list) -> Expr.NewArray (typeof<NpgsqlParameter>, ps |> List.map (fun p ->
            Expr.Call (mi,
                [ Expr.Value p.Name; Expr.Value p.NpgsqlDbType; Expr.Value (if p.DataType.IsFixedLength then 0 else p.MaxLength); Expr.Value p.Scale; Expr.Value p.Precision ])))

    static member internal GetNullableValueFromDataRow (t: Type, name: string) (exprArgs: Expr list) =
        Expr.Call (typeof<Utils>.GetMethod("GetNullableValueFromDataRow").MakeGenericMethod t, [
            exprArgs.[0]
            Expr.Value name ])

    static member internal SetNullableValueInDataRow (t: Type, name: string) (exprArgs: Expr list) =
        Expr.Call (typeof<Utils>.GetMethod("SetNullableValueInDataRow").MakeGenericMethod t, [
            exprArgs.[0]
            Expr.Value name
            Expr.Coerce (exprArgs.[1], typeof<obj>) ])

    static member internal GetNonNullableValueFromDataRow (name: string) (exprArgs: Expr list) =
        Expr.Call (exprArgs.Head, typeof<DataRow>.GetMethod ("get_Item", [| typeof<string> |]), [ Expr.Value name ])

    static member internal SetNonNullableValueInDataRow (name: string) (exprArgs: Expr list) =
        Expr.Call (exprArgs.Head, typeof<DataRow>.GetMethod ("set_Item", [| typeof<string>; typeof<obj> |]), [ Expr.Value name; Expr.Coerce (exprArgs.[1], typeof<obj>) ])
    
    static member internal GetMapperFromOptionToObj (t: Type, value: Expr) =
        Expr.Call (typeof<Utils>.GetMethod("OptionToObj").MakeGenericMethod t, [ Expr.Coerce (value, typeof<obj>) ])

    static member internal AddGeneratedMethod (sqlParameters: Parameter list, executeArgs: ProvidedParameter list, erasedType, providedOutputType, name) =
        let mappedInputParamValues (exprArgs: Expr list) = 
            (exprArgs.Tail, sqlParameters)
            ||> List.map2 (fun expr param ->
                let value = 
                    if param.Direction = ParameterDirection.Input
                    then 
                        if param.Optional 
                        then 
                            QuotationsFactory.GetMapperFromOptionToObj(param.DataType.ClrType, expr)
                        else
                            expr
                    else
                        let t = param.DataType.ClrType

                        if t.IsArray
                        then Expr.Value(Array.CreateInstance(t.GetElementType(), param.MaxLength))
                        else Expr.Value(Activator.CreateInstance(t), t)

                Expr.NewTuple [ Expr.Value param.Name; Expr.Coerce (value, typeof<obj>) ])

        let invokeCode exprArgs =
            let methodInfo = typeof<ISqlCommand>.GetMethod(name)
            let vals = mappedInputParamValues(exprArgs)
            let paramValues = Expr.NewArray( typeof<string * obj>, elements = vals)
            Expr.Call(Expr.Coerce(exprArgs.[0], erasedType), methodInfo, [ paramValues ])    

        ProvidedMethod(name, executeArgs, providedOutputType, invokeCode)

    static member internal GetRecordType(columns: Column list, customTypes: Map<string, ProvidedTypeDefinition>, typeNameSuffix, providedTypeReuse) =
        columns 
            |> Seq.groupBy (fun x -> x.Name) 
            |> Seq.tryFind (fun (_, xs) -> Seq.length xs > 1)
            |> Option.iter (fun (name, _) -> failwithf "Non-unique column name %s is illegal for ResultType.Records." name)
        
        let createType typeName sortColumns =
            let recordType = ProvidedTypeDefinition(typeName, baseType = Some typeof<obj>, hideObjectMethods = true)
            
            let properties, ctorParameters = 
                if sortColumns then columns |> List.sortBy (fun x -> x.Name) else columns
                |> List.mapi (fun i col ->
                    let propertyName =
                        if String.IsNullOrEmpty col.Name then
                            let originalIndex = List.findIndex (fun x -> x = col) columns
                            failwithf "Column #%i doesn't have a name. Only named columns are supported. Use an explicit alias." (originalIndex + 1)
                        else
                            col.Name

                    let propType = col.MakeProvidedType(customTypes)
                    let property = ProvidedProperty(propertyName, propType, fun args -> QuotationsFactory.GetValueAtIndexExpr (Expr.Coerce(args.[0], typeof<obj[]>), i))

                    let ctorParameter = ProvidedParameter(propertyName, propType)  

                    property, ctorParameter
                )
                |> List.unzip

            recordType.AddMembers properties

            let ctor = ProvidedConstructor(ctorParameters, fun args -> Expr.NewArray(typeof<obj>, List.map (fun arg -> Expr.Coerce(arg, typeof<obj>)) args))
            recordType.AddMember ctor
            
            recordType

        match providedTypeReuse with
        | WithCache cache ->
            let typeName = columns |> List.map (fun x ->
                let t = if Map.containsKey x.DataType.FullName customTypes then x.DataType.FullName else x.ClrType.Name
                if x.Nullable then sprintf "%s:Option<%s>" x.Name t else sprintf "%s:%s" x.Name t) |> List.sort |> String.concat ", "

            cache.GetOrAdd (typeName, fun typeName -> createType typeName true)
        | NoReuse ->
            createType ("Record" + typeNameSuffix) false

    static member internal GetDataRowPropertyGetterAndSetterCode (column: Column) =
        let name = column.Name
        if column.Nullable then
            let setter = if column.ReadOnly then None else Some (QuotationsFactory.SetNullableValueInDataRow (column.ClrType, name))
            QuotationsFactory.GetNullableValueFromDataRow (column.ClrType, name), setter
        else
            let setter = if column.ReadOnly then None else Some (QuotationsFactory.SetNonNullableValueInDataRow name)
            QuotationsFactory.GetNonNullableValueFromDataRow name, setter

    static member internal GetDataRowType (customTypes: Map<string, ProvidedTypeDefinition>, columns: Column list) = 
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

    static member internal GetDataTableType
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
                ProvidedParameter("batchTimeout", typeof<int>, optionalValue = defaultCommandTimeout) 
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

    static member internal GetOutputTypes(sql, statementType, customTypes: Map<string, ProvidedTypeDefinition>, resultType, singleRow, typeNameSuffix, providedTypeReuse) =    
        let returnType =
            match resultType, statementType with
            | ResultType.DataReader, _
            | _, Control ->
                None
            | _, NonQuery ->
                Some { Single = typeof<int>; PerRow = None }
            | ResultType.DataTable, Query columns ->
                let dataRowType = QuotationsFactory.GetDataRowType (customTypes, columns)
                let dataTableType =
                    QuotationsFactory.GetDataTableType(
                        "Table" + typeNameSuffix,
                        dataRowType,
                        customTypes,
                        columns)

                dataTableType.AddMember dataRowType

                Some { Single = dataTableType; PerRow = None }
            | _, Query columns ->
                let providedRowType, erasedToRowType =
                    if List.length columns = 1 then
                        let column0 = columns.Head
                        let erasedTo = column0.ClrTypeConsideringNullability
                        let provided = column0.MakeProvidedType customTypes
                        provided, erasedTo
                    elif resultType = ResultType.Records then 
                        let provided = QuotationsFactory.GetRecordType (columns, customTypes, typeNameSuffix, providedTypeReuse)
                        upcast provided, typeof<obj>
                    else
                        let providedType =
                            match columns with
                            | [ x ] -> x.MakeProvidedType customTypes
                            | xs -> Reflection.FSharpType.MakeTupleType [| for x in xs -> x.MakeProvidedType customTypes |]

                        let erasedToTupleType =
                            match columns with
                            | [ x ] -> x.ClrTypeConsideringNullability
                            | xs -> Reflection.FSharpType.MakeTupleType [| for x in xs -> x.ClrTypeConsideringNullability |]

                        providedType, erasedToTupleType

                Some {
                    Single =
                        if singleRow then
                            ProvidedTypeBuilder.MakeGenericType (typedefof<_ option>, [ providedRowType ])
                        else
                            ProvidedTypeBuilder.MakeGenericType (typedefof<_ list>, [ providedRowType ])
                    PerRow = Some { Provided = providedRowType; ErasedTo = erasedToRowType } }

        { Type = statementType; Sql = sql; ReturnType = returnType }

    static member internal GetExecuteArgs(sqlParameters: Parameter list, customTypes: Map<string, ProvidedTypeDefinition>) = 
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

    static member ConnectionUcis = Reflection.FSharpType.GetUnionCases typeof<Choice<string, NpgsqlConnection * NpgsqlTransaction>>

    static member internal GetCommandFactoryMethod (cmdProvidedType: ProvidedTypeDefinition, designTimeConfig, isExtended, methodName) = 
        let ctorImpl = typeof<ISqlCommandImplementation>.GetConstructors() |> Array.exactlyOne

        if isExtended then
            let body (Arg3(connection, transaction, commandTimeout)) =
                let arguments = [ designTimeConfig; Expr.NewUnionCase (QuotationsFactory.ConnectionUcis.[1], [ Expr.NewTuple [ connection; transaction ] ]); commandTimeout ]
                Expr.NewObject (ctorImpl, arguments)

            let parameters = [
                ProvidedParameter("connection", typeof<NpgsqlConnection>) 
                ProvidedParameter("transaction", typeof<NpgsqlTransaction>, optionalValue = null)
                ProvidedParameter("commandTimeout", typeof<int>, optionalValue = defaultCommandTimeout) ]

            ProvidedMethod (methodName, parameters, cmdProvidedType, body, true)
        else
            let body (args: _ list) =
                Expr.NewObject (ctorImpl, designTimeConfig :: Expr.NewUnionCase (QuotationsFactory.ConnectionUcis.[0], [ args.Head ]) :: args.Tail)

            let parameters = [
                ProvidedParameter("connectionString", typeof<string>)
                ProvidedParameter("commandTimeout", typeof<int>, optionalValue = defaultCommandTimeout) ]

            ProvidedMethod (methodName, parameters, cmdProvidedType, body, true)

    static member internal AddProvidedTypeToDeclaring resultType returnType columnCount (declaringType: ProvidedTypeDefinition) =
        if resultType = ResultType.Records then
            returnType.PerRow
            |> Option.filter (fun x -> x.Provided <> x.ErasedTo && columnCount > 1)
            |> Option.iter (fun x -> declaringType.AddMember x.Provided)
        elif resultType = ResultType.DataTable && not returnType.Single.IsPrimitive then
            returnType.Single |> declaringType.AddMember

    static member EmptyResultSet = Expr.NewRecord (typeof<ResultSetDefinition>, [ Expr.Value (null: string); Expr.NewArray (typeof<DataColumn>, []) ])

    static member internal BuildResultSetDefinitionsExpr (statements, slimDataColumns) =
        Expr.NewArray (typeof<ResultSetDefinition>,
            statements
            |> List.map (fun x ->
                match x.ReturnType, x.Type with
                | Some returnType, Query columns ->
                    Expr.NewRecord (typeof<ResultSetDefinition>, [
                        Expr.Value returnType.SeqItemTypeName;
                        Expr.NewArray (typeof<DataColumn>, columns |> List.map (fun x -> x.ToDataColumnExpr slimDataColumns)) ])
                | _ ->
                    QuotationsFactory.EmptyResultSet))

    static member internal AddTopLevelTypes (cmdProvidedType: ProvidedTypeDefinition) parameters resultType (methodTypes: MethodTypes) customTypes statements typeToAttachTo =
        let executeArgs = QuotationsFactory.GetExecuteArgs (parameters, customTypes)
        
        let addRedirectToISqlCommandMethod outputType name xmlDoc = 
            let m = QuotationsFactory.AddGeneratedMethod (parameters, executeArgs, cmdProvidedType.BaseType, outputType, name) 
            Option.iter m.AddXmlDoc xmlDoc
            cmdProvidedType.AddMember m

        match statements with
        | _ when resultType = ResultType.DataReader ->
            if methodTypes.HasFlag MethodTypes.Sync then
                addRedirectToISqlCommandMethod typeof<NpgsqlDataReader> "Execute" None

            if methodTypes.HasFlag MethodTypes.Async then
                addRedirectToISqlCommandMethod typeof<Async<NpgsqlDataReader>> "AsyncExecute" None
        | [ { ReturnType = Some returnType; Sql = sql; Type = typ } ] ->
            let xmlDoc = if returnType.Single = typeof<int> then sprintf "Returns the number of rows affected by \"%s\"." sql |> Some else None

            if methodTypes.HasFlag MethodTypes.Sync then
                addRedirectToISqlCommandMethod returnType.Single "Execute" xmlDoc

            if methodTypes.HasFlag MethodTypes.Async then
                let asyncReturnType = ProvidedTypeBuilder.MakeGenericType (typedefof<_ Async>, [ returnType.Single ])
                addRedirectToISqlCommandMethod asyncReturnType "AsyncExecute" xmlDoc

            let columnCount =
                match typ with
                | Query columns -> columns.Length
                | _ -> 0

            QuotationsFactory.AddProvidedTypeToDeclaring resultType returnType columnCount typeToAttachTo
        | _ ->
            let resultSetsType = ProvidedTypeDefinition ("ResultSets", baseType = Some typeof<obj>, hideObjectMethods = true)

            let props, ctorParams =
                statements
                |> List.mapi (fun i statement -> i, statement)
                |> List.choose (fun (i, statement) ->
                    match statement.Type, statement.ReturnType with
                    | NonQuery, Some rt -> Some (i, rt, sprintf "RowsAffected%d" (i + 1), sprintf "Number of rows affected by \"%s\"." statement.Sql)
                    | Query _, Some rt -> Some (i, rt, sprintf "ResultSet%d" (i + 1), sprintf "Rows returned for query \"%s\"." statement.Sql)
                    | _ -> None)
                |> List.map (fun (i, rt, propName, xmlDoc) ->
                    let prop = ProvidedProperty (propName, rt.Single, fun args -> QuotationsFactory.GetValueAtIndexExpr (Expr.Coerce(args.[0], typeof<obj[]>), i))
                    prop.AddXmlDoc xmlDoc
                    let ctorParam = ProvidedParameter (propName, rt.Single)
                    prop, ctorParam)
                |> List.unzip

            resultSetsType.AddMembers props

            let ctor = ProvidedConstructor (ctorParams, fun args -> Expr.NewArray(typeof<obj>, args))
            resultSetsType.AddMember ctor

            statements
            |> List.choose (fun statement -> statement.ReturnType |> Option.map (fun rt -> (match statement.Type with Query columns -> columns.Length | _ -> 0), rt))
            |> List.iter (fun (columnCount, rt) -> QuotationsFactory.AddProvidedTypeToDeclaring resultType rt columnCount typeToAttachTo)

            if methodTypes.HasFlag MethodTypes.Sync then
                addRedirectToISqlCommandMethod resultSetsType "Execute" None
            
            if methodTypes.HasFlag MethodTypes.Async then
                let asyncReturnType = ProvidedTypeBuilder.MakeGenericType (typedefof<_ Async>, [ resultSetsType ])
                addRedirectToISqlCommandMethod asyncReturnType "AsyncExecute" None

            cmdProvidedType.AddMember resultSetsType


