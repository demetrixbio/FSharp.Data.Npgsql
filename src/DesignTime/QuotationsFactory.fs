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
        | Some x -> Expr.Value( x.ErasedTo.PartiallyQualifiedName)
        | None -> <@@ null: string @@>

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

    static let defaultCommandTimeout = (new NpgsqlCommand()).CommandTimeout

    static let getValueAtIndex = typeof<Unit>.Assembly.GetType("Microsoft.FSharp.Collections.ArrayModule").GetMethod("Get").MakeGenericMethod(typeof<obj>)

    static member internal GetValueAtIndexExpr arrayExpr index = Expr.Call(getValueAtIndex, [ arrayExpr; Expr.Value index ])

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

            if not isFixedLength then x.Size <- %%Expr.Value p.MaxLength 

            x.Precision <- %%Expr.Value p.Precision
            x.Scale <- %%Expr.Value p.Scale

            x
        @@>

    static member internal GetNullableValueFromDataRow<'T>(exprArgs : Expr list, name : string) =
        <@
            let row : DataRow = %%exprArgs.[0]
            if row.IsNull name then None else Some(unbox<'T> row.[name])
        @> 

    static member internal SetNullableValueInDataRow<'T>(exprArgs : Expr list, name : string) =
        <@
            (%%exprArgs.[0] : DataRow).[name] <- match (%%exprArgs.[1] : option<'T>) with None -> Utils.DbNull | Some value -> box value
        @> 

    static member internal GetNonNullableValueFromDataRow<'T>(exprArgs : Expr list, name: string) =
        <@ (%%exprArgs.[0] : DataRow).[name] @>

    static member internal SetNonNullableValueInDataRow<'T>(exprArgs : Expr list, name : string) =
        <@ (%%exprArgs.[0] : DataRow).[name] <- %%Expr.Coerce(exprArgs.[1], typeof<obj>) @>
    
    static member internal OptionToObj<'T> value = <@@ match %%value with Some (x : 'T) -> box x | None -> Utils.DbNull @@>    
    
    static member internal GetMapperFromOptionToObj(t: Type, value: Expr) =
        typeof<QuotationsFactory>
            .GetMethod("OptionToObj", BindingFlags.NonPublic ||| BindingFlags.Static)
            .MakeGenericMethod(t)
            .Invoke(null, [| box value|])
            |> unbox        

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
                    let property = ProvidedProperty(propertyName, propType, fun args -> QuotationsFactory.GetValueAtIndexExpr (Expr.Coerce(args.[0], typeof<obj[]>)) i)

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
            let getter = QuotationsFactory.GetBody("GetNullableValueFromDataRow", column.ClrType, name)
            let setter = if column.ReadOnly then None else Some( QuotationsFactory.GetBody("SetNullableValueInDataRow", column.ClrType, name))
            getter, setter
        else
            let getter = QuotationsFactory.GetBody("GetNonNullableValueFromDataRow", column.ClrType, name)
            let setter = if column.ReadOnly then None else Some( QuotationsFactory.GetBody("SetNonNullableValueInDataRow", column.ClrType, name))
            getter, setter

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
                            QuotationsFactory.GetMapperFromOptionToObj(c.ClrType, valueExpr) |>unbox
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

    static member internal GetOutputTypes(outputColumns, customTypes: Map<string, ProvidedTypeDefinition>, resultType, commandBehaviour: CommandBehavior,
                                          typeNameSuffix, providedTypeReuse) =    
         
        if resultType = ResultType.DataReader
        then 
            { Single = typeof<NpgsqlDataReader>; PerRow = None }
        elif List.isEmpty outputColumns
        then 
            { Single = typeof<int>; PerRow = None }
        elif resultType = ResultType.DataTable
        then
            let dataRowType = QuotationsFactory.GetDataRowType(customTypes, outputColumns)
            let dataTableType = 
                QuotationsFactory.GetDataTableType(
                    "Table" + typeNameSuffix, 
                    dataRowType,
                    customTypes,
                    outputColumns)

            dataTableType.AddMember dataRowType

            { Single = dataTableType; PerRow = None }

        else 
            let providedRowType, erasedToRowType = 
                if List.length outputColumns = 1
                then
                    let column0 = outputColumns.Head
                    let erasedTo = column0.ClrTypeConsideringNullability
                    let provided = column0.MakeProvidedType(customTypes)
                    provided, erasedTo

                elif resultType = ResultType.Records 
                then 
                    let provided = QuotationsFactory.GetRecordType(outputColumns, customTypes, typeNameSuffix, providedTypeReuse)
                    upcast provided, typeof<obj>
                else 
                    let providedType = 
                        match outputColumns with
                        | [ x ] -> x.MakeProvidedType(customTypes)
                        | xs -> Reflection.FSharpType.MakeTupleType [| for x in xs -> x.MakeProvidedType(customTypes) |]

                    let erasedToTupleType = 
                        match outputColumns with
                        | [ x ] -> x.ClrTypeConsideringNullability
                        | xs -> Reflection.FSharpType.MakeTupleType [| for x in xs -> x.ClrTypeConsideringNullability |]

                    providedType, erasedToTupleType
            
            { 
                Single = 
                    if commandBehaviour.HasFlag(CommandBehavior.SingleRow)  
                    then
                        ProvidedTypeBuilder.MakeGenericType(typedefof<_ option>, [ providedRowType ])
                    else
                        ProvidedTypeBuilder.MakeGenericType(typedefof<_ list>, [ providedRowType ])

                PerRow = Some { Provided = providedRowType; ErasedTo = erasedToRowType }               
            }

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

    static member internal GetCommandFactoryMethod (cmdProvidedType: ProvidedTypeDefinition, designTimeConfig, isExtended, methodName) = 
        let ctorImpl = typeof<``ISqlCommand Implementation``>.GetConstructors() |> Array.exactlyOne

        if isExtended then
            let body (Arg3(connection, transaction, commandTimeout)) = 
                let arguments = [  
                    designTimeConfig
                    <@@ Choice<string, NpgsqlConnection * NpgsqlTransaction>.Choice2Of2(%%connection, %%transaction) @@>
                    commandTimeout
                ]
                Expr.NewObject(ctorImpl, arguments)
                    
            let parameters = [ 
                ProvidedParameter("connection", typeof<NpgsqlConnection>) 
                ProvidedParameter("transaction", typeof<NpgsqlTransaction>, optionalValue = null)
                ProvidedParameter("commandTimeout", typeof<int>, optionalValue = defaultCommandTimeout) ]

            ProvidedMethod (methodName, parameters, cmdProvidedType, body, true)
        else
            let body (args: _ list) = 
                Expr.NewObject(ctorImpl, designTimeConfig :: <@@ Choice<string, NpgsqlConnection * NpgsqlTransaction>.Choice1Of2 %%args.Head @@> :: args.Tail)

            let parameters = [ 
                ProvidedParameter("connectionString", typeof<string>) 
                ProvidedParameter("commandTimeout", typeof<int>, optionalValue = defaultCommandTimeout) ]

            ProvidedMethod (methodName, parameters, cmdProvidedType, body, true)
       
    static member internal AddProvidedTypeToDeclaring resultType returnType outputColumns (declaringType: ProvidedTypeDefinition) =
        if resultType = ResultType.Records 
        then
            returnType.PerRow 
            |> Option.filter (fun x -> x.Provided <> x.ErasedTo && List.length outputColumns > 1)
            |> Option.iter (fun x -> declaringType.AddMember x.Provided)

        elif resultType = ResultType.DataTable && not returnType.Single.IsPrimitive
        then
            returnType.Single |> declaringType.AddMember

    static member internal BuildResultSetDefinitions (outputColumns: Column list list) (returnTypes: ReturnType list) =
        List.zip outputColumns returnTypes
        |> List.map (fun (outputColumns, returnType) ->
            <@@ {
                SeqItemTypeName = %%returnType.SeqItemTypeName
                ExpectedColumns = %%Expr.NewArray(typeof<DataColumn>, [ for c in outputColumns -> c.ToDataColumnExpr() ])
            } @@>)

    static member internal AddTopLevelTypes (cmdProvidedType: ProvidedTypeDefinition) parameters resultType customTypes returnTypes (outputColumns: Column list list) typeToAttachTo =
        let executeArgs = QuotationsFactory.GetExecuteArgs(parameters, customTypes)
        
        let addRedirectToISqlCommandMethod outputType name = 
            QuotationsFactory.AddGeneratedMethod(parameters, executeArgs, cmdProvidedType.BaseType, outputType, name) 
            |> cmdProvidedType.AddMember

        match returnTypes with
        | [ returnType ] ->
            addRedirectToISqlCommandMethod returnType.Single "Execute" 
                        
            let asyncReturnType = ProvidedTypeBuilder.MakeGenericType(typedefof<_ Async>, [ returnType.Single ])
            addRedirectToISqlCommandMethod asyncReturnType "AsyncExecute"

            QuotationsFactory.AddProvidedTypeToDeclaring resultType returnType outputColumns.Head typeToAttachTo
        | _ ->
            let resultSetsType = ProvidedTypeDefinition("ResultSets", baseType = Some typeof<obj>, hideObjectMethods = true)
            let props, ctorParams =
                returnTypes
                |> List.mapi (fun i rt ->
                    let propName = sprintf "ResultSet%d" (i + 1)
                    let prop = ProvidedProperty(propName, rt.Single, fun args -> QuotationsFactory.GetValueAtIndexExpr (Expr.Coerce(args.[0], typeof<obj[]>)) i)
                    let ctorParam = ProvidedParameter(propName, rt.Single)
                    prop, ctorParam)
                |> List.unzip

            resultSetsType.AddMembers props

            let ctor = ProvidedConstructor(ctorParams, fun args -> Expr.NewArray(typeof<obj>, args))
            resultSetsType.AddMember ctor

            List.zip outputColumns returnTypes
            |> List.iter (fun (outputColumns, returnType) -> QuotationsFactory.AddProvidedTypeToDeclaring resultType returnType outputColumns typeToAttachTo)

            addRedirectToISqlCommandMethod resultSetsType "Execute" 
            
            let asyncReturnType = ProvidedTypeBuilder.MakeGenericType(typedefof<_ Async>, [ resultSetsType ])
            addRedirectToISqlCommandMethod asyncReturnType "AsyncExecute"

            cmdProvidedType.AddMember resultSetsType


