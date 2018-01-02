namespace FSharp.Data

open System
open System.Reflection
open System.Data
open Microsoft.FSharp.Quotations
open ProviderImplementation.ProvidedTypes
open FSharp.Data.InformationSchema
open Npgsql
open NpgsqlTypes
open Npgsql.PostgresTypes
open System.Collections.Generic

type internal RowType = {
    Provided: Type
    ErasedTo: Type
    Mapping: Expr
}

type internal ReturnType = {
    Single: Type
    PerRow: RowType option
}  with 
    member this.Row2ItemMapping = 
        match this.PerRow with
        | Some x -> x.Mapping
        | None -> Expr.Value Unchecked.defaultof<obj[] -> obj> 
    member this.SeqItemTypeName = 
        match this.PerRow with
        | Some x -> Expr.Value( x.ErasedTo.AssemblyQualifiedName)
        | None -> <@@ null: string @@>

[<CompilerMessageAttribute("This API supports the FSharp.Data.Npgsql infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
type DesignTime private() = 
    static member internal AddGeneratedMethod
        (sqlParameters: Parameter list, hasOutputParameters, executeArgs: ProvidedParameter list, erasedType, providedOutputType, name) =

        let mappedInputParamValues (exprArgs: Expr list) = 
            (exprArgs.Tail, sqlParameters)
            ||> List.map2 (fun expr param ->
                let value = 
                    if param.Direction = ParameterDirection.Input
                    then 
                        if param.Optional 
                        then 
                            typeof<QuotationsFactory>
                                .GetMethod("OptionToObj", BindingFlags.NonPublic ||| BindingFlags.Static)
                                .MakeGenericMethod(param.DataType.ClrType)
                                .Invoke(null, [| box expr|])
                                |> unbox
                        else
                            expr
                    else
                        let t = param.DataType.ClrType

                        if t.IsArray
                        then Expr.Value(Array.CreateInstance(t.GetElementType(), param.Size))
                        else Expr.Value(Activator.CreateInstance(t), t)

                <@@ (%%Expr.Value(param.Name) : string), %%Expr.Coerce(value, typeof<obj>) @@>
            )

        let invokeCode exprArgs =
            let methodInfo = typeof<ISqlCommand>.GetMethod(name)
            let vals = mappedInputParamValues(exprArgs)
            let paramValues = Expr.NewArray( typeof<string * obj>, elements = vals)
            if not hasOutputParameters
            then 
                Expr.Call( Expr.Coerce( exprArgs.[0], erasedType), methodInfo, [ paramValues ])    
            else
                let mapOutParamValues = 
                    let arr = Var("parameters", typeof<(string * obj)[]>)
                    let body = 
                        (sqlParameters, exprArgs.Tail)
                        ||> List.zip
                        |> List.mapi (fun index (sqlParam, argExpr) ->
                            if sqlParam.Direction.HasFlag( ParameterDirection.Output)
                            then 
                                let mi = 
                                    typeof<DesignTime>
                                        .GetMethod("SetRef")
                                        .MakeGenericMethod( sqlParam.DataType.ClrType)
                                Expr.Call(mi, [ argExpr; Expr.Var arr; Expr.Value index ]) |> Some
                            else 
                                None
                        ) 
                        |> List.choose id
                        |> List.fold (fun acc x -> Expr.Sequential(acc, x)) <@@ () @@>

                    Expr.Lambda(arr, body)

                let xs = Var("parameters", typeof<(string * obj)[]>)
                let execute = Expr.Lambda(xs , Expr.Call( Expr.Coerce( exprArgs.[0], erasedType), methodInfo, [ Expr.Var xs ]))
                <@@
                    let ps: (string * obj)[] = %%paramValues
                    let result = (%%execute) ps
                    ps |> %%mapOutParamValues
                    result
                @@>

        ProvidedMethod(name, executeArgs, providedOutputType, invokeCode)

    static member SetRef<'t>(r : byref<'t>, arr: (string * obj)[], i) = 
        r <- arr.[i] |> snd |> unbox

    static member internal GetRecordType(columns: Column list) =
        columns 
            |> Seq.groupBy (fun x -> x.Name) 
            |> Seq.tryFind (fun (_, xs) -> Seq.length xs > 1)
            |> Option.iter (fun (name, _) -> failwithf "Non-unique column name %s is illegal for ResultType.Records." name)
        
        let recordType = ProvidedTypeDefinition("Record", baseType = Some typeof<obj>, hideObjectMethods = true)
        let properties, ctorParameters = 
            columns
            |> List.mapi ( fun i col ->
                let propertyName = col.Name

                if propertyName = "" then failwithf "Column #%i doesn't have name. Only columns with names accepted. Use explicit alias." (i + 1)
                    
                let propType = col.GetProvidedType()

                let property = 
                    ProvidedProperty(
                        propertyName, 
                        propType, 
                        fun args -> <@@ %%args.[0] |> unbox |> Map.find<string, obj> propertyName @@>
                    )

                let ctorParameter = ProvidedParameter(propertyName, propType)  

                property, ctorParameter
            )
            |> List.unzip

        recordType.AddMembers properties

        let invokeCode args =
            let pairs =  
                List.zip args properties //Because we need original names in dictionary
                |> List.map (fun (arg,p) -> <@@ (%%Expr.Value(p.Name):string), %%Expr.Coerce(arg, typeof<obj>) @@>)

            <@@
                Map.ofArray<string, obj>( %%Expr.NewArray( typeof<string * obj>, pairs))
            @@> 
        
        let ctor = ProvidedConstructor(ctorParameters, invokeCode)
        recordType.AddMember ctor
        
        recordType

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

    static member internal GetDataRowType (columns: Column list) = 
        let rowType = ProvidedTypeDefinition("Row", Some typeof<DataRow>)

        columns 
        |> List.mapi(fun i col ->

            if col.Name = "" then failwithf "Column #%i doesn't have name. Only columns with names accepted. Use explicit alias." (i + 1)

            let propertyType = col.GetProvidedType()

            let getter, setter = DesignTime.GetDataRowPropertyGetterAndSetterCode col

            let p = ProvidedProperty(col.Name, propertyType, getter, ?setterCode = setter)

            if col.Description <> "" then p.AddXmlDoc col.Description

            p
        )
        |> rowType.AddMembers

        rowType

    static member internal GetDataTableType(typeName, dataRowType: ProvidedTypeDefinition, outputColumns: Column list) =
        let tableType = ProvidedTypeBuilder.MakeGenericType(typedefof<_ DataTable>, [ dataRowType ])
        let tableProvidedType = ProvidedTypeDefinition(typeName, Some tableType)
      
        let columnsType = ProvidedTypeDefinition("Columns", Some typeof<DataColumnCollection>)

        let getterCode args =
            <@@
                let table : DataTable<DataRow> = %% List.head args
                table.Columns
            @@>

        let columnsProperty = ProvidedProperty("Columns", columnsType, getterCode)
        tableProvidedType.AddMember columnsType

        tableProvidedType.AddMember columnsProperty
      
        for column in outputColumns do
            let propertyType = ProvidedTypeDefinition(column.Name, Some typeof<DataColumn>)

            let getterCode args =
                let columnName = column.Name
                <@@ 
                    let columns: DataColumnCollection = %% List.head args
                    columns.[columnName]
                @@>

            let property = ProvidedProperty(column.Name, propertyType, getterCode)
            
            columnsType.AddMember property
            columnsType.AddMember propertyType


        let tableProperty =
            ProvidedProperty(
                "Table"
                , tableProvidedType
                , getterCode = 
                    fun args ->
                        <@@
                            let row : DataRow = %%args.[0]
                            let table = row.Table
                            table
                        @@>
            )

        dataRowType.AddMember tableProperty

        tableProvidedType

    static member internal GetOutputTypes (outputColumns: Column list, resultType, rank: ResultRank, hasOutputParameters) =    
         
        if resultType = ResultType.DataReader 
        then 
            { Single = typeof<NpgsqlDataReader>; PerRow = None }
        elif outputColumns.IsEmpty
        then 
            { Single = typeof<int>; PerRow = None }
        elif resultType = ResultType.DataTable 
        then
            let dataRowType = DesignTime.GetDataRowType(outputColumns)
            let dataTableType = DesignTime.GetDataTableType("Table", dataRowType, outputColumns)
            dataTableType.AddMember dataRowType

            { Single = dataTableType; PerRow = None }

        else 
            let providedRowType, erasedToRowType, rowMapping = 
                if List.length outputColumns = 1
                then
                    let column0 = outputColumns.Head
                    let erasedTo = column0.ClrTypeConsideringNullability
                    let provided = column0.GetProvidedType()
                    let values = Var("values", typeof<obj[]>)
                    let indexGet = Expr.Call(Expr.Var values, typeof<Array>.GetMethod("GetValue", [| typeof<int> |]), [ Expr.Value 0 ])
                    provided, erasedTo, Expr.Lambda(values,  indexGet) 

                elif resultType = ResultType.Records 
                then 
                    let provided = DesignTime.GetRecordType(outputColumns)
                    let names = Expr.NewArray(typeof<string>, outputColumns |> List.map (fun x -> Expr.Value(x.Name))) 
                    let mapping = <@@ fun (values: obj[]) -> ((%%names: string[]), values) ||> Array.zip |> Map.ofArray |> box @@>

                    upcast provided, typeof<obj>, mapping
                else 
                    let providedType = 
                        match outputColumns with
                        | [ x ] -> x.GetProvidedType()
                        | xs -> Reflection.FSharpType.MakeTupleType [| for x in xs -> x.GetProvidedType() |]

                    let erasedToTupleType = 
                        match outputColumns with
                        | [ x ] -> x.ClrTypeConsideringNullability
                        | xs -> Reflection.FSharpType.MakeTupleType [| for x in xs -> x.ClrTypeConsideringNullability |]

                    let clrTypeName = erasedToTupleType.FullName
                    let mapping = <@@ Reflection.FSharpValue.PreComputeTupleConstructor( Type.GetType( clrTypeName, throwOnError = true))  @@>
                    providedType, erasedToTupleType, mapping
            
            let nullsToOptions = QuotationsFactory.MapArrayNullableItems(outputColumns, "MapArrayObjItemToOption") 
            let combineWithNullsToOptions = typeof<QuotationsFactory>.GetMethod("GetMapperWithNullsToOptions") 
            
            { 
                Single = 
                    match rank with
                    | ResultRank.ScalarValue -> providedRowType
                    | ResultRank.SingleRow -> ProvidedTypeBuilder.MakeGenericType(typedefof<_ option>, [ providedRowType ])
                    | ResultRank.Sequence -> 
                        let collectionType = if hasOutputParameters then typedefof<_ list> else typedefof<_ seq>
                        ProvidedTypeBuilder.MakeGenericType( collectionType, [ providedRowType ])
                    | unexpected -> failwithf "Unexpected ResultRank value: %A" unexpected

                PerRow = Some { 
                    Provided = providedRowType
                    ErasedTo = erasedToRowType
                    Mapping = Expr.Call( combineWithNullsToOptions, [ nullsToOptions; rowMapping ]) 
                }               
            }

    static member internal GetExecuteArgs(sqlParameters: Parameter list, customType: IDictionary<_, ProvidedTypeDefinition>) = 
        [
            for p in sqlParameters do
                //assert p.Name.StartsWith("@")
                //let parameterName = p.Name.Substring 1
                let parameterName = p.Name

                let t = 
                    if p.DataType.IsUserDefinedType
                    then
                        let t = customType.[ p.DataType.UdtTypeName ] 
                        if p.DataType.ClrType.IsArray 
                        then t.MakeArrayType()
                        else upcast t
                    else p.DataType.ClrType

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

    static member internal GetCommandCtors(cmdProvidedType: ProvidedTypeDefinition, designTimeConfig, designTimeConnectionString: string, ?factoryMethodName) = 
        [
            let ctorImpl = typeof<``ISqlCommand Implementation``>.GetConstructor [| typeof<DesignTimeConfig>; typeof<Connection>; typeof<int> |]

            let parameters1 = [ 
                ProvidedParameter("connectionString", typeof<string>) 
                ProvidedParameter("commandTimeout", typeof<int>, optionalValue = defaultCommandTimeout) 
            ]

            let body1 (args: _ list) = Expr.NewObject(ctorImpl, designTimeConfig :: <@@ Connection.Choice1Of2 %%args.Head @@> :: args.Tail)

            yield ProvidedConstructor(parameters1, invokeCode = body1) :> MemberInfo
            
            if factoryMethodName.IsSome
            then 
                yield upcast ProvidedMethod(factoryMethodName.Value, parameters1, returnType = cmdProvidedType, invokeCode = body1, isStatic = true)
           
            let parameters2 = 
                    [ 
                        ProvidedParameter("transaction", typeof<NpgsqlTransaction>) 
                        ProvidedParameter("commandTimeout", typeof<int>, optionalValue = defaultCommandTimeout) 
                    ]

            let body2 (args: _ list) = Expr.NewObject(ctorImpl, designTimeConfig :: <@@ Connection.Choice2Of2 %%args.Head @@> :: args.Tail )
                    
            yield upcast ProvidedConstructor(parameters2, invokeCode = body2)
            if factoryMethodName.IsSome
            then 
                yield upcast ProvidedMethod(factoryMethodName.Value, parameters2, returnType = cmdProvidedType, invokeCode = body2, isStatic = true)
        ]
