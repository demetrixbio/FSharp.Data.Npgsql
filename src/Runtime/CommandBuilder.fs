namespace FSharp.Data.Npgsql

open System.Data
open System.Data.Common
open Npgsql
open System

type internal CommandBuilder(source: DataTable<DataRow>) = 
    inherit DbCommandBuilder(QuotePrefix = "\"", QuoteSuffix = "\"", SchemaSeparator = ".") 

    let npgsql = new NpgsqlCommandBuilder()  
    let mutable rowUpdatingCleanUp = null
    let mutable updatingRowNumber = -1

    let schemaTable = 
        use reader = new DataTableReader(source)
        let schema = reader.GetSchemaTable()

        for row in schema.Rows do   
            let col = source.Columns.[string row.[SchemaTableColumn.ColumnName]]
            let xprop = col.ExtendedProperties
            assert(xprop.Count = 4 || xprop.Count = 5 (* for enums SchemaTableColumn.ProviderType set to NpgsqlDbType.Unknown*))
            for k in xprop.Keys do
                row.[string k] <- xprop.[k]

        schema

    override __.ApplyParameterInfo(p, row, _, _) = 
        match p, row.[SchemaTableColumn.ProviderType] with
        | (:? NpgsqlParameter as param), (:? int as providerType) -> 
            param.NpgsqlDbType <- enum providerType
        | _ -> ()

    override __.GetParameterName(parameterName: string): string = raise( NotImplementedException())
    override this.GetParameterName parameterOrdinal = 
        if updatingRowNumber > 0 
        then sprintf "@p%i_%i" parameterOrdinal updatingRowNumber
        else sprintf "@p%i" parameterOrdinal 
    override this.GetParameterPlaceholder parameterOrdinal = this.GetParameterName parameterOrdinal
    override __.QuoteIdentifier unquotedIdentifier = npgsql.QuoteIdentifier unquotedIdentifier
    override __.UnquoteIdentifier quotedIdentifier = npgsql.UnquoteIdentifier quotedIdentifier
                
    member private __.SqlRowUpdatingHandler eventArgs = 
        updatingRowNumber <- updatingRowNumber + 1
        base.RowUpdatingHandler(eventArgs)

    override this.SetRowUpdatingHandler adapter = 
        let cleaningUp = (adapter = this.DataAdapter)
        if not cleaningUp
        then rowUpdatingCleanUp <- (adapter :?> BatchDataAdapter).RowUpdating.Subscribe(this.SqlRowUpdatingHandler)
        else rowUpdatingCleanUp.Dispose()

    override __.GetSchemaTable _ = schemaTable

