namespace FSharp.Data.Npgsql

open System.Data
open System.Data.Common
open Npgsql

type internal CommandBuilder(source: DataTable<DataRow>) = 
    inherit DbCommandBuilder(QuotePrefix = "\"", QuoteSuffix = "\"", SchemaSeparator = ".") 

    let npgsql = new NpgsqlCommandBuilder()  
    let rowUpdatingCleanUp = ref null

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

    override __.GetParameterName parameterName = sprintf "@%s" parameterName
    override __.GetParameterName parameterOrdinal = sprintf "@p%i" parameterOrdinal
    override this.GetParameterPlaceholder parameterOrdinal = this.GetParameterName(parameterOrdinal)
    override __.QuoteIdentifier unquotedIdentifier = npgsql.QuoteIdentifier unquotedIdentifier
    override __.UnquoteIdentifier quotedIdentifier = npgsql.UnquoteIdentifier quotedIdentifier
                
    member private __.SqlRowUpdatingHandler eventArgs = base.RowUpdatingHandler(eventArgs)

    override this.SetRowUpdatingHandler adapter = 
        if (adapter <> this.DataAdapter)
        then
            rowUpdatingCleanUp := (adapter :?> NpgsqlDataAdapter).RowUpdating.Subscribe(this.SqlRowUpdatingHandler)
        else
            rowUpdatingCleanUp.Value.Dispose()

    override __.GetSchemaTable _ = schemaTable

