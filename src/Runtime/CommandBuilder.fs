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

    override _.ApplyParameterInfo(p, row, _, _) =
        // First update command will be used by ADO CommandBuilder to auto generate all next update commands.
        // Upon statement generation db parameter data will get auto inferred based on parameter **ordinal**, not name!!!
        // Example: System.Data.Common.DbCommandBuilder.BuildUpdateCommand -> CreateParameterForValue
        // Reuse of existing shifted parameter occurs here: System.Data.Common.DbCommandBuilder.GetNextParameter
        // However: Update logic in Command builder will omit columns that are not getting updated, leading to shifting the parameter data
        // Example: if two rows are updated in table Foo (bar1, bar2): if first row got only bar1 modified, and second row only bar2,
        // CommandBuilder will generate updates only for single column, however db parameter type data will be always inferred from bar1 parameter, as it's position is '0'
        // To remove possibility of this behavior, - we force parameter data to be always created from scratch, not reused.
        p.ResetDbType()
        match p, row.[SchemaTableColumn.ProviderType] with
        | :? NpgsqlParameter as param, (:? int as providerType) -> 
            param.NpgsqlDbType <- enum providerType
        | _ -> ()

    override _.GetParameterName(_parameterName: string): string = raise( NotImplementedException())
    override this.GetParameterName parameterOrdinal = 
        if updatingRowNumber > 0 
        then $"@p%i{parameterOrdinal}_%i{updatingRowNumber}"
        else $"@p%i{parameterOrdinal}" 
    override this.GetParameterPlaceholder parameterOrdinal = this.GetParameterName parameterOrdinal
    override _.QuoteIdentifier unquotedIdentifier = npgsql.QuoteIdentifier unquotedIdentifier
    override _.UnquoteIdentifier quotedIdentifier = npgsql.UnquoteIdentifier quotedIdentifier
                
    member private _.SqlRowUpdatingHandler eventArgs = 
        updatingRowNumber <- updatingRowNumber + 1
        base.RowUpdatingHandler(eventArgs)

    override this.SetRowUpdatingHandler adapter = 
        let cleaningUp = (adapter = this.DataAdapter)
        if not cleaningUp
        then rowUpdatingCleanUp <- (adapter :?> BatchDataAdapter).RowUpdating.Subscribe(this.SqlRowUpdatingHandler)
        else rowUpdatingCleanUp.Dispose()

    override _.GetSchemaTable _ = schemaTable

