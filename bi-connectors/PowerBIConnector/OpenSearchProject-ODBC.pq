// This file contains Data Connector logic
[Version = "1.0.0"]
section #"OpenSearchProject-ODBC";

// When set to true, additional trace information will be written out to the User log. 
// This should be set to false before release. Tracing is done through a call to 
// Diagnostics.LogValue(). When EnableTraceOutput is set to false, the call becomes a 
// no-op and simply returns the original value.
EnableTraceOutput = false;

[DataSource.Kind="OpenSearchProject-ODBC", Publish="OpenSearchProject-ODBC.Publish"]
shared #"OpenSearchProject-ODBC.Contents" = Value.ReplaceType(#"OpenSearchProject-ODBCImpl", #"OpenSearchProject-ODBCType");

// Wrapper function to provide additional UI customization.
#"OpenSearchProject-ODBCType" = type function (
        Server as (type text meta [
            Documentation.FieldCaption = "Server",
            Documentation.FieldDescription = "The hostname of the OpenSearch server.",
            Documentation.SampleValues = { "localhost" }
        ]),
        Port as (type number meta [
            Documentation.FieldCaption = "Port",
            Documentation.FieldDescription = "Port which OpenSearch server listens on.",
            Documentation.SampleValues = { 9200 }
        ]),
        UseSSL as (type logical meta [
            Documentation.FieldCaption = "Use SSL",
            Documentation.FieldDescription = "Use SSL",
            Documentation.AllowedValues = { true, false }
        ])
    )
    as table meta [
        Documentation.Name = "OpenSearch Project"
    ];

#"OpenSearchProject-ODBCImpl" = (Server as text, Port as number, UseSSL as logical) as table =>
    let
        Credential = Extension.CurrentCredential(),
        AuthenticationMode = Credential[AuthenticationKind],

        // Sets connection string properties for authentication.
        CredentialConnectionString =
            if AuthenticationMode = "UsernamePassword" then
                [
                    Auth = "BASIC",
                    UID = Credential[Username],
                    PWD = Credential[Password]
                ]
            else if AuthenticationMode = "Key" then
                [
                    Auth = "AWS_SIGV4",
                    Region = Credential[Key]
                ]
            else 
                [
                    Auth = "NONE"
                ],

        // Sets connection string properties for encrypted connections.
        EncryptedConnectionString =
            if Credential[EncryptConnection] = null or Credential[EncryptConnection] = true then
                [
                    UseSSL = 1
                ]
            else
                [
                    UseSSL = 0
                ],

        // Subtract the server from the user input in case it's entered like 'http://localhost' or 'https://srv.com:100500' or 'localhost:0'
        // And build the proper string on our own
        FinalServerString = if UseSSL then
                "https://" & Uri.Parts(Server)[Host] & ":" & Text.From(Port)
            else
                "http://" & Uri.Parts(Server)[Host] & ":" & Text.From(Port),

        ConnectionString = [
            Driver = "OpenSearch SQL ODBC Driver",
            Host = FinalServerString
        ],

        SQLGetInfo = Diagnostics.LogValue("SQLGetInfo_Options", [
            SQL_AGGREGATE_FUNCTIONS = ODBC[SQL_AF][All],
            SQL_SQL_CONFORMANCE = ODBC[SQL_SC][SQL_SC_SQL92_INTERMEDIATE]
        ]),

        SQLGetTypeInfo = (types) => 
            if (EnableTraceOutput <> true) then types else
            let
                // Outputting the entire table might be too large, and result in the value being truncated.
                // We can output a row at a time instead with Table.TransformRows()
                rows = Table.TransformRows(types, each Diagnostics.LogValue("SQLGetTypeInfo " & _[TYPE_NAME], _)),
                toTable = Table.FromRecords(rows)
            in
                Value.ReplaceType(toTable, Value.Type(types)),

        // SQLColumns is a function handler that receives the results of an ODBC call to SQLColumns().
        SQLColumns = (catalogName, schemaName, tableName, columnName, source) =>
            if (EnableTraceOutput <> true) then source else
            // the if statement conditions will force the values to evaluated/written to diagnostics
            if (Diagnostics.LogValue("SQLColumns.TableName", tableName) <> "***" and Diagnostics.LogValue("SQLColumns.ColumnName", columnName) <> "***") then
                let
                    // Outputting the entire table might be too large, and result in the value being truncated.
                    // We can output a row at a time instead with Table.TransformRows()
                    rows = Table.TransformRows(source, each Diagnostics.LogValue("SQLColumns", _)),
                    toTable = Table.FromRecords(rows)
                in
                    Value.ReplaceType(toTable, Value.Type(source))
            else
                source,
        
        SQLGetFunctions = Diagnostics.LogValue("SQLGetFunctions_Options", [
            SQL_API_SQLBINDPARAMETER = false
        ]),

        SqlCapabilities = Diagnostics.LogValue("SqlCapabilities_Options", [
            SupportsTop = false,
            LimitClauseKind = LimitClauseKind.LimitOffset,
            Sql92Conformance = ODBC[SQL_SC][SQL_SC_SQL92_FULL],
            SupportsNumericLiterals = true,
            SupportsStringLiterals = true,
            SupportsOdbcDateLiterals = true,
            SupportsOdbcTimeLiterals = true,
            SupportsOdbcTimestampLiterals = true
        ]),

        OdbcOptions = [
             // Do not view the tables grouped by their schema names.
            HierarchicalNavigation = false,
            // Prevents execution of native SQL statements. Extensions should set this to true.
            HideNativeQuery = true,
            // Allows upconversion of numeric types
            SoftNumbers = true,
            // Allow upconversion / resizing of numeric and string types
            TolerateConcatOverflow = true,
            // Enables connection pooling via the system ODBC manager
            ClientConnectionPooling = true,

            // These values should be set by previous steps
            SQLColumns = SQLColumns,
            SQLGetTypeInfo = SQLGetTypeInfo,
            SQLGetInfo = SQLGetInfo,
            SQLGetFunctions = SQLGetFunctions,
            SqlCapabilities = SqlCapabilities,

            OnError = OnOdbcError,

            // Connection string properties used for encrypted connections.
            CredentialConnectionString = EncryptedConnectionString
        ],

        FullConnectionString = ConnectionString & CredentialConnectionString & EncryptedConnectionString,

        OdbcDatasource = Odbc.DataSource(FullConnectionString, OdbcOptions)
    in
        OdbcDatasource;

// Handles ODBC errors.
OnOdbcError = (errorRecord as record) =>
    let
        ErrorMessage = errorRecord[Message],
        ConnectionServer = errorRecord[Detail][DataSourcePath], 
        
        IsDriverNotInstalled = Text.Contains(ErrorMessage, "doesn't correspond to an installed ODBC driver"),
        
        OdbcError = errorRecord[Detail][OdbcErrors]{0},
        OdbcErrorCode = OdbcError[NativeError],
        
        // Failed to connect to given host
        IsHostUnreachable = 
            OdbcErrorCode = 202
    in
        if IsDriverNotInstalled then
            error Error.Record("DataSource.Error", "The OpenSearch SQL ODBC driver is not installed. Please install the driver")
        else if IsHostUnreachable then 
            error Error.Record("DataSource.Error", "Couldn't reach server. Please double-check the server and auth. [" & ConnectionServer & "]")
        else 
            error errorRecord;

// Data Source Kind description
#"OpenSearchProject-ODBC" = [
    // Required for use with Power BI Service.
    TestConnection = (dataSourcePath) => 
        let
            json = Json.Document(dataSourcePath),
            Server = json[Server],
            Port = json[Port],
            UseSSL = json[UseSSL]
        in
            { "OpenSearchProject-ODBC.Contents", Server, Port, UseSSL },

    // Authentication modes
    Authentication = [
        Implicit = [
            Label = "NONE"
        ],
        UsernamePassword = [
            Label = "BASIC"
        ],
        Key = [
            Label = "AWS_SIGV4",
            KeyLabel = "Region"
        ]
    ],

    // PBIDS Handler
    DSRHandlers = [
      #"opensearchproject-odbc" = [
        GetDSR = (server, schema, object, optional options) => [ protocol = "opensearchproject-odbc", address = [ server = server, port = schema, useSSL = object ] ],
        GetFormula = (dsr, optional options) => () =>
          let
            db = #"OpenSearchProject-ODBC.Contents"(dsr[address][server], dsr[address][port], dsr[address][useSSL])
          in
            db,
        GetFriendlyName = (dsr) => "OpenSearch Project SQL ODBC"
      ]
    ],

    // Enable Encryption
    SupportsEncryption = true,

    Label = Extension.LoadString("DataSourceLabel")
];

// Data Source UI publishing description
#"OpenSearchProject-ODBC.Publish" = [
    Beta = true,
    Category = "Other",
    ButtonText = { Extension.LoadString("ButtonTitle"), Extension.LoadString("ButtonHelp") },
    LearnMoreUrl = "https://github.com/opensearch-project/sql/blob/main/bi-connectors/PowerBIConnector/docs/OpenSearchProject.md",
    
    SupportsDirectQuery = true,

    SourceImage = OpenSearch.Icons,
    SourceTypeImage = OpenSearch.Icons
];

OpenSearch.Icons = [
    Icon16 = { Extension.Contents("OpenSearch16.png"), Extension.Contents("OpenSearch20.png"), Extension.Contents("OpenSearch24.png"), Extension.Contents("OpenSearch32.png") },
    Icon32 = { Extension.Contents("OpenSearch32.png"), Extension.Contents("OpenSearch40.png"), Extension.Contents("OpenSearch48.png"), Extension.Contents("OpenSearch64.png") }
];

// Load common library functions
Extension.LoadFunction = (name as text) =>
    let
        binary = Extension.Contents(name),
        asText = Text.FromBinary(binary)
    in
        Expression.Evaluate(asText, #shared);

// Diagnostics module contains multiple functions. .
Diagnostics = Extension.LoadFunction("Diagnostics.pqm");
Diagnostics.LogValue = if (EnableTraceOutput) then Diagnostics[LogValue] else (prefix, value) => value;

// OdbcConstants contains numeric constants from the ODBC header files, and helper function to create bitfield values.
ODBC = Extension.LoadFunction("OdbcConstants.pqm");
