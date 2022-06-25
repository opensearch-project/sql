## Connector Download

The most recent build could be donwloaded from an automated CI workflow: [link](https://github.com/opensearch-project/sql/actions/workflows/bi-connectors.yml).
The release snapshot is also available [here](opensearch_sql_jdbc.taco).

## Connector Install

1. Put connector `taco` file into
  * Windows: `C:\Users\%USERNAME%\Documents\My Tableau Repository\Connectors`;
  * MacOS: `~/Documents/My Tableau Repository/Connectors`.
2. Put OpenSearch `JDBC` [driver](../../sql-jdbc/README.md) (`jar` file) into
  * Windows: `C:\Program Files\Tableau\Drivers`;
  * MacOS: `~/Library/Tableau/Drivers`.
3. Run `Tableau Desktop` with command line flag `-DDisableVerifyConnectorPluginSignature=true`. You can create a shortcut or a script to simplify this step.

## TDVT report for OpenSearch JDBC Tableau connector

Each Tableau connector has to be tested and verified using [TDVT](https://tableau.github.io/connector-plugin-sdk/docs/tdvt).

Most recent tests of the connector were performed on OpenSearch v.1.2.0 with SQL plugin v.1.2.0.

TDVT test results are available in [tdvt_test_results.csv](tdvt_test_results.csv).

Test pass rate is 669/837 (80%).
