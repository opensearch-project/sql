#  Connecting OpenSearch Project to Microsoft Power BI Desktop

>[!Note]

>The following connector article is provided by OpenSearch, the owner of this connector and a member of the Microsoft Power Query Connector Certification Program. If you have questions regarding the content of this article or have changes you would like to see made to this article, visit the OpenSearch website and use the support channels there.

## Summary
| Item | Description |
| ---- | ----------- |
| Release State | General Availability |
| Products | Power BI Desktop |
| Authentication Types Supported | Basic |

## Prerequisites
* Microsoft Power BI Desktop
* [OpenSearch](https://opensearch.org/docs/latest/opensearch/install/index/)
* [OpenSearch SQL ODBC driver](https://opensearch.org/docs/latest/search-plugins/sql/odbc/)

## Capabilities supported
* Import
* DirectQuery

## Connect to OpenSearch Project
1. Open Power BI Desktop.

2. Click on **Home** > **Get Data** > **More** > **Other**. Select **OpenSearch Project**. Click on **Connect**.

<img src="img/pbi_select_connector.png" width="500">

3. You will get a warning for using a third-party service. Click on **Continue**.

<img src="img/pbi_third_party_warning.png" width="500">

4. Enter host and port values and select your preferred SSL and Certificate validation options. Click on **OK**.

<img src="img/pbi_connection_string_options.png" width="500">

5. Select authentication option. Enter credentials if required and click on **Connect**.

<img src="img/pbi_auth.png" width="500">

6. Select required table. Data preview will be loaded.

<img src="img/pbi_data_preview.png">

7. Click on **Load**.

8. Select required columns for creating a graph.

<img src="img/pbi_simple_graph.png">


## Troubleshooting 

* If you get the following error, please install the [OpenSearch SQL ODBC Driver](https://docs-beta.opensearch.org/search-plugins/sql/odbc/).

<img src="img/pbi_error_driver_not_installed.png" width="350">

* If you get the following error,

<img src="img/pbi_error_conn.png" width="350">

1. Check if host and port values are correct.
2. Check if auth credentials are correct.
3. Check if server is running.

## Limitations and Known issues

There are known limitations and issues that are tracked by OpenSearch including the items listed below.

| Issue | Description |
| ---- | ----------- |
| [Visualizations without numerical columns](https://github.com/opensearch-project/sql/issues/347) | Visualizations is only supported if there is a numerical column included due to a known limitation in OpenSearch regarding subqueries. An exception to this would be visualizations with columns only of type string is supported as well. |
| [Visualizations with First or Last options selected are not yet supported](https://github.com/opensearch-project/sql/issues/279) | First and Last options generate an sql query that uses MAX and MIN for strings which is not yet supported in OpenSearch |
| [Visualizations with aggregate functions selected are not yet supported](https://github.com/opensearch-project/sql/issues/363) | Specifically Minimum, Maximum, Standard deviation, Variance, or Median options are not yet supported. |
| [Basic filtering limitations](https://github.com/opensearch-project/sql/issues/347) | Selecting more than one value is not yet supported when using basic filtering. Due to the issue in the first row of this table, selecting one value for basic filtering when there is no numerical column is not yet supported. |
| [Top N filtering limitations](https://opensearch.org/docs/latest/search-plugins/sql/limitation/) | OpenSearch has limitations on subquery which does not yet support the Top N filtering functionality. |
| [Advanced filtering limitations](https://github.com/opensearch-project/sql/issues/308) | `does not contain` and `does not start with` filters for string columns are not yet supported. All advanced filtering for numerical columns are not yet supported except for `is` and `is blank`. All advanced filtering for date and time columns are not yet supported except for `is blank` and `is not blank`. |
| [Relative Date filtering limitations](https://github.com/opensearch-project/sql/issues/364) | Due to a known timestamp issue in OpenSearch, all relative date filtering is not yet supported. |
