# Connecting OpenSearch to Tableau Desktop via the Tableau Connector

## Overview

Connect an OpenSearch data source to Tableau Desktop via the Tableau Connector to create a basic graph.

## Prerequisites

* Download and Install [Tableau Desktop](https://www.tableau.com/products/desktop/download) 2020 and higher
* Install and Configure [OpenSearch](https://docs-beta.opensearch.org/opensearch/install/index/)
* Download and Install [OpenSearch SQL JDBC driver](../../sql-jdbc/README.md)
* Download Tableau Connector ([opensearch_sql_jdbc.taco](opensearch_sql_jdbc.taco)).
Click on **Download** option for downloading `opensearch_sql_jdbc.taco` file.
<img src="img/tableau_download_taco.png" >

## Prepare data 

* Copy `opensearch_sql_jdbc.taco` file to My Tableau Repository.

  * On windows: **%User%/Documents/My Tableau Repository/Connectors/**.
  * On Mac: **~/Documents/My Tableau Repository/Connectors/**.

* Open Tableau using following command

```
<full-Tableau-path>\bin\tableau.exe -DDisableVerifyConnectorPluginSignature=true
```

* Click on **Connect** > **More** > **OpenSearch by OpenSearch Project**.

<img src="img/tableau_select_connector.png" width=600>

* Enter **Server** & **Port** value. 
* Select required authentication option. For **AWS_SIGV4** authentication, select **AWS_SIGv4** and enter value for **Region**. You need to create `~/.aws/credentials` and add `default` profile with aws access key id and secret key.
* Use **Additional Options** section for specifying options like **FetchSize**, **ResponseTimeout**. Use `;` to separate values. For example,

```
FetchSize=2000;ResponseTimeout=20;
```

<img src="img/tableau_dialog.png" width=400>

* Click on **Sign In**.
* You will get a list of tables when the connection is successful.

<img src="img/tableau_table_list.png">

## Analyze Data

To generate a graph,

* Double click on any required table from the list and click on **Update Now** to load data preview.

<img src="img/tableau_select_table.png">

* Data preview will be loaded.

<img src="img/tableau_data_preview.png">

* Click on **Sheet 1**. You can see a list of attributes under section **Data**.

<img src="img/tableau_columns_list.png">

* Double click on any required attributes to generate a simple graph. 

<img src="img/tableau_graph.png">

* You can change visualizations by selecting any active visualization from **Show Me**.
