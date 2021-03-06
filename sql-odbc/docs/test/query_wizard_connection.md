## Query Wizard Connection

* Open blank workbook in Microsoft Excel.
* Click on **Data** > **Get Data** > **From Other Sources** > **From Microsoft Query**

<img src="img/select_microsoft_query.png" width="400">

* Select **Databases** > **OpenSearch SQL ODBC DSN**. 
* Ensure the **Use the Query Wizard to create/edit queries** check box is selected, and then click **OK**.

<img src="img/query_wizard_enable_use_the_query_wizard_option.png" width="400">

* You will see list of available tables & columns. Select required tables/columns and click on **>**. 
* After selecting all required columns, Click on **Next**.

<img src="img/query_wizard_select_tables.png" width="500">

* Specify conditions to apply filter if needed. Ensure selected operations are supported by OpenSearch. Click on **Next**.

<img src="img/query_wizard_filter_data.png" width="500">

* Specify sorting options if required. Ensure selected operations are supported by the [OpenSearch SQL plugin](https://github.com/opensearch-project/sql). Click on **Next**.

<img src="img/query_wizard_sort_order.png" width="500">

* Select **Return Data to Microsoft Excel** and click on **Finish**.

<img src="img/query_wizard_finish.png" width="500">

*  Select worksheet and click on **OK**.

<img src="img/query_wizard_import_data.png" width="300">

* Data will be loaded in the spreadsheet

<img src="img/query_wizard_loaded_data.png">
