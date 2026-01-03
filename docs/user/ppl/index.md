# OpenSearch PPL Reference Manual  

### Overview  

Piped Processing Language (PPL), powered by OpenSearch, enables OpenSearch users with exploration and discovery of, and finding search patterns in data stored in OpenSearch, using a set of commands delimited by pipes (\|). These are essentially read-only requests to process data and return results.  

Currently, OpenSearch users can query data using either Query DSL or SQL. Query DSL is powerful and fast. However, it has a steep learning curve, and was not designed as a human interface to easily create ad hoc queries and explore user data. SQL allows users to extract and analyze data in OpenSearch in a declarative manner. OpenSearch now makes its search and query engine robust by introducing Piped Processing Language (PPL). It enables users to extract insights from OpenSearch with a sequence of commands delimited by pipes (\|). It supports  a comprehensive set of commands including search, where, fields, rename, dedup, sort, eval, head, top and rare, and functions, operators and expressions. Even new users who have recently adopted OpenSearch, can be productive day one, if they are familiar with the pipe (\|) syntax. It enables developers, DevOps engineers, support engineers, site reliability engineers (SREs), and IT managers to effectively discover and explore log, monitoring and observability data stored in OpenSearch.  

We expand the capabilities of our Workbench, a comprehensive and integrated visual query tool currently supporting only SQL, to run on-demand PPL commands, and view and save results as text and JSON. We also add  a new interactive standalone command line tool, the PPL CLI, to run on-demand PPL commands, and view and save results as text and JSON.
The query start with search command and then flowing a set of command delimited by pipe (\|).  

for example, the following query retrieve firstname and lastname from accounts if age large than 18. 
  
```ppl ignore
source=accounts
| where age > 18
| fields firstname, lastname
```
  
* **Interfaces**  
  - [Endpoint](interfaces/endpoint.md)  
  - [Protocol](interfaces/protocol.md)  
* **Administration**  
  - [Plugin Settings](admin/settings.md)  
  - [Security Settings](admin/security.md)  
  - [Monitoring](admin/monitoring.md)  
  - [Datasource Settings](admin/datasources.md)  
  - [Prometheus Connector](admin/connectors/prometheus_connector.md)  
  - [Cross-Cluster Search](admin/cross_cluster_search.md)  
* **Language Structure**  
  - [Identifiers](general/identifiers.md)  
  - [Data Types](general/datatypes.md)  
* **Commands**  
  
  The following commands are available in PPL:  
  **Note:** Experimental commands are ready for use, but specific parameters may change based on feedback.
  
| Command Name | Version Introduced | Current Status | Command Description |
| --- | --- | --- | --- |
| [search command](cmd/search.md) | 1.0 | stable (since 1.0) | Retrieve documents from the index. |
| [where command](cmd/where.md) | 1.0 | stable (since 1.0) | Filter the search result using boolean expressions. |
| [subquery command](cmd/subquery.md) | 3.0 | experimental (since 3.0) | Embed one PPL query inside another for complex filtering and data retrieval operations. |
| [fields command](cmd/fields.md) | 1.0 | stable (since 1.0) | Keep or remove fields from the search result. |
| [rename command](cmd/rename.md) | 1.0 | stable (since 1.0) | Rename one or more fields in the search result. |
| [eval command](cmd/eval.md) | 1.0 | stable (since 1.0) | Evaluate an expression and append the result to the search result. |
| [replace command](cmd/replace.md) | 3.4 | experimental (since 3.4) | Replace text in one or more fields in the search result |
| [fillnull command](cmd/fillnull.md) | 3.0 | experimental (since 3.0) | Fill null with provided value in one or more fields in the search result. |
| [expand command](cmd/expand.md) | 3.1 | experimental (since 3.1) | Transform a single document into multiple documents by expanding a nested array field. |
| [flatten command](cmd/flatten.md) | 3.1 | experimental (since 3.1) | Flatten a struct or an object field into separate fields in a document. |
| [table command](cmd/table.md) | 3.3 | experimental (since 3.3) | Keep or remove fields from the search result using enhanced syntax options. |
| [stats command](cmd/stats.md) | 1.0 | stable (since 1.0) | Calculate aggregation from search results. |
| [eventstats command](cmd/eventstats.md) | 3.1 | experimental (since 3.1) | Calculate aggregation statistics and add them as new fields to each event. |
| [streamstats command](cmd/streamstats.md) | 3.4 | experimental (since 3.4) | Calculate cumulative or rolling statistics as events are processed in order. |
| [bin command](cmd/bin.md) | 3.3 | experimental (since 3.3) | Group numeric values into buckets of equal intervals. |
| [timechart command](cmd/timechart.md) | 3.3 | experimental (since 3.3) | Create time-based charts and visualizations. |
| [chart command](cmd/chart.md) | 3.4 | experimental (since 3.4) | Apply statistical aggregations to search results and group the data for visualizations. |
| [trendline command](cmd/trendline.md) | 3.0 | experimental (since 3.0) | Calculate moving averages of fields. |
| [sort command](cmd/sort.md) | 1.0 | stable (since 1.0) | Sort all the search results by the specified fields. |
| [reverse command](cmd/reverse.md) | 3.2 | experimental (since 3.2) | Reverse the display order of search results. |
| [head command](cmd/head.md) | 1.0 | stable (since 1.0) | Return the first N number of specified results after an optional offset in search order. |
| [dedup command](cmd/dedup.md) | 1.0 | stable (since 1.0) | Remove identical documents defined by the field from the search result. |
| [top command](cmd/top.md) | 1.0 | stable (since 1.0) | Find the most common tuple of values of all fields in the field list. |
| [rare command](cmd/rare.md) | 1.0 | stable (since 1.0) | Find the least common tuple of values of all fields in the field list. |
| [parse command](cmd/parse.md) | 1.3 | stable (since 1.3) | Parse a text field with a regular expression and append the result to the search result. |
| [grok command](cmd/grok.md) | 2.4 | stable (since 2.4) | Parse a text field with a grok pattern and append the results to the search result. |
| [rex command](cmd/rex.md) | 3.3 | experimental (since 3.3) | Extract fields from a raw text field using regular expression named capture groups. |
| [regex command](cmd/regex.md) | 3.3 | experimental (since 3.3) | Filter search results by matching field values against a regular expression pattern. |
| [spath command](cmd/spath.md) | 3.3 | experimental (since 3.3) | Extract fields from structured text data. |
| [patterns command](cmd/patterns.md) | 2.4 | stable (since 2.4) | Extract log patterns from a text field and append the results to the search result. |
| [join command](cmd/join.md) | 3.0 | stable (since 3.0) | Combine two datasets together. |
| [append command](cmd/append.md) | 3.3 | experimental (since 3.3) | Append the result of a sub-search to the bottom of the input search results. |
| [appendcol command](cmd/appendcol.md) | 3.1 | experimental (since 3.1) | Append the result of a sub-search and attach it alongside the input search results. |
| [lookup command](cmd/lookup.md) | 3.0 | experimental (since 3.0) | Add or replace data from a lookup index. |
| [multisearch command](cmd/multisearch.md) | 3.4 | experimental (since 3.4) | Execute multiple search queries and combine their results. |
| [ml command](cmd/ml.md) | 2.5 | stable (since 2.5) | Apply machine learning algorithms to analyze data. |
| [kmeans command](cmd/kmeans.md) | 1.3 | stable (since 1.3) | Apply the kmeans algorithm on the search result returned by a PPL command. |
| [ad command](cmd/ad.md) | 1.3 | deprecated (since 2.5) | Apply Random Cut Forest algorithm on the search result returned by a PPL command. |
| [describe command](cmd/describe.md) | 2.1 | stable (since 2.1) | Query the metadata of an index. |
| [explain command](cmd/explain.md) | 3.1 | stable (since 3.1) | Explain the plan of query. |
| [show datasources command](cmd/showdatasources.md) | 2.4 | stable (since 2.4) | Query datasources configured in the PPL engine. |
| [addtotals command](cmd/addtotals.md) | 3.5 | stable (since 3.5) | Adds row and column values and appends a totals column and row. | 
| [addcoltotals command](cmd/addcoltotals.md) | 3.5 | stable (since 3.5) | Adds column values and appends a totals row. |
| [transpose command](cmd/transpose.md) | 3.5 | stable (since 3.5) | Transpose rows to columns. |
  
  - [Syntax](cmd/syntax.md) - PPL query structure and command syntax formatting  
* **Functions**  
  - [Aggregation Functions](functions/aggregations.md)  
  - [Collection Functions](functions/collection.md)  
  - [Condition Functions](functions/condition.md)  
  - [Cryptographic Functions](functions/cryptographic.md)  
  - [Date and Time Functions](functions/datetime.md)  
  - [Expressions](functions/expressions.md)  
  - [IP Address Functions](functions/ip.md)  
  - [JSON Functions](functions/json.md)  
  - [Math Functions](functions/math.md)  
  - [Relevance Functions](functions/relevance.md)  
  - [String Functions](functions/string.md)  
  - [System Functions](functions/system.md)  
  - [Type Conversion Functions](functions/conversion.md)  
* **Optimization**  
  - [Optimization](../../user/optimization/optimization.rst)  
* **Limitations**  
  - [Limitations](limitations/limitations.md)  