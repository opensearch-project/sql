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
  **Note:** The **DataFusion Backend** column indicates support on the analytics engine (DataFusion) execution path used for `parquet`/composite-format indices: **Yes** = supported; **Partial** = supported with a known limitation on specific modes or variants (see the command page); **No** = operates on index data but is not supported on the DataFusion path (runs on the default engine, is rejected, or has no plan translation); **N/A** = does not touch the parquet/AE data path by design (planning, metadata, or management commands).
  
| Command Name | Version Introduced | Current Status | DataFusion Backend | Command Description |
| --- | --- | --- | --- | --- |
| [search command](cmd/search.md) | 1.0 | stable (since 1.0) | Partial | Retrieve documents from the index. |
| [where command](cmd/where.md) | 1.0 | stable (since 1.0) | Yes | Filter the search result using boolean expressions. |
| [subquery command](cmd/subquery.md) | 3.0 | experimental (since 3.0) | Yes | Embed one PPL query inside another for complex filtering and data retrieval operations. |
| [fields command](cmd/fields.md) | 1.0 | stable (since 1.0) | Yes | Keep or remove fields from the search result. |
| [format command](cmd/format.md) | 3.8 | experimental (since 3.8) | No | Collapse rows and fields into a single search-expression string. |
| [rename command](cmd/rename.md) | 1.0 | stable (since 1.0) | Yes | Rename one or more fields in the search result. |
| [eval command](cmd/eval.md) | 1.0 | stable (since 1.0) | Yes | Evaluate an expression and append the result to the search result. |
| [foreach command](cmd/foreach.md) | 3.8 | experimental (since 3.8) | No | Run a templated evaluation for each selected field or collection element. |
| [convert command](cmd/convert.md) | 3.5 | experimental (since 3.5) | Partial | Transform field values to numeric values using specialized conversion functions. |
| [replace command](cmd/replace.md) | 3.4 | experimental (since 3.4) | Yes | Replace text in one or more fields in the search result |
| [fillnull command](cmd/fillnull.md) | 3.0 | experimental (since 3.0) | Yes | Fill null with provided value in one or more fields in the search result. |
| [expand command](cmd/expand.md) | 3.1 | experimental (since 3.1) | No | Transform a single document into multiple documents by expanding a nested array field. |
| [flatten command](cmd/flatten.md) | 3.1 | experimental (since 3.1) | No | Flatten a struct or an object field into separate fields in a document. |
| [table command](cmd/table.md) | 3.3 | experimental (since 3.3) | Yes | Keep or remove fields from the search result using enhanced syntax options. |
| [stats command](cmd/stats.md) | 1.0 | stable (since 1.0) | Yes | Calculate aggregation from search results. |
| [eventstats command](cmd/eventstats.md) | 3.1 | experimental (since 3.1) | Yes | Calculate aggregation statistics and add them as new fields to each event. |
| [streamstats command](cmd/streamstats.md) | 3.4 | experimental (since 3.4) | Partial | Calculate cumulative or rolling statistics as events are processed in order. |
| [bin command](cmd/bin.md) | 3.3 | experimental (since 3.3) | Partial | Group numeric values into buckets of equal intervals. |
| [timechart command](cmd/timechart.md) | 3.3 | experimental (since 3.3) | Yes | Create time-based charts and visualizations. |
| [chart command](cmd/chart.md) | 3.4 | experimental (since 3.4) | Yes | Apply statistical aggregations to search results and group the data for visualizations. |
| [trendline command](cmd/trendline.md) | 3.0 | experimental (since 3.0) | Yes | Calculate moving averages of fields. |
| [sort command](cmd/sort.md) | 1.0 | stable (since 1.0) | Yes | Sort all the search results by the specified fields. |
| [reverse command](cmd/reverse.md) | 3.2 | experimental (since 3.2) | Yes | Reverse the display order of search results. |
| [head command](cmd/head.md) | 1.0 | stable (since 1.0) | Yes | Return the first N number of specified results after an optional offset in search order. |
| [dedup command](cmd/dedup.md) | 1.0 | stable (since 1.0) | Yes | Remove identical documents defined by the field from the search result. |
| [top command](cmd/top.md) | 1.0 | stable (since 1.0) | Yes | Find the most common tuple of values of all fields in the field list. |
| [rare command](cmd/rare.md) | 1.0 | stable (since 1.0) | Yes | Find the least common tuple of values of all fields in the field list. |
| [parse command](cmd/parse.md) | 1.3 | stable (since 1.3) | Partial | Parse a text field with a regular expression and append the result to the search result. |
| [grok command](cmd/grok.md) | 2.4 | stable (since 2.4) | Yes | Parse a text field with a grok pattern and append the results to the search result. |
| [rex command](cmd/rex.md) | 3.3 | experimental (since 3.3) | Yes | Extract fields from a raw text field using regular expression named capture groups. |
| [regex command](cmd/regex.md) | 3.3 | experimental (since 3.3) | Yes | Filter search results by matching field values against a regular expression pattern. |
| [spath command](cmd/spath.md) | 3.3 | experimental (since 3.3) | Yes | Extract fields from structured text data. |
| [patterns command](cmd/patterns.md) | 2.4 | stable (since 2.4) | Partial | Extract log patterns from a text field and append the results to the search result. |
| [join command](cmd/join.md) | 3.0 | stable (since 3.0) | Yes | Combine two datasets together. |
| [append command](cmd/append.md) | 3.3 | experimental (since 3.3) | Yes | Append the result of a sub-search to the bottom of the input search results. |
| [appendcol command](cmd/appendcol.md) | 3.1 | experimental (since 3.1) | Yes | Append the result of a sub-search and attach it alongside the input search results. |
| [lookup command](cmd/lookup.md) | 3.0 | experimental (since 3.0) | Partial | Add or replace data from a lookup index. |
| [multisearch command](cmd/multisearch.md) | 3.4 | experimental (since 3.4) | Partial | Execute multiple search queries and combine their results. |
| [union command](cmd/union.md) | 3.7 | experimental (since 3.7) | Yes | Combine results from multiple datasets using UNION ALL semantics. |
| [rest command](cmd/rest.md) | 3.9 | experimental (since 3.9) | N/A | Read an allow-listed, read-only in-cluster management endpoint (cluster/cat/nodes) as rows. Calcite engine only. |
| [ml command](cmd/ml.md) | 2.5 | stable (since 2.5) | No | Apply machine learning algorithms to analyze data. |
| [kmeans command](cmd/kmeans.md) | 1.3 | stable (since 1.3) | No | Apply the kmeans algorithm on the search result returned by a PPL command. |
| [ad command](cmd/ad.md) | 1.3 | deprecated (since 2.5) | No | Apply Random Cut Forest algorithm on the search result returned by a PPL command. |
| [describe command](cmd/describe.md) | 2.1 | stable (since 2.1) | N/A | Query the metadata of an index. |
| [explain command](cmd/explain.md) | 3.1 | stable (since 3.1) | N/A | Explain the plan of query. |
| [show datasources command](cmd/showdatasources.md) | 2.4 | stable (since 2.4) | N/A | Query datasources configured in the PPL engine. |
| [makeresults command](cmd/makeresults.md) | 3.8 | experimental (since 3.8) | No | Generate in-memory rows for testing and seeding, optionally from inline CSV/JSON data. |
| [addtotals command](cmd/addtotals.md) | 3.5 | stable (since 3.5) | Yes | Adds row and column values and appends a totals column and row. |
| [addcoltotals command](cmd/addcoltotals.md) | 3.5 | stable (since 3.5) | Yes | Adds column values and appends a totals row. |
| [transpose command](cmd/transpose.md) | 3.5 | stable (since 3.5) | Yes | Transpose rows to columns. |
| [mvcombine command](cmd/mvcombine.md) | 3.5 | stable (since 3.4) | No | Combines values of a specified field across rows identical on all other fields. |
| [nomv command](cmd/nomv.md) | 3.6 | stable (since 3.6) | No | Converts a multivalue field to a single-value string by joining elements with newlines. |
| [mvexpand command](cmd/mvexpand.md) | 3.6                | stable (since 3.6)       | No | Expand a multi-valued field into separate documents (one per value). |
| [graphlookup command](cmd/graphlookup.md) | 3.6 | experimental (since 3.6) | No | Performs recursive graph traversal on a collection using a BFS algorithm.|
| [xyseries command](cmd/xyseries.md) | 3.8 | experimental (since 3.8) | Partial | Converts row-oriented grouped results into a wide table format suitable for chart visualizations. One field serves as the X axis (row key), one field provides pivot values for generating output column names, and one or more data fields fill the pivoted cells. Only rows matching the explicitly provided pivot values in the `in` clause are included. |

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
