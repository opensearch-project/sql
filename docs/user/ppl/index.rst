
===============================
OpenSearch PPL Reference Manual
===============================

Overview
---------
Piped Processing Language (PPL), powered by OpenSearch, enables OpenSearch users with exploration and discovery of, and finding search patterns in data stored in OpenSearch, using a set of commands delimited by pipes (|). These are essentially read-only requests to process data and return results.

Currently, OpenSearch users can query data using either Query DSL or SQL. Query DSL is powerful and fast. However, it has a steep learning curve, and was not designed as a human interface to easily create ad hoc queries and explore user data. SQL allows users to extract and analyze data in OpenSearch in a declarative manner. OpenSearch now makes its search and query engine robust by introducing Piped Processing Language (PPL). It enables users to extract insights from OpenSearch with a sequence of commands delimited by pipes (|). It supports  a comprehensive set of commands including search, where, fields, rename, dedup, sort, eval, head, top and rare, and functions, operators and expressions. Even new users who have recently adopted OpenSearch, can be productive day one, if they are familiar with the pipe (|) syntax. It enables developers, DevOps engineers, support engineers, site reliability engineers (SREs), and IT managers to effectively discover and explore log, monitoring and observability data stored in OpenSearch.

We expand the capabilities of our Workbench, a comprehensive and integrated visual query tool currently supporting only SQL, to run on-demand PPL commands, and view and save results as text and JSON. We also add  a new interactive standalone command line tool, the PPL CLI, to run on-demand PPL commands, and view and save results as text and JSON.

The query start with search command and then flowing a set of command delimited by pipe (|).
| for example, the following query retrieve firstname and lastname from accounts if age large than 18.

.. code-block::

   source=accounts
   | where age > 18
   | fields firstname, lastname

* **Interfaces**

  - `Endpoint <interfaces/endpoint.rst>`_

  - `Protocol <interfaces/protocol.rst>`_

* **Administration**

  - `Plugin Settings <admin/settings.rst>`_

  - `Security Settings <admin/security.rst>`_

  - `Monitoring <admin/monitoring.rst>`_

  - `Datasource Settings <admin/datasources.rst>`_

  - `Prometheus Connector <admin/connectors/prometheus_connector.rst>`_

  - `Cross-Cluster Search <admin/cross_cluster_search.rst>`_

* **Language Structure**

  - `Identifiers <general/identifiers.rst>`_

  - `Data Types <general/datatypes.rst>`_

* **Commands**

  - `Syntax <cmd/syntax.rst>`_

  - `ad command <cmd/ad.rst>`_ (1.3+, deprecated): Apply Random Cut Forest algorithm on the search result returned by a PPL command.

  - `append command <cmd/append.rst>`_ (3.3+, experimental): Append the result of a sub-search to the bottom of the input search results.

  - `appendcol command <cmd/appendcol.rst>`_ (3.1+, experimental): Append the result of a sub-search and attach it alongside the input search results.

  - `bin command <cmd/bin.rst>`_ (3.3+, experimental): Group numeric values into buckets of equal intervals.

  - `dedup command <cmd/dedup.rst>`_ (1.0+, stable): Remove identical documents defined by the field from the search result.

  - `describe command <cmd/describe.rst>`_ (2.1+, stable): Query the metadata of an index.

  - `eval command <cmd/eval.rst>`_ (1.0+, stable): Evaluate an expression and append the result to the search result.

  - `eventstats command <cmd/eventstats.rst>`_ (3.1+, experimental): Calculate aggregation statistics and add them as new fields to each event.

  - `expand command <cmd/expand.rst>`_ (3.1+, experimental): Transform a single document into multiple documents by expanding a nested array field.

  - `explain command <cmd/explain.rst>`_ (3.1+, stable): Explain the plan of query.

  - `fields command <cmd/fields.rst>`_ (1.0+, stable): Keep or remove fields from the search result.

  - `fillnull command <cmd/fillnull.rst>`_ (3.0+, experimental): Fill null with provided value in one or more fields in the search result.

  - `flatten command  <cmd/flatten.rst>`_ (3.1+, experimental): Flatten a struct or an object field into separate fields in a document.

  - `grok command <cmd/grok.rst>`_ (2.4+, stable): Parse a text field with a grok pattern and append the results to the search result.

  - `head command <cmd/head.rst>`_ (1.0+, stable): Return the first N number of specified results after an optional offset in search order.

  - `join command  <cmd/join.rst>`_ (3.0+, stable): Combine two datasets together.

  - `kmeans command <cmd/kmeans.rst>`_ (1.3+, stable): Apply the kmeans algorithm on the search result returned by a PPL command.

  - `lookup command <cmd/lookup.rst>`_ (3.0, experimental): Add or replace data from a lookup index (dimension table).

  - `ml command <cmd/ml.rst>`_: Apply machine learning algorithms to analyze data.

  - `multisearch command <cmd/multisearch.rst>`_ (3.4+, experimental): Execute multiple search queries and combine their results.

  - `parse command <cmd/parse.rst>`_ (1.3+, stable): Parse a text field with a regular expression and append the result to the search result.

  - `patterns command <cmd/patterns.rst>`_ (2.4+, stable): Extract log patterns from a text field and append the results to the search result.

  - `rare command <cmd/rare.rst>`_ (1.0+, stable): Find the least common tuple of values of all fields in the field list.

  - `regex command <cmd/regex.rst>`_ (3.3+, experimental): Filter search results by matching field values against a regular expression pattern.

  - `rename command <cmd/rename.rst>`_ (1.0+, stable): Rename one or more fields in the search result.

  - `reverse command <cmd/reverse.rst>`_ (3.2+, experimental): Reverse the display order of search results.

  - `rex command <cmd/rex.rst>`_ (3.3+, experimental): Extract fields from a raw text field using regular expression named capture groups.

  - `search command <cmd/search.rst>`_ (1.0+, stable): Retrieve documents from the index.

  - `show datasources command <cmd/showdatasources.rst>`_ (2.4+, stable): Query datasources configured in the PPL engine.

  - `sort command <cmd/sort.rst>`_ (1.0+, stable): Sort all the search results by the specified fields.

  - `spath command <cmd/spath.rst>`_ (3.3+, experimental): Extract fields from structured text data.

  - `stats command <cmd/stats.rst>`_ (1.0+, stable): Calculate aggregation from search results.

  - `subquery command <cmd/subquery.rst>`_ (3.0, experimental): Embed one PPL query inside another for complex filtering and data retrieval operations.

  - `table command <cmd/table.rst>`_ (3.3+, experimental): Keep or remove fields from the search result using enhanced syntax options.

  - `timechart command <cmd/timechart.rst>`_ (3.3+, experimental): Create time-based charts and visualizations.

  - `top command <cmd/top.rst>`_ (1.0+, stable): Find the most common tuple of values of all fields in the field list.

  - `trendline command <cmd/trendline.rst>`_ (3.0+, experimental): Calculate moving averages of fields.

  - `where command <cmd/where.rst>`_ (1.0+, stable): Filter the search result using boolean expressions.

* **Functions**

  - `Aggregation Functions <functions/aggregation.rst>`_

  - `Collection Functions <functions/collection.rst>`_

  - `Condition Functions <functions/condition.rst>`_

  - `Cryptographic Functions <functions/cryptographic.rst>`_

  - `Date and Time Functions <functions/datetime.rst>`_

  - `Expressions <functions/expressions.rst>`_

  - `IP Address Functions <functions/ip.rst>`_

  - `JSON Functions <functions/json.rst>`_

  - `Math Functions <functions/math.rst>`_

  - `Relevance Functions <functions/relevance.rst>`_

  - `String Functions <functions/string.rst>`_

  - `System Functions <functions/system.rst>`_

  - `Type Conversion Functions <functions/conversion.rst>`_

* **Optimization**

  - `Optimization <../../user/optimization/optimization.rst>`_

* **Limitations**

  - `Limitations <limitations/limitations.rst>`_
