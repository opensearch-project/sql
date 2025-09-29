
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

  - `ad command <cmd/ad.rst>`_

  - `append command <cmd/append.rst>`_

  - `appendcol command <cmd/appendcol.rst>`_

  - `bin command <cmd/bin.rst>`_

  - `dedup command <cmd/dedup.rst>`_

  - `describe command <cmd/describe.rst>`_

  - `eval command <cmd/eval.rst>`_

  - `eventstats command <cmd/eventstats.rst>`_

  - `expand command <cmd/expand.rst>`_

  - `explain command <cmd/explain.rst>`_

  - `fields command <cmd/fields.rst>`_

  - `fillnull command <cmd/fillnull.rst>`_

  - `flatten command  <cmd/flatten.rst>`_

  - `grok command <cmd/grok.rst>`_

  - `head command <cmd/head.rst>`_
  
  - `join command  <cmd/join.rst>`_

  - `kmeans command <cmd/kmeans.rst>`_

  - `lookup command <cmd/lookup.rst>`_

  - `ml command <cmd/ml.rst>`_

  - `parse command <cmd/parse.rst>`_

  - `patterns command <cmd/patterns.rst>`_

  - `rare command <cmd/rare.rst>`_

  - `rename command <cmd/rename.rst>`_

  - `regex command <cmd/regex.rst>`_

  - `rex command <cmd/rex.rst>`_

  - `search command <cmd/search.rst>`_

  - `show datasources command <cmd/showdatasources.rst>`_

  - `sort command <cmd/sort.rst>`_

  - `spath command <cmd/spath.rst>`_

  - `stats command <cmd/stats.rst>`_

  - `subquery (aka subsearch) command <cmd/subquery.rst>`_

  - `reverse command <cmd/reverse.rst>`_

  - `table command <cmd/table.rst>`_
  
  - `timechart command <cmd/timechart.rst>`_

  - `top command <cmd/top.rst>`_

  - `trendline command <cmd/trendline.rst>`_

  - `where command <cmd/where.rst>`_

* **Functions**

  - `Expressions <functions/expressions.rst>`_

  - `Math Functions <functions/math.rst>`_

  - `Date and Time Functions <functions/datetime.rst>`_

  - `String Functions <functions/string.rst>`_

  - `Condition Functions <functions/condition.rst>`_

  - `Relevance Functions <functions/relevance.rst>`_

  - `Type Conversion Functions <functions/conversion.rst>`_

  - `System Functions <functions/system.rst>`_

  - `IP Address Functions <functions/ip.rst>`_

  - `Collection Functions <functions/collection.rst>`_

  - `Cryptographic Functions <functions/cryptographic.rst>`_

  - `JSON Functions <functions/json.rst>`_

* **Optimization**

  - `Optimization <../../user/optimization/optimization.rst>`_

* **Limitations**

  - `Limitations <limitations/limitations.rst>`_
