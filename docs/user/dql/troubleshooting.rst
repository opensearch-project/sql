
===============
Troubleshooting
===============

.. contents::
   :local:
   :depth: 2


Narrative
=========

SQL plugin is stateless for now so mostly the troubleshooting is focused on why a single query fails.


Syntax Analysis / Semantic Analysis Exceptions
----------------------------------------------

**Symptoms**

When you end up with exceptions similar to as follows:

Query:

.. code-block:: JSON

	POST /_opensearch/_sql
	{
	  "query" : "SELECT * FROM sample:data"
	}

Result:

.. code-block:: JSON

    {
      "error": {
        "reason": "Invalid SQL query",
        "details": "Failed to parse query due to offending symbol [:] at: 'SELECT * FROM sample:' <--- HERE... More details: Expecting tokens in {<EOF>, ';'}",
        "type": "SyntaxAnalysisException"
      },
      "status": 400
    }

**Workaround**

You need to confirm if the syntax is not supported and disable query analysis if that's the case by the following steps:

1. Check if there is any identifier (e.g. index name, column name etc.) that contains special characters (e.g. ".", " ", "&", "@",...). The SQL parser gets confused when parsing these characters within the identifiers and throws exception. If you do have such identifiers, try to quote them with backticks to pass the syntax and semantic analysis. For example, assume the index name in the query above is "sample:data" but the plugin fails the analysis, you may well try:

.. code-block:: JSON

	POST /_opensearch/_sql
	{
	  "query" : "SELECT * FROM `sample:data`"
	}

Go to the step 2 if not working.

2. Identify syntax error in failed query, and correct the syntax if the query does not follow MySQL grammar.


Index Mapping Verification Exception
------------------------------------

**Symptoms**

.. code-block:: JSON

    {
      "error": {
        "reason": "There was internal problem at backend",
        "details": "When using multiple indices, the mappings must be identical.",
        "type": "VerificationException"
      },
      "status": 503
    }

**Workaround**

If index in query is not an index pattern (index name ends with wildcard), check if the index has multiple types. If nothing works during your workaround, please create an issue in our `GitHub Issues <https://github.com/opensearch-project/sql/issues>`_ section so that we can provide you with our suggestions and help.