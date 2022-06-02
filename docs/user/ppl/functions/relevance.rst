===================
Relevance Functions
===================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

The relevance based functions enable users to search the index for documents by the relevance of the input query. The functions are built on the top of the search queries of the OpenSearch engine, but in memory execution within the plugin is not supported. These functions are able to perform the global filter of a query, for example the condition expression in a ``WHERE`` clause or in a ``HAVING`` clause. For more details of the relevance based search, check out the design here: `Relevance Based Search With SQL/PPL Query Engine <https://github.com/opensearch-project/sql/issues/182>`_

MATCH
-----

Description
>>>>>>>>>>>

``match(field_expression, query_expression[, option=<option_value>]*)``

The match function maps to the match query used in search engine, to return the documents that match a provided text, number, date or boolean value with a given field. Available parameters include:

- analyzer
- auto_generate_synonyms_phrase
- fuzziness
- max_expansions
- prefix_length
- fuzzy_transpositions
- fuzzy_rewrite
- lenient
- operator
- minimum_should_match
- zero_terms_query
- boost

Example with only ``field`` and ``query`` expressions, and all other parameters are set default values::

    os> source=accounts | where match(address, 'Street') | fields lastname, address;
    fetched rows / total rows = 2/2
    +------------+--------------------+
    | lastname   | address            |
    |------------+--------------------|
    | Bond       | 671 Bristol Street |
    | Bates      | 789 Madison Street |
    +------------+--------------------+



Another example to show how to set custom values for the optional parameters::

    os> source=accounts | where match(firstname, 'Hattie', operator='AND', boost=2.0) | fields lastname;
    fetched rows / total rows = 1/1
    +------------+
    | lastname   |
    |------------|
    | Bond       |
    +------------+


SIMPLE_QUERY_STRING
-------------------

Description
>>>>>>>>>>>

``simple_query_string([field_expression+], query_expression[, option=<option_value>]*)``

The simple_query_string function maps to the simple_query_string query used in search engine, to return the documents that match a provided text, number, date or boolean value with a given field or fields.
The **^** lets you *boost* certain fields. Boosts are multipliers that weigh matches in one field more heavily than matches in other fields. The syntax allows to specify the fields in double quotes, single quotes, in backtick or even without any wrap. All fields search using star ``"*"`` is also available (star symbol should be wrapped). The weight is optional and should be specified using after the field name, it could be delimeted by the `caret` character or by whitespace. Please, refer to examples below:

| ``simple_query_string(["Tags" ^ 2, 'Title' 3.4, `Body`, Comments ^ 0.3], ...)``
| ``simple_query_string(["*"], ...)``


Available parameters include:

- analyze_wildcard
- analyzer
- auto_generate_synonyms_phrase
- flags
- fuzziness
- fuzzy_max_expansions
- fuzzy_prefix_length
- fuzzy_transpositions
- lenient
- default_operator
- minimum_should_match
- quote_field_suffix
- boost

Example with only ``fields`` and ``query`` expressions, and all other parameters are set default values::

    os> source=accounts | where simple_query_string(['firstname', city ^ 2], 'Amber | Nogal') | fields firstname, lastname, city, address;
    fetched rows / total rows = 2/2
    +-------------+------------+--------+--------------------+
    | firstname   | lastname   | city   | address            |
    |-------------+------------+--------+--------------------|
    | Amber       | Duke       | Brogan | 880 Holmes Lane    |
    | Nanette     | Bates      | Nogal  | 789 Madison Street |
    +-------------+------------+--------+--------------------+



Another example to show how to set custom values for the optional parameters::

    os> source=accounts | where simple_query_string(['firstname', city ^ 2], 'Amber Nogal', analyzer=keyword, default_operator='AND') | fields firstname, lastname, city, address;
    fetched rows / total rows = 0/0
    +-------------+------------+--------+-----------+
    | firstname   | lastname   | city   | address   |
    |-------------+------------+--------+-----------|
    +-------------+------------+--------+-----------+

Limitations
>>>>>>>>>>>

The relevance functions are available to execute only in OpenSearch DSL but not in memory as of now, so the relevance search might fail for queries that are too complex to translate into DSL if the relevance function is following after a complex PPL query. To make your queries always work-able, it is recommended to place the relevance commands as close to the search command as possible, to ensure the relevance functions are eligible to push down. For example, a complex query like ``search source = people | rename firstname as name | dedup account_number | fields name, account_number, balance, employer | where match(employer, 'Open Search') | stats count() by city`` could fail because it is difficult to translate to DSL, but it would be better if we rewrite it to an equivalent query as ``search source = people | where match(employer, 'Open Search') | rename firstname as name | dedup account_number | fields name, account_number, balance, employer | stats count() by city`` by moving the where command with relevance function to the second command right after the search command, and the relevance would be optimized and executed smoothly in OpenSearch DSL. See `Optimization <../../optimization/optimization.rst>`_ to get more details about the query engine optimization.