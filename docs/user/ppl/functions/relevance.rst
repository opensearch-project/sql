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

