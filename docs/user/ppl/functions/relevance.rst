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


MATCH_PHRASE
------------

Description
>>>>>>>>>>>

``match_phrase(field_expression, query_expression[, option=<option_value>]*)``

The match_phrase function maps to the match_phrase query used in search engine, to return the documents that match a provided text with a given field. Available parameters include:

- analyzer
- slop
- zero_terms_query

For backward compatibility, matchphrase is also supported and mapped to match_phrase query as well.

Example with only ``field`` and ``query`` expressions, and all other parameters are set default values::

    os> source=books | where match_phrase(author, 'Alexander Milne') | fields author, title
    fetched rows / total rows = 2/2
    +----------------------+--------------------------+
    | author               | title                    |
    |----------------------+--------------------------|
    | Alan Alexander Milne | The House at Pooh Corner |
    | Alan Alexander Milne | Winnie-the-Pooh          |
    +----------------------+--------------------------+



Another example to show how to set custom values for the optional parameters::

    os> source=books | where match_phrase(author, 'Alan Milne', slop = 2) | fields author, title
    fetched rows / total rows = 2/2
    +----------------------+--------------------------+
    | author               | title                    |
    |----------------------+--------------------------|
    | Alan Alexander Milne | The House at Pooh Corner |
    | Alan Alexander Milne | Winnie-the-Pooh          |
    +----------------------+--------------------------+


MATCH_PHRASE_PREFIX
-------------------

Description
>>>>>>>>>>>

``match_phrase_prefix(field_expression, query_expression[, option=<option_value>]*)``

The match_phrase_prefix function maps to the match_phrase_prefix query used in search engine, to return the documents that match a provided text with a given field. Available parameters include:

- analyzer
- slop
- max_expansions
- boost
- zero_terms_query

Example with only ``field`` and ``query`` expressions, and all other parameters are set default values::

    os> source=books | where match_phrase_prefix(author, 'Alexander Mil') | fields author, title
    fetched rows / total rows = 2/2
    +----------------------+--------------------------+
    | author               | title                    |
    |----------------------+--------------------------|
    | Alan Alexander Milne | The House at Pooh Corner |
    | Alan Alexander Milne | Winnie-the-Pooh          |
    +----------------------+--------------------------+



Another example to show how to set custom values for the optional parameters::

    os> source=books | where match_phrase_prefix(author, 'Alan Mil', slop = 2) | fields author, title
    fetched rows / total rows = 2/2
    +----------------------+--------------------------+
    | author               | title                    |
    |----------------------+--------------------------|
    | Alan Alexander Milne | The House at Pooh Corner |
    | Alan Alexander Milne | Winnie-the-Pooh          |
    +----------------------+--------------------------+


MULTI_MATCH
-----------

Description
>>>>>>>>>>>

``multi_match([field_expression+], query_expression[, option=<option_value>]*)``

The multi_match function maps to the multi_match query used in search engine, to return the documents that match a provided text, number, date or boolean value with a given field or fields.
The **^** lets you *boost* certain fields. Boosts are multipliers that weigh matches in one field more heavily than matches in other fields. The syntax allows to specify the fields in double quotes, single quotes, in backtick or even without any wrap. All fields search using star ``"*"`` is also available (star symbol should be wrapped). The weight is optional and should be specified using after the field name, it could be delimeted by the `caret` character or by whitespace. Please, refer to examples below:

| ``multi_match(["Tags" ^ 2, 'Title' 3.4, `Body`, Comments ^ 0.3], ...)``
| ``multi_match(["*"], ...)``


Available parameters include:

- analyzer
- auto_generate_synonyms_phrase
- cutoff_frequency
- fuzziness
- fuzzy_transpositions
- lenient
- max_expansions
- minimum_should_match
- operator
- prefix_length
- tie_breaker
- type
- slop
- boost

Example with only ``fields`` and ``query`` expressions, and all other parameters are set default values::

    os> source=books | where multi_match(['title'], 'Pooh House') | fields id, title, author;
    fetched rows / total rows = 2/2
    +------+--------------------------+----------------------+
    | id   | title                    | author               |
    |------+--------------------------+----------------------|
    | 1    | The House at Pooh Corner | Alan Alexander Milne |
    | 2    | Winnie-the-Pooh          | Alan Alexander Milne |
    +------+--------------------------+----------------------+

Another example to show how to set custom values for the optional parameters::

    os> source=books | where multi_match(['title'], 'Pooh House', operator='AND', analyzer=default) | fields id, title, author;
    fetched rows / total rows = 1/1
    +------+--------------------------+----------------------+
    | id   | title                    | author               |
    |------+--------------------------+----------------------|
    | 1    | The House at Pooh Corner | Alan Alexander Milne |
    +------+--------------------------+----------------------+


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

    os> source=books | where simple_query_string(['title'], 'Pooh House') | fields id, title, author;
    fetched rows / total rows = 2/2
    +------+--------------------------+----------------------+
    | id   | title                    | author               |
    |------+--------------------------+----------------------|
    | 1    | The House at Pooh Corner | Alan Alexander Milne |
    | 2    | Winnie-the-Pooh          | Alan Alexander Milne |
    +------+--------------------------+----------------------+

Another example to show how to set custom values for the optional parameters::

    os> source=books | where simple_query_string(['title'], 'Pooh House', flags='ALL', default_operator='AND') | fields id, title, author;
    fetched rows / total rows = 1/1
    +------+--------------------------+----------------------+
    | id   | title                    | author               |
    |------+--------------------------+----------------------|
    | 1    | The House at Pooh Corner | Alan Alexander Milne |
    +------+--------------------------+----------------------+


MATCH_BOOL_PREFIX
-----------------

Description
>>>>>>>>>>>

``match_bool_prefix(field_expression, query_expression)``

The match_bool_prefix function maps to the match_bool_prefix query in the search engine. match_bool_prefix creates a match query from all but the last term in the query string. The last term is used to create a prefix query.

- analyzer
- fuzziness
- max_expansions
- prefix_length
- fuzzy_transpositions
- operator
- fuzzy_rewrite
- minimum_should_match
- boost

Example with only ``field`` and ``query`` expressions, and all other parameters are set default values::

    os> source=accounts | where match_bool_prefix(address, 'Bristol Stre') | fields firstname, address
    fetched rows / total rows = 2/2
    +-------------+--------------------+
    | firstname   | address            |
    |-------------+--------------------|
    | Hattie      | 671 Bristol Street |
    | Nanette     | 789 Madison Street |
    +-------------+--------------------+

Another example to show how to set custom values for the optional parameters::

    os> source=accounts | where match_bool_prefix(address, 'Bristol Stre', minimum_should_match = 2) | fields firstname, address
    fetched rows / total rows = 1/1
    +-------------+--------------------+
    | firstname   | address            |
    |-------------+--------------------|
    | Hattie      | 671 Bristol Street |
    +-------------+--------------------+


QUERY_STRING
------------

Description
>>>>>>>>>>>

``query_string([field_expression+], query_expression[, option=<option_value>]*)``

The query_string function maps to the query_string query used in search engine, to return the documents that match a provided text, number, date or boolean value with a given field or fields.
The **^** lets you *boost* certain fields. Boosts are multipliers that weigh matches in one field more heavily than matches in other fields. The syntax allows to specify the fields in double quotes,
single quotes, in backtick or even without any wrap. All fields search using star ``"*"`` is also available (star symbol should be wrapped). The weight is optional and should be specified using after the field name,
it could be delimeted by the `caret` character or by whitespace. Please, refer to examples below:

| ``query_string(["Tags" ^ 2, 'Title' 3.4, `Body`, Comments ^ 0.3], ...)``
| ``query_string(["*"], ...)``


Available parameters include:

- analyzer
- escape
- allow_leading_wildcard
- analyze_wildcard
- auto_generate_synonyms_phrase_query
- boost
- default_operator
- enable_position_increments
- fuzziness
- fuzzy_max_expansions
- fuzzy_prefix_length
- fuzzy_transpositions
- fuzzy_rewrite
- tie_breaker
- lenient
- type
- max_determinized_states
- minimum_should_match
- quote_analyzer
- phrase_slop
- quote_field_suffix
- rewrite
- time_zone

Example with only ``fields`` and ``query`` expressions, and all other parameters are set default values::

    os> source=books | where query_string(['title'], 'Pooh House') | fields id, title, author;
    fetched rows / total rows = 2/2
    +------+--------------------------+----------------------+
    | id   | title                    | author               |
    |------+--------------------------+----------------------|
    | 1    | The House at Pooh Corner | Alan Alexander Milne |
    | 2    | Winnie-the-Pooh          | Alan Alexander Milne |
    +------+--------------------------+----------------------+

Another example to show how to set custom values for the optional parameters::

    os> source=books | where query_string(['title'], 'Pooh House', default_operator='AND') | fields id, title, author;
    fetched rows / total rows = 1/1
    +------+--------------------------+----------------------+
    | id   | title                    | author               |
    |------+--------------------------+----------------------|
    | 1    | The House at Pooh Corner | Alan Alexander Milne |
    +------+--------------------------+----------------------+

Limitations
>>>>>>>>>>>

The relevance functions are available to execute only in OpenSearch DSL but not in memory as of now, so the relevance search might fail for queries that are too complex to translate into DSL if the relevance function is following after a complex PPL query. To make your queries always work-able, it is recommended to place the relevance commands as close to the search command as possible, to ensure the relevance functions are eligible to push down. For example, a complex query like ``search source = people | rename firstname as name | dedup account_number | fields name, account_number, balance, employer | where match(employer, 'Open Search') | stats count() by city`` could fail because it is difficult to translate to DSL, but it would be better if we rewrite it to an equivalent query as ``search source = people | where match(employer, 'Open Search') | rename firstname as name | dedup account_number | fields name, account_number, balance, employer | stats count() by city`` by moving the where command with relevance function to the second command right after the search command, and the relevance would be optimized and executed smoothly in OpenSearch DSL. See `Optimization <../../optimization/optimization.rst>`_ to get more details about the query engine optimization.
