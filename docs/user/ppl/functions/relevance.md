# Relevance functions

Relevance-based functions enable users to search an index for documents based on query relevance. These functions are built on top of OpenSearch engine search queries, but in-memory execution within the plugin is not supported.

You can use these functions for global query filtering, such as in condition expressions within `WHERE` or `HAVING` clauses. For more details about relevance-based search, see [Relevance Based Search With SQL/PPL Query Engine](https://github.com/opensearch-project/sql/issues/182).

## Usage constraints

Relevance functions are executed by the OpenSearch search engine, not by the plugin. A relevance function matches against the inverted index that OpenSearch builds when a document is written, so it can only be applied to a field that exists in the index mapping, and only at a point in the pipeline where the query can still be handed to the search engine.

In practice this means a relevance function must be applied **directly to an indexed field, before any command that transforms rows**. Place it immediately after `source=`:

```
source=logs | where match(body, 'ERROR') | eval level = upper(body) | fields level
```

The following are not supported, because there is no indexed field left to search against:

| Pattern | Why it cannot work |
|---|---|
| `... \| eval b = upper(body) \| where match(b, 'ERROR')` | `b` is computed while the query runs; it was never indexed. The same applies to columns produced by `parse` and `rex`. |
| `... \| top 1 body \| where match(body, 'ERROR')`<br>`... \| rare 1 body \| where match(body, 'ERROR')` | These commands produce summary rows after the search phase completes; there are no documents left to search. |
| `... \| sort body \| head 1 \| where match(body, 'ERROR')` | Once a sort and a row limit have been applied, the filter can no longer be handed to the search engine. |
| `... \| eval m = match(body, 'ERROR')` | A relevance function is a search condition, not a per-row value, so it cannot be assigned to a column. |

A query in this category fails at planning time, before any data is read, with an error naming the function and the column:

```
Relevance search function [match] cannot be applied to column [b]. It must run against a
field indexed by OpenSearch, and cannot be evaluated on a value the query computes (eval,
parse, rex), on aggregated output (stats, top, rare), or after a row limit (head).
```

A relevance filter placed after a `stats` aggregation on the grouping key is supported, because the filter can be applied to the underlying documents before grouping:

```
source=logs | stats count() by body | where match(body, 'ERROR')
```

If you need to filter on a computed column, use a predicate that can be evaluated per row, such as `like`:

```
source=logs | eval b = upper(body) | where like(b, '%ERROR%')
```

Note that `like` performs substring matching rather than analyzed text matching, so it does not apply tokenization, stemming, or relevance scoring.

## Matching behavior by field type

A relevance function matches against the terms OpenSearch produced when the document was indexed, so the field's mapping type determines what it matches. The same query can behave very differently on `text` and `keyword` fields.

Given a document whose `body` value is `ERROR something bad happened`:

| `body` mapping | `match(body, 'ERROR')` | Why |
|---|---|---|
| `text` | matches | The value is analyzed into terms (`error`, `something`, `bad`, `happened`), and `ERROR` matches one of them. |
| `text` with a `keyword` subfield | matches | `match` uses the analyzed `text` form. |
| `keyword` | **does not match** | A `keyword` field is indexed as one whole term. The single term is the full string `ERROR something bad happened`, which is not equal to `ERROR`. |

This applies to `match_phrase` as well, and is the most common reason a relevance query returns zero results without an error. It is correct Lucene behavior, not a failure.

On a `keyword` field, a relevance function only matches when the query value equals the entire field value:

```
source=logs | where match(body, 'ERROR something bad happened')
```

To search within a `keyword` field, use a predicate that does not depend on analysis:

| Goal | Use |
|---|---|
| Exact whole-value match | `where body = 'ERROR something bad happened'` |
| Substring match | `where like(body, '%ERROR%')` |
| Word or phrase matching | Map the field as `text`, or add a `text` subfield, then use `match` |

Note that a `keyword` subfield cannot be referenced directly in PPL — `match(body.keyword, 'ERROR')` fails with `Field [body.keyword] not found`. For a `text` field with a `keyword` subfield, the engine selects the appropriate form automatically: `match` uses the analyzed `text` form, and `like` uses the `keyword` subfield.

### Known issue: relevance filters after `head`

Avoid placing a relevance filter directly after `head`:

```
source=logs | head 100 | where match(body, 'ERROR')
```

A search request applies its query before limiting results, so this is evaluated as "search the whole index, then take 100 matches" rather than "take 100 rows, then search them". The query returns results without an error, but they are drawn from the entire index rather than from the intended sample. Apply the relevance filter before `head` instead:

```
source=logs | where match(body, 'ERROR') | head 100
```

## MATCH

**Usage**: `MATCH(<field_expression>, <query_expression>[, <option>=<option_value>]*)`

Maps to the `match` query in the OpenSearch engine. Returns documents in which the specified field matches the provided text, number, date, or Boolean value.

**Parameters**:

- `<field_expression>` (Required): The field to search in.
- `<query_expression>` (Required): The text, number, date, or Boolean value to match.
- `<option>` (Optional): Additional options specified as `<option>=<option_value>` pairs.

  The following options are available:

  - `analyzer`: Specifies which analyzer to use for the query.
  - `auto_generate_synonyms_phrase`: Whether to auto-generate synonym phrase queries.
  - `fuzziness`: Controls fuzzy matching behavior.
  - `max_expansions`: The maximum number of terms the query can expand to.
  - `prefix_length`: The number of beginning characters left unchanged for fuzzy matching.
  - `fuzzy_transpositions`: Whether fuzzy matching includes transpositions of two adjacent characters.
  - `fuzzy_rewrite`: The method used to rewrite the query.
  - `lenient`: Whether format-based failures should be ignored.
  - `operator`: The Boolean logic used to interpret text in the query value.
  - `minimum_should_match`: The minimum number of clauses that must match.
  - `zero_terms_query`: What to return when the analyzer removes all tokens.
  - `boost`: A floating-point value used to decrease or increase relevance scores.

**Return type**: `BOOLEAN`

#### Examples

The following example uses only the required parameters, with all optional parameters set to default values:
  
```ppl
source=accounts
| where match(address, 'Street')
| fields lastname, address
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----------+--------------------+
| lastname | address            |
|----------+--------------------|
| Bond     | 671 Bristol Street |
| Bates    | 789 Madison Street |
+----------+--------------------+
```
  
The following example shows how to set custom values for the optional parameters:
  
```ppl
source=accounts
| where match(firstname, 'Hattie', operator='AND', boost=2.0)
| fields lastname
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------+
| lastname |
|----------|
| Bond     |
+----------+
```
  
## MATCH_PHRASE

**Usage**: `MATCH_PHRASE(<field_expression>, <query_expression>[, <option>=<option_value>]*)`

Maps to the `match_phrase` query in the OpenSearch engine. Returns documents in which the specified field matches the provided text as a phrase.

**Parameters**:

- `<field_expression>` (Required): The field to search in.
- `<query_expression>` (Required): The text to match as a phrase.
- `<option>` (Optional): Additional options specified as `<option>=<option_value>` pairs.

  The following options are available:

  - `analyzer`: Specifies which analyzer to use for the query.
  - `slop`: The maximum number of positions between matching terms.
  - `zero_terms_query`: What to return when the analyzer removes all tokens.

**Return type**: `BOOLEAN`

For backward compatibility, `matchphrase` is also supported and mapped to the `match_phrase` query.

#### Examples

The following example uses only the required parameters, with all optional parameters set to default values:
  
```ppl
source=books
| where match_phrase(author, 'Alexander Milne')
| fields author, title
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----------------------+--------------------------+
| author               | title                    |
|----------------------+--------------------------|
| Alan Alexander Milne | The House at Pooh Corner |
| Alan Alexander Milne | Winnie-the-Pooh          |
+----------------------+--------------------------+
```
  
The following example shows how to set custom values for the optional parameters:
  
```ppl
source=books
| where match_phrase(author, 'Alan Milne', slop = 2)
| fields author, title
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----------------------+--------------------------+
| author               | title                    |
|----------------------+--------------------------|
| Alan Alexander Milne | The House at Pooh Corner |
| Alan Alexander Milne | Winnie-the-Pooh          |
+----------------------+--------------------------+
```
  
## MATCH_PHRASE_PREFIX

**Usage**: `MATCH_PHRASE_PREFIX(<field_expression>, <query_expression>[, <option>=<option_value>]*)`

Maps to the `match_phrase_prefix` query in the OpenSearch engine. Returns documents in which the specified field matches the provided text using prefix matching on the last term.

**Parameters**:

- `<field_expression>` (Required): The field to search in.
- `<query_expression>` (Required): The text to match using prefix matching on the last term.
- `<option>` (Optional): Additional options specified as `<option>=<option_value>` pairs.

  The following options are available:

  - `analyzer`: Specifies which analyzer to use for the query.
  - `slop`: The maximum number of positions between matching terms.
  - `max_expansions`: The maximum number of terms the last provided term can expand to.
  - `boost`: A floating-point value used to decrease or increase relevance scores.
  - `zero_terms_query`: What to return when the analyzer removes all tokens.

**Return type**: `BOOLEAN`

#### Examples

The following example uses only the required parameters, with all optional parameters set to default values:
  
```ppl
source=books
| where match_phrase_prefix(author, 'Alexander Mil')
| fields author, title
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----------------------+--------------------------+
| author               | title                    |
|----------------------+--------------------------|
| Alan Alexander Milne | The House at Pooh Corner |
| Alan Alexander Milne | Winnie-the-Pooh          |
+----------------------+--------------------------+
```
  
The following example shows how to set custom values for the optional parameters:
  
```ppl
source=books
| where match_phrase_prefix(author, 'Alan Mil', slop = 2)
| fields author, title
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----------------------+--------------------------+
| author               | title                    |
|----------------------+--------------------------|
| Alan Alexander Milne | The House at Pooh Corner |
| Alan Alexander Milne | Winnie-the-Pooh          |
+----------------------+--------------------------+
```
  
## MULTI_MATCH

**Usage**:
- `MULTI_MATCH([<field_expression+>], <query_expression>[, <option>=<option_value>]*)`.
- `MULTI_MATCH(<query_expression>[, <option>=<option_value>]*)`.

Maps to the `multi_match query` in the OpenSearch engine. Returns documents in which one or more specified fields match the provided text, number, date, or Boolean value.

**Two syntax forms are supported**:
1. **With explicit fields** (classic syntax): `multi_match([field_list], query, ...)`
2. **Without fields** (search default fields): `multi_match(query, ...)`

When fields are omitted, the query searches in the fields specified by the `index.query.default_field` setting.

You can boost specific fields using the `^` symbol. Boosts are multipliers that weigh matches in one field more heavily than matches in other fields. You can specify fields using double quotation marks, single quotation marks, backticks, or without quotation marks. You can also search all fields using `"*"` (the star symbol must be enclosed in quotation marks). The boost value is optional and should be specified after the field name, separated by the `^` character or white space:
- `multi_match(["Tags" ^ 2, 'Title' 3.4, `Body`, Comments ^ 0.3], ...)`.
- `multi_match(["*"], ...)`.
- `multi_match("search text", ...)` (searches default fields).

**Parameters**:

- `<field_expression+>` (Optional): List of fields to search in, with optional boost values.
- `<query_expression>` (Required): The text, number, date, or Boolean value to match.
- `<option>` (Optional): Additional options specified as `<option>=<option_value>` pairs.

  The following options are available:

  - `analyzer`: Specifies which analyzer to use for the query.
  - `auto_generate_synonyms_phrase`: Whether to auto-generate synonym phrase queries.
  - `cutoff_frequency`: Allows high frequency terms to be put into a query.
  - `fuzziness`: Controls fuzzy matching behavior.
  - `fuzzy_transpositions`: Whether fuzzy matching includes transpositions of two adjacent characters.
  - `lenient`: Whether format-based failures should be ignored.
  - `max_expansions`: The maximum number of terms the query can expand to.
  - `minimum_should_match`: The minimum number of clauses that must match.
  - `operator`: The Boolean logic used to interpret text in the query value.
  - `prefix_length`: The number of beginning characters left unchanged for fuzzy matching.
  - `tie_breaker`: A value between 0.0 and 1.0 to use as a tiebreaker for fields with the same relevance.
  - `type`: How the multi_match query should be executed internally.
  - `slop`: The maximum number of positions between matching terms (for phrase queries).
  - `boost`: A floating-point value used to decrease or increase relevance scores.

**Return type**: `BOOLEAN`

#### Examples

The following example uses explicit field specification with required parameters only:
  
```ppl
source=books
| where multi_match(['title'], 'Pooh House')
| fields id, title, author
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----+--------------------------+----------------------+
| id | title                    | author               |
|----+--------------------------+----------------------|
| 1  | The House at Pooh Corner | Alan Alexander Milne |
| 2  | Winnie-the-Pooh          | Alan Alexander Milne |
+----+--------------------------+----------------------+
```
  
The following example shows explicit field specification with optional parameters:
  
```ppl
source=books
| where multi_match(['title'], 'Pooh House', operator='AND', analyzer=default)
| fields id, title, author
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----+--------------------------+----------------------+
| id | title                    | author               |
|----+--------------------------+----------------------|
| 1  | The House at Pooh Corner | Alan Alexander Milne |
+----+--------------------------+----------------------+
```
  
The following example uses the default field syntax without explicit field specification:
  
```ppl
source=books
| where multi_match('Pooh House')
| fields id, title, author
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----+--------------------------+----------------------+
| id | title                    | author               |
|----+--------------------------+----------------------|
| 1  | The House at Pooh Corner | Alan Alexander Milne |
| 2  | Winnie-the-Pooh          | Alan Alexander Milne |
+----+--------------------------+----------------------+
```
  
## SIMPLE_QUERY_STRING

**Usage**:
- `SIMPLE_QUERY_STRING([<field_expression+>], <query_expression>[, <option>=<option_value>]*)`.
- `SIMPLE_QUERY_STRING(<query_expression>[, <option>=<option_value>]*)`.

Maps to the `simple_query_string` query in the OpenSearch engine. Returns documents in which one or more specified fields match the provided text, number, date, or Boolean value.

**Two syntax forms are supported**:
1. **With explicit fields** (classic syntax): `simple_query_string([field_list], query, ...)`
2. **Without fields** (search default fields): `simple_query_string(query, ...)`

When fields are omitted, the query searches in the fields specified by the `index.query.default_field` setting.

You can boost specific fields using the `^` symbol. Boosts are multipliers that weigh matches in one field more heavily than matches in other fields. You can specify fields using double quotation marks, single quotation marks, backticks, or without quotation marks. You can also search all fields using `"*"` (the star symbol must be enclosed in quotation marks). The boost value is optional and should be specified after the field name, separated by the `^` character or white space:
- `simple_query_string(["Tags" ^ 2, 'Title' 3.4, `Body`, Comments ^ 0.3], ...)`.
- `simple_query_string(["*"], ...)`.
- `simple_query_string("search text", ...)` (searches default fields).

**Parameters**:

- `<field_expression+>` (Optional): List of fields to search in, with optional boost values.
- `<query_expression>` (Required): The text, number, date, or Boolean value to match.
- `<option>` (Optional): Additional options specified as `<option>=<option_value>` pairs.

  The following options are available:

  - `analyze_wildcard`: Whether to analyze wildcard and prefix queries.
  - `analyzer`: Specifies which analyzer to use for the query.
  - `auto_generate_synonyms_phrase`: Whether to auto-generate synonym phrase queries.
  - `flags`: Simple query string flags to enable operators.
  - `fuzziness`: Controls fuzzy matching behavior.
  - `fuzzy_max_expansions`: The maximum number of terms fuzzy queries can expand to.
  - `fuzzy_prefix_length`: The number of beginning characters left unchanged for fuzzy matching.
  - `fuzzy_transpositions`: Whether fuzzy matching includes transpositions of two adjacent characters.
  - `lenient`: Whether format-based failures should be ignored.
  - `default_operator`: The default Boolean logic used to interpret text in the query.
  - `minimum_should_match`: The minimum number of clauses that must match.
  - `quote_field_suffix`: The suffix to append to quoted text in the query string.
  - `boost`: A floating-point value used to decrease or increase relevance scores.

**Return type**: `BOOLEAN`

#### Examples

The following example uses explicit field specification with required parameters only:
  
```ppl
source=books
| where simple_query_string(['title'], 'Pooh House')
| fields id, title, author
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----+--------------------------+----------------------+
| id | title                    | author               |
|----+--------------------------+----------------------|
| 1  | The House at Pooh Corner | Alan Alexander Milne |
| 2  | Winnie-the-Pooh          | Alan Alexander Milne |
+----+--------------------------+----------------------+
```
  
The following example shows explicit field specification with optional parameters:
  
```ppl
source=books
| where simple_query_string(['title'], 'Pooh House', flags='ALL', default_operator='AND')
| fields id, title, author
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----+--------------------------+----------------------+
| id | title                    | author               |
|----+--------------------------+----------------------|
| 1  | The House at Pooh Corner | Alan Alexander Milne |
+----+--------------------------+----------------------+
```
  
The following example uses the default field syntax without explicit field specification:
  
```ppl
source=books
| where simple_query_string('Pooh House')
| fields id, title, author
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----+--------------------------+----------------------+
| id | title                    | author               |
|----+--------------------------+----------------------|
| 1  | The House at Pooh Corner | Alan Alexander Milne |
| 2  | Winnie-the-Pooh          | Alan Alexander Milne |
+----+--------------------------+----------------------+
```
  
## MATCH_BOOL_PREFIX

**Usage**: `MATCH_BOOL_PREFIX(<field_expression>, <query_expression>[, <option>=<option_value>]*)`

Maps to the `match_bool_prefix` query in the OpenSearch engine. Returns documents in which the specified field matches the provided text, where all terms except the last are matched exactly and the last term is treated as a prefix.

**Parameters**:

- `<field_expression>` (Required): The field to search in.
- `<query_expression>` (Required): The text to match, where the last term is treated as a prefix.
- `<option>` (Optional): Additional options specified as `<option>=<option_value>` pairs.

  The following options are available:

  - `analyzer`: Specifies which analyzer to use for the query.
  - `fuzziness`: Controls fuzzy matching behavior.
  - `max_expansions`: The maximum number of terms the query can expand to.
  - `prefix_length`: The number of beginning characters left unchanged for fuzzy matching.
  - `fuzzy_transpositions`: Whether fuzzy matching includes transpositions of two adjacent characters.
  - `operator`: The Boolean logic used to interpret text in the query value.
  - `fuzzy_rewrite`: The method used to rewrite the query.
  - `minimum_should_match`: The minimum number of clauses that must match.
  - `boost`: A floating-point value used to decrease or increase relevance scores.

**Return type**: `BOOLEAN`

#### Examples

The following example uses only the required parameters, with all optional parameters set to default values:
  
```ppl
source=accounts
| where match_bool_prefix(address, 'Bristol Stre')
| fields firstname, address
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+-----------+--------------------+
| firstname | address            |
|-----------+--------------------|
| Hattie    | 671 Bristol Street |
| Nanette   | 789 Madison Street |
+-----------+--------------------+
```
  
The following example shows setting optional parameters:
  
```ppl
source=accounts
| where match_bool_prefix(address, 'Bristol Stre', minimum_should_match = 2)
| fields firstname, address
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------+--------------------+
| firstname | address            |
|-----------+--------------------|
| Hattie    | 671 Bristol Street |
+-----------+--------------------+
```
  
## QUERY_STRING

**Usage**:
- `QUERY_STRING([<field_expression+>], <query_expression>[, <option>=<option_value>]*)`.
- `QUERY_STRING(<query_expression>[, <option>=<option_value>]*)`.

Maps to the `query_string` query in the OpenSearch engine. Returns documents in which one or more specified fields match the provided text, number, date, or Boolean value.

**Two syntax forms are supported**:
1. **With explicit fields** (classic syntax): `query_string([field_list], query, ...)`
2. **Without fields** (search default fields): `query_string(query, ...)`

When fields are omitted, the query searches in the fields specified by the `index.query.default_field` setting.

You can boost specific fields using the `^` symbol. Boosts are multipliers that weigh matches in one field more heavily than matches in other fields. You can specify fields using double quotation marks, single quotation marks, backticks, or without quotation marks. You can also search all fields using `"*"` (the star symbol must be enclosed in quotation marks). The boost value is optional and should be specified after the field name, separated by the `^` character or white space:
- `query_string(["Tags" ^ 2, 'Title' 3.4, `Body`, Comments ^ 0.3], ...)`.
- `query_string(["*"], ...)`.
- `query_string("search text", ...)` (searches default fields).

**Parameters**:

- `<field_expression+>` (Optional): List of fields to search in, with optional boost values.
- `<query_expression>` (Required): The text, number, date, or Boolean value to match.
- `<option>` (Optional): Additional options specified as `<option>=<option_value>` pairs.

  The following options are available:

  - `analyzer`: Specifies which analyzer to use for the query.
  - `escape`: Whether to escape special characters in the query string.
  - `allow_leading_wildcard`: Whether to allow leading wildcards.
  - `analyze_wildcard`: Whether to analyze wildcard and prefix queries.
  - `auto_generate_synonyms_phrase_query`: Whether to auto-generate synonym phrase queries.
  - `boost`: A floating-point value used to decrease or increase relevance scores.
  - `default_operator`: The default Boolean logic used to interpret text in the query.
  - `enable_position_increments`: Whether to enable position increments in result queries.
  - `fuzziness`: Controls fuzzy matching behavior.
  - `fuzzy_max_expansions`: The maximum number of terms fuzzy queries can expand to.
  - `fuzzy_prefix_length`: The number of beginning characters left unchanged for fuzzy matching.
  - `fuzzy_transpositions`: Whether fuzzy matching includes transpositions of two adjacent characters.
  - `fuzzy_rewrite`: The method used to rewrite fuzzy queries.
  - `tie_breaker`: A value between 0.0 and 1.0 to use as a tiebreaker for fields with the same relevance.
  - `lenient`: Whether format-based failures should be ignored.
  - `type`: How the query_string query should be executed internally.
  - `max_determinized_states`: The maximum number of automaton states for regexp or fuzzy queries.
  - `minimum_should_match`: The minimum number of clauses that must match.
  - `quote_analyzer`: The analyzer to use for quoted text in the query string.
  - `phrase_slop`: The default slop for phrase queries built from the query string.
  - `quote_field_suffix`: The suffix to append to quoted text in the query string.
  - `rewrite`: The method used to rewrite the query.
  - `time_zone`: The time zone to use for date range queries.

**Return type**: `BOOLEAN`

#### Examples

The following example uses explicit field specification with required parameters only:
  
```ppl
source=books
| where query_string(['title'], 'Pooh House')
| fields id, title, author
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----+--------------------------+----------------------+
| id | title                    | author               |
|----+--------------------------+----------------------|
| 1  | The House at Pooh Corner | Alan Alexander Milne |
| 2  | Winnie-the-Pooh          | Alan Alexander Milne |
+----+--------------------------+----------------------+
```
  
The following example shows explicit field specification with optional parameters:
  
```ppl
source=books
| where query_string(['title'], 'Pooh House', default_operator='AND')
| fields id, title, author
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----+--------------------------+----------------------+
| id | title                    | author               |
|----+--------------------------+----------------------|
| 1  | The House at Pooh Corner | Alan Alexander Milne |
+----+--------------------------+----------------------+
```
  
The following example uses the default field syntax without explicit field specification:
  
```ppl
source=books
| where query_string('Pooh House')
| fields id, title, author
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----+--------------------------+----------------------+
| id | title                    | author               |
|----+--------------------------+----------------------|
| 1  | The House at Pooh Corner | Alan Alexander Milne |
| 2  | Winnie-the-Pooh          | Alan Alexander Milne |
+----+--------------------------+----------------------+
```
  
## Limitations

Relevance functions execute only in the OpenSearch query DSL, not in memory. Relevance searches can fail if a query is too complex to translate into DSL, particularly when a relevance function follows complex PPL operations.

To ensure correct execution, place relevance functions as close to the `search` command as possible. This increases the likelihood that the functions are eligible for push-down optimization.

**Example of problematic query structure**:
```sql
search source = people
| rename firstname as name
| dedup account_number
| fields name, account_number, balance, employer
| where match(employer, 'Open Search')
| stats count() by city
```

Place the `where` command containing the relevance function immediately after the `search` command so that the function can be optimized and executed in the OpenSearch DSL.

**Recommended query structure**:
```sql
search source = people
| where match(employer, 'Open Search')
| rename firstname as name
| dedup account_number
| fields name, account_number, balance, employer
| stats count() by city
```

See [Optimization](../../optimization/optimization.rst) to get more details about the query engine optimization.