## 1 Overview

In a search engine, the relevance is the measure of the relationship accuracy between the search query and the search result. The higher the relevance is, the higher quality of the search result, then the users are able to get more relevant content from the search result. For the searches in OpenSearch engine, the returned results (documents) are ordered by the relevance by default, the top documents are of the highest relevance. In OpenSearch, the relevance is indicated by a field `_score`. This float type field gives a score to the current document to measure how relevant it is related to the search query, with a higher score the document is indicated to be more relevant to the search.

The OpenSearch query engine is the engine to do the query planning for user input queries. Currently the query engine is interfaced with SQL and PPL (Piped Processing Language), thus the users are able to write SQL and PPL queries to explore their data in OpenSearch. Most of the queries supported in the query engine are following the SQL use cases, which are mapped to the structured queries in OpenSearch.

This design is to support the relevance based search queries with query engine, in another word to enable the OpenSearch users to write SQL/PPL languages to do the search by relevance in the search engine.


### 1.1 Problem Statement

**1. DSL is not commonly used**
OpenSearch query language (DSL) is not commonly used in regular databases, especially for the users in the realm of analytics rather than development. This is also the reason we created the SQL plugin, where the query engine lies in. Like many other SQL servers where the full text search features are enabled to support the relevance based search with SQL language, the SQL plugin in OpenSearch is also a perfect target for users to search by relevance on the indexed database if we support the search features in the query engine.

**2. OpenSearch search features are not present in the new query engine**
The current query engine is working more like a traditional SQL server to retrieve exact data from the OpenSearch indices, so from the query engine standpoint, the OpenSearch is treated as an ordinary database to store data rather than a search engine. One of the gap in between is the search features in the search engine are not supported in the query engine. By bringing search by relevance, one of the most relevant features of the search engine, into the query engine, our users would be able to explore and do the search using SQL or PPL language directly.

**3. Full text functions in old engine**
Last year (2020) we migrated the legacy SQL plugin to a new constructed architecture with a new query engine to do the query planning for SQL and PPL queries. Some of the search functions were already enabled in the old engine. However, the full text functions in the old engine are not migrated to the new engine, so when users try to do the search query with SQL, it falls back to the old engine rather be planned in the new query engine. This is not causing any issue for short term since the use of old and new engines are out of the user awareness. But for long-term prospective, we need to support these functions in new engine as well in rid of in consistency, and also good for future plan of the old engine deprecation.


### 1.2 Use Cases

**Use case 1:** **Full text search**
The full text search functions are very common in most of the SQL servers now due to the high demanding of the search features, and also because its high efficiency compared to the wildcard match (like) operator. With the support of search features in the query engine, users are able to execute the full text functions with SQL language on the top of OpenSearch search engine, while not limited to the simple search but also the complicated search like the search with prefix, search multiple fields and so forth.

**Use case 2: Field independent search with SQL/PPL**
In many cases the users want to search the entire document for a term rather than in a specific field. For example a user might want to search an index for a keyword “Seattle”, this might come from fields like “DestinationCity”, “OriginCity”, “AirportLocation” etc., and all these results matter for the user. The search features proposed in this design also include this case to enable users to do multi field search or even field independent search with SQL and PPL language.

**Use case 3: Keyword highlighting**
The highlighters supported in the search engine is another feature that is on the top of relevance based search. By enabling the highlighters, users are able to get the highlighted snippets from one or more fields in the search results, so users can clearly see where the query matches are.

**Use case 4: Observability project essential**
The observability is a project that aims to enable users to explore all type of data like logs, metrics etc. in one place by bringing the logs, metrics and traces together. The relevance based search feature would be a key feature for user to search and filter the data in observability.


### 1.3 Requests from the community
- https://github.com/opendistro-for-elasticsearch/sql/issues/1137
- https://github.com/opensearch-project/sql/issues/160
- https://github.com/opensearch-project/sql/issues/183


## 2 Requirements

### 2.1 Functional Requirements

* Support relevance based search functions with SQL/PPL language.
    * Define user friendly SQL/PPL syntax for the search use.
    * Should push the search query down to OpenSearch engine if possible. For queries that are unable to push down, the engine should return error with explicit message to raise users’ awareness.
* Support custom input values for function parameters to adjust the scoring strategy.
* (Optional) Enable user actions on the scoring field, for example sort by `_score` field in either ascending or descending order.
* (Optional) Enable in memory execution within the query engine if the plan fails to optimize to search engine.

### 2.2 Non-functional Requirements

#### A. Reliability
* The search is entirely executed in the OpenSearch engine, so the search result is consistent with the result in OpenSearch search engine with corresponding DSL.
* The old engine already has supported some of the full text functions, so the new supported features should not break the existing full text functions in syntax prospective for users.

#### B. Extensibility
* Extensible to supplement more relevance based search functions or other relevant features.
* The architecture and implementation should be extensible to enable in memory execution for long term improvement of the query engine.

### 2.3 Tenets

1. The search with query engine is on the top of the search query in OpenSearch search engine, the relevance based search should be entirely executed in the search engine. Different from the ordinary queries in the query engine, the search query is not going to be executed in memory in the query engine even when it fails to push down into search engine.
2. The search with SQL/PPL interface respects the search query with OpenSearch DSL in syntax and results. This means the users are able to search with DSL

### 2.4 Out of Scope

1. All the search feature is built on the top of the search engine. Basically a complex query that fails to push down to the search engine is executed in the query engine in node level. But for the search features, the in memory search is out of scope for this design.
2. This design focuses on enabling users to do the search with SQL/PPL interface, but the analyzers and search algorithms are actually of the search engine job, so the features or enhancement of the search queries within the search engine are out of scope for this design.


## 3 High Level Design

### 3.1 Search functions to support

Since the relevance based search is highly depending on the OpenSearch core engine, we are therefore defining the search functions following the existing search queries (see **appendix A1**) but in the language style of SQL full text search functions. Here comes the list of functions with basic functionalities (i.e. parameter options) to support as the relevance based search functions in the query engine:

|Query Type	|Function Name In Query Engine	|Description	|
|---	|---	|---	|
|Match query	|match	|Returns documents that match a provided text, number, date or boolean value. The provided text is analyzed before matching.	|
|Match phrase query	|match_phrase	|The `match_phrase` query analyzes the text and creates a `phrase` query out of the analyzed text.	|
|Match phrase prefix query	|match_phrase_prefix	|Returns documents that contain the words of a provided text, in the **same order** as provided. The last term of the provided text is treated as a prefix, matching any words that begin with that term.	|
|Match boolean prefix query	|match_bool_prefix	|analyzes its input and constructs a `bool` query from the terms. Each term except the last is used in a `term` query. The last term is used in a `prefix` query.	|
|Multi match query	- best fields	|multi_match	|Finds documents which match any field, but uses the `_score` from the best field.	|
|Multi match query	- most fields	|multi_match	|Finds documents which match any field and combines the `_score` from each field.	|
|Multi match query	- cross fields	|multi_match	|Treats fields with the same `analyzer` as though they were one big field. Looks for each word in **any** field.	|
|Multi match query	- phrase	|multi_match	|Runs a `match_phrase` query on each field and uses the `_score` from the best field.	|
|Multi match query	- phrase prefix	|multi_match	|Runs a `match_phrase_prefix` query on each field and uses the `_score` from the best field.	|
|Multi match query	- bool prefix	|multi_match	|Creates a `match_bool_prefix` query on each field and combines the `_score` from each field. 	|
|Combined fields 	|combined_fields	|analyzes the query string into individual terms, then looks for each term in any of the fields	|
|Common terms query	|common	|The `common` terms query is a modern alternative to stopwords which improves the precision and recall of search results (by taking stopwords into account), without sacrificing performance.	|
|Query string	|query_string	|Returns documents based on a provided query string, using a parser with a strict syntax.	|
|Simple query string	|simple_query_string	|Returns documents based on a provided query string, using a parser with a limited but fault-tolerant syntax.	|

The function names follow the query type names directly to avoid confusion when using the exactly the same functionalities with different languages. Besides, all the available options for the queries are passed as parameters in the functions. See **4.2 Search function details** for more details.


### 3.2 Architecture diagram

#### Option A: Remain the original query plans, register all the search functions as SQL/PPL scalar functions.
The current engine architecture is well constructed with plan component that have their own unique job. For example, all the conditions in a query would be converted to the filter plans, and an aggregation plan is converted also the aggregation functions.

Regarding the functionalities of search features, they are essentially performing the filter roles to find the documents that contain specific terms. Therefore, the search functions could be perfectly fitted into the filter conditions just like the other SQL functions in the condition expression.

Besides, one of the query engine architecture tenets is to keep the job of every plan node simple and unique, and construct the entire query planning using existing plan node rather than creating new type of plan. This can keep each of operators in the physical plans simple and well defined. The following figure shows a simplified logical query plan with only project, filter and relation plans for a match query like `SELECT * FROM my_index WHERE match(message, "this is a test")`.

![relevance-Page-3](https://user-images.githubusercontent.com/33583073/130142789-bd78f8e6-c7b9-43e2-abb5-46c845af5a8e.png)

**Pros:**
- Obey the tenet to avoid logical and physical plans with overlapped work
- Could follow the similar pattern of scalar functions to do the implementation work

**Cons:**
- The search queries are served as scalar functions, this could be a limitation in extensibility for PPL language, for example this option is over complicated if we create commands like `match` to support the search features.


#### Option B: Create a new query plan node dedicated for the search features.

The diagram below is simplified with only the logical plan and physical plan sections and leaves out others. Please check out [OpenSearch SQL Engine Architecture](https://github.com/opensearch-project/sql/blob/main/docs/dev/Architecture.md) for the complete architecture of the query engine.

![relevance-Page-2 (4)](https://user-images.githubusercontent.com/33583073/129938534-28fa4845-4246-4707-9519-e68c9e86d174.png)

The match component in the logical plans stands for the logical match in logical planning stage. This could be an extension of the filter plan, which is specially to handle the search functions.

Ideally a relevance based search query is optimized to a plan where the score is able to merge with the logical index scan, this also means the score operation is eligible to turn to the OpenSearch query and pushed down to the search engine. However the query engine should also have the capability to deal with the scoring operations locally when the query planning runs into a complicated case and fails to push score down to the search engine.

Since the in memory execution of the search queries are out of scope for current phase, we are not setting the corresponding match operator in the query engine, and throwing out errors instead if a query tries to reach the match operator.

Pros:
- All the search plans are aggregated in one plan node and distinguished from other scalar function expression.
- Extensible for the implementation of in-memory execution since they share common algorithm base (BM25).

Cons:
- Redundant plan node and disobey the query plan tenet.


## 4 Detailed Design

### 4.1 SQL and PPL language interface

In the legacy engine, the solution is to allow user to write a DSL segment as the parameter and pass it directly to the filter. This syntax could be much flexible for users but it skips the syntax analysis and type check in between. So we are proposing a more SQL-like syntax for the search functions by passing the options as named arguments instead. However this might bring some challenge in future maintenance if more contexts for the supported queries in core engine come up in future release.

For the sake of user experience, we decided to put on the SQL like language interface. So from the SQL language standpoint, the search functions could be performed similar to the SQL functions existing in SELECT, WHERE, GROUP BY clauses etc., and the returned data type is set to boolean. For example:

```
SELECT match(message, "this is a test") FROM my_index
SELECT * FROM my_index where match(message, "this is a test")
```


Similarly from PPL standpoint, we could define it either as functions or as a command named `match`:
**Option 1: served as eval functions similar to SQL language**

```
search source=my_index | eval f=match(message, "this is a test")
search source=my_index | where match(message, "this is a test") | fields message
```

**Option 2: define a new command**

```
search source=my_index | match field=message query="this is a test"
search source=my_index | match type=match_phrase field=message query="this is a test" | fields event, message, timestamp
```

### 4.2 Search mode
The search type for the new engine is defaulted to `query_then_fetch` mode as the default setting of OpenSearch engine, and usually it works fine unless the document number is too small to smooth out the term/document frequency statistics. The alternative option for the query type is `dfs_query_then_fetch`, which adds an additional step of prequerying each shard asking about term and document frequencies before `query_then_fetch` procedures.

Similarly to the DSL queries, the search mode options for SQL plugin could be supported through the request endpoint, for example to set the `dfs_query_then_fetch` as the search mode in specific query:
```
POST _plugins/_sql?type=dfs_query_then_fetch
{
  "query": "SELECT message FROM my_index WHERE match(message, 'this is a test')
}
```


### 4.3 Search function details

In this section we focus on defining the syntax and functionalities for each of the functions to support, and how these functions are mapped with the search engine queries. Since most of the functions are reusing the common arguments, here lists the arguments that could be used by the search functions

Required parameters:

* **field(s):** Field(s) you wish to search. Defaults to the index.query.default_field index setting, which has a default value of *.
* **query:** Text, number, boolean value or date you wish to find in the provided `<field>`. For the functions that are not requiring a specific field, the `fields` parameter might be an option to specify the fields to search for terms.

Optional parameters:

* **fields**: Field list to search for the query string. Set to "*" for all fields.
* **analyzer:** (string) Used to convert the text in `query` into tokens. Available values: (default) standard analyzer | simple analyzer | whitespace analyzer | stop analyzer | keyword analyzer | pattern analyzer | language analyzer | fingerprint analyzer | custom analyzer
* **auto_generate_synonyms_phrase_query:** (boolean) If true, match phrase queries are automatically created for multi-term synonyms. Defaults to true.
* **fuzziness**: (string) Maximum edit distance allowed for matching.
* **max_expansions:** (integer) Maximum number of terms to which the last provided term of the `query` value will expand. Defaults to 50.
* **prefix_length**: (integer) Number of beginning characters left unchanged for fuzzy matching. Defaults to 0.
* **fuzzy_transpositions**: (boolean) If true, edits for fuzzy matching include transpositions of two adjacent characters (ab → ba). Defaults to true.
* **fuzzy_rewrite**: (string) Method used to rewrite the query.
* **lenient**: (boolean) If true, format-based errors, such as providing a text query value for a numeric field, are ignored. Defaults to false.
* **operator**: (string) Boolean logic used to interpret text in the query value. Available operators: (default) OR | AND.
* **minimum_should_match**: (string) Minimum number of clauses that must match for a document to be returned.
* **zero_terms_query**: (string)  Indicates whether no documents are returned if the analyzer removes all tokens, such as when using a stop filter. Available values: (default) none | all.
* **boost**: (float) Boost value toward the relevance score. Defaults to 1.0.
* **type_breaker**: (float) Floating point number between 0 and 1.0 used to increase the relevance scores of documents matching multiple query clauses. Defaults to 0.0.
* **cutoff_frequency**: (float) Specifies an absolute or relative document frequency where high frequency terms are moved into an optional subquery and are only scored if one of the low frequency (below the cutoff) terms in the case of an or operator or all of the low frequency terms in the case of an and operator match. The `cutoff_frequency` value can either be relative to the total number of documents if in the range [0..1) or absolute if greater or equal to 1.0.
* **slop:** (integer) **** maximum number of positions allowed between matching tokens. Defaults to 0.


The following links are redirecting to the issues pages of the search functions details including syntax, functionality and available paramters:

**1. match function**
https://github.com/opensearch-project/sql/issues/184

**2. match_phrase function**
https://github.com/opensearch-project/sql/issues/185

**3. match_phrase_prefix function**
https://github.com/opensearch-project/sql/issues/186

**4. match_bool_prefix function**
https://github.com/opensearch-project/sql/issues/187

**5. multi_match function**
https://github.com/opensearch-project/sql/issues/188

**6. combined_fields function**
https://github.com/opensearch-project/sql/issues/189

**7. common function:**
https://github.com/opensearch-project/sql/issues/190

**8. query_string function**
https://github.com/opensearch-project/sql/issues/191

**9. simple_query_string function**
https://github.com/opensearch-project/sql/issues/192


## 5 Implementation
The implementation covers the planner and optimizer in the query engine but skip the changes in in-memory executor as discussed in the design for current phase. One of the tricky things in the implementations is how to recognize the custom values as query parameters or options.

The current solution is to pass the type check and environment resolve for the parameter values since they are not participating the in memory computing, but take the values all as string expressions and resolve them in the layer of optimizer when translated to query DSL. Take `analyzer` as an example:
```
// define the analyzer action
private final BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder> analyzer =
    (b, v) -> b.analyzer(v.stringValue());

...

ImmutableMap<Object, Object> argAction = ImmutableMap.builder()
    .put("analyzer", analyzer)
    .put(...)
    .build();

...

// build match query
while (iterator.hasNext()) {
  NamedArgumentExpression arg = (NamedArgumentExpression) iterator.next();
  if (!argAction.containsKey(arg.getArgName())) {
    throw new SemanticCheckException(String
        .format("Parameter %s is invalid for match function.", arg.getArgName()));
  }
  ((BiFunction<MatchQueryBuilder, ExprValue, MatchQueryBuilder>) argAction
      .get(arg.getArgName()))
      .apply(queryBuilder, arg.getValue().valueOf(null));
}
```

## 6 Testing
All the code changes should be test driven. The pull requests should include unit test cases, integration test cases and comparison test cases in applicable.



## Appendix

### A1. Relevance based search queries in OpenSearch

|Query Type	|Advanced options for common parameter	|Example of basic query	|
|---	|---	|---	|
|Match query	|enable synonyms, fuzziness options, max expansions, prefix length, lenient, operator, minimum should match, zero terms query	|{"query": {"match": {"message": {"query": "this is a test"}}}}	|
|Match phrase query	|zero terms query	|{"query": {"match_phrase": {"message": "this is a test"}}}	|
|Match phrase prefix query	|max expansions, slop, zero terms query	|{"query": {"match_phrase_prefix": {"message": {"query": "quick brown f"}}}}	|
|Match boolean prefix query	|fuzziness options, max expansions, prefix length, operator, minimum should match	|{"query": {"match_bool_prefix" : {"message" : "quick brown f"}}}	|
|Multi match query	| type (best fields, most fields, cross fields, phrase, phrase prefix, bool prefix) |{"query": {"multi_match" : {"query":      "brown fox","type":       "best_fields","fields":     [ "subject", "message" ],"tie_breaker": 0.3}}}	|
|Combined fields 	|enable synonyms, operator, minimum should match, zero terms query	|{"query": {"combined_fields" : {"query":      "database systems","fields":     [ "title", "abstract", "body"],"operator":   "and"}}}	|
|Common terms query	|minimum should match, low/high frequency operator, cutoff frequency, boost	|{"query": {"common": {"body": {"query": "this is bonsai cool","cutoff_frequency": 0.001}}}}	|
|Query string	|wildcard options, enable synonyms, boost, operator, enable position increments, fields (multi fields search), fuzziness options, lenient, max determined states, minimum should match, quote analyzer, phrase slop, quote field suffix, rewrite, time zone	|{"query": {"query_string": {"query": "(new york city) OR (big apple)","default_field": "content"}}}	|
|Simple query string	|operator, enable all fields search, analyze wildcard, enable synonyms, flags, fuzziness options, lenient, minimum should match, quote field suffix	|{"query": {"simple_query_string" : {"query": "\"fried eggs\" +(eggplant | potato) -frittata","fields": ["title^5", "body"],"default_operator": "and"}}}	|

### A2. Search flow in search engine

![relevance](https://user-images.githubusercontent.com/33583073/129938659-5b49f43d-a83f-47d5-be5b-937b1c96e5bc.png)