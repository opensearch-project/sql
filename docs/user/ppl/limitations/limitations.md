# Limitations  

## Inconsistent Field Types across indices  

* If the same field has different types across indices (e.g., `field` is a `string` in one index and an `integer` in another), PPL selects a field type from one of the indices—this selection is non-deterministic. Fields with other types are ignored during query execution.  
* For `object` fields, [PPL merges subfields from different indices to tolerate schema variations](https://github.com/opensearch-project/sql/issues/3625).  
  
## Unsupported OpenSearch Field Types  

PPL does not support all [OpenSearch data types](https://docs.opensearch.org/latest/mappings/supported-field-types/index/). (e.g., `flattened`, some complex `nested` usages). Unsupported fields are excluded from `DESCRIBE` and `SOURCE` outputs. At runtime: Queries referencing unsupported fields fail with semantic or resolution errors. Such fields are ignored in projections unless explicitly filtered out or removed at ingestion.
  
| OpenSearch Data Type | PPL |
| --- | --- |
| knn_vector | Ignored |
| Range field types | Ignored |
| Object - flat_object | Ignored |
| Object - join | Ignored |
| String - Match-only text | Ignored |
| String - Wildcard | Ignored |
| String - token_count | Ignored |
| String - constant_keyword | Ignored |
| Autocomplete | Ignored |
| Geoshape | Ignored |
| Cartesian field types | Ignored |
| Rank field types | Ignored |
| Star-tree | Ignored |
| derived | Ignored |
| Percolator | Ignored |
  
## Field Parameters  

For a field to be queryable in PPL, the following index settings must be enabled:
  
| Setting | Description | Required For |
| --- | --- | --- |
| _source: true | Stores the original JSON document | Required for fetch raw data. |
| index: true | Enables field indexing | Required for filtering, search, and aggregations |
| doc_values: true | Enables columnar access for aggregations/sorting | Required for `stats`, `sort` |
  
## Nested Field Behavior  

* There are [limitations](https://github.com/opensearch-project/sql/issues/4625) regarding the nested levels and query types that need improvement.  
  
## Multi-value Field Behavior  

OpenSearch does not natively support the ARRAY data type but does allow multi-value fields implicitly. The
SQL/PPL plugin adheres strictly to the data type semantics defined in index mappings. When parsing OpenSearch
responses, it expects data to match the declared type and does not account for data in array format. If the
plugins.query.field_type_tolerance setting is enabled, the SQL/PPL plugin will handle array datasets by returning
scalar data types, allowing basic queries (e.g., source = tbl \| where condition). However, using multi-value
fields in expressions or functions will result in exceptions. If this setting is disabled or absent, only the
first element of an array is returned, preserving the default behavior.
## Unsupported Functionalities in Calcite Engine  

Since 3.0.0, we introduce Apache Calcite as an experimental query engine. Please see [introduce v3 engine](../../../dev/intro-v3-engine.md).
For the following functionalities, the query will be forwarded to the V2 query engine. It means following functionalities cannot work with new PPL commands/functions introduced in 3.0.0 and above.
* All SQL queries  
* PPL Queries against non-OpenSearch data sources  
* `dedup` with `consecutive=true`  
* Search relevant commands  
    * AD  
    * ML  
    * Kmeans  
* `show datasources` and command  
* Commands with `fetch_size` parameter  


## Malformed Field Names in Object Fields

OpenSearch normally rejects field names containing problematic dot patterns (such as `.`, `..`, `.a`, `a.`, or `a..b`). However, when an object field has `enabled: false`, OpenSearch bypasses field name validation and allows storing documents with any field names.

If a document contains malformed field names inside an object field, PPL ignores those malformed field names. Other valid fields in the document are returned normally.

**Example of affected data:**

```json
{
    "log": {
    ".": "value1",
    ".a": "value2",
    "a.": "value3",
    "a..b": "value4"
    }
}
```

When ``log`` is an object field with ``enabled: false``, subfields with malformed names are ignored.

**Recommendation:** Avoid using field names that contain leading dots, trailing dots, consecutive dots, or consist only of dots. This aligns with OpenSearch's default field naming requirements.

## Bucket aggregation result may be approximate in large dataset

In OpenSearch, `doc_count` values for a terms bucket aggregation may be approximate. As a result, any aggregations (such as `sum` and `avg`) on the terms bucket aggregation may also be approximate.
For example, the following PPL query (find the top 10 URLs) may return an approximate result if the cardinality of `URL` is high.

```ppl ignore
source=hits
| stats bucket_nullable=false count() as c by URL
| sort - c
| head 10
```

This query is pushed down to a terms bucket aggregation DSL query with `"order": { "_count": "desc" }`. In OpenSearch, this terms aggregation may throw away some buckets.

## Sorting by ascending doc_count may produce inaccurate results

Similar to above PPL query, the following query (find the rare 10 URLs) often produces inaccurate results.

```ppl ignore
source=hits
| stats bucket_nullable=false count() as c by URL
| sort + c
| head 10
```

A term that is globally infrequent might not appear as infrequent on every individual shard or might be entirely absent from the least frequent results returned by some shards. Conversely, a term that appears infrequently on one shard might be common on another. In both scenarios, rare terms can be missed during shard-level aggregation, resulting in incorrect overall results.
