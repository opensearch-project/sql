# Scripts

## Table of contents
- [V2 Scripts](#v2-scripts)
- [V3 Scripts](#v3-scripts)

Script queries are generated when we push down some operators(e.g. `FILTER`, `AGGREGATE`, `SORT`) with functions that are not supported by OpenSearch DSL.

We support script push down on both v2 and v3 engine, while their registered script languages and script format are different.

## V2 Scripts

V2 script is a v2 expression tree encoded in base64 string. It's a pure byte string content without any parameters.

Example:

For any query, it will generate a script query like below, with `V2_SCRIPT_ENCODED_BYTE_STRING` varies while it's also unreadable.

```json
{
    "script": {
        "source": "{\"langType\":\"v2\", \"script\":\"<V2_SCRIPT_ENCODED_BYTE_STRING>\"}",
        "lang": "opensearch_compounded_script"
    }
}
```

## V3 Scripts

V3 script is a calcite expression tree encoded in base64 string. But before encoding, it has been standardized to make it reusable and then get higher cache hit ratio. As so, some specific information(like fields names, literal values) are extracted from the expression tree and put in the params field.

Example1:

Expression tree of `case(age < 35, 'u35' else email)` will generate a script query like below. And the expression tree will be standardized to `case(?0 < ?1, ?2 else ?3)` before encoding, in which `?i` indicates the i-th parameter.

```json
{
    "script": {
        "source": "{\"langType\":\"calcite\", \"script\":\"<V3_SCRIPT_ENCODED_BYTE_STRING>\"}",
        "lang": "opensearch_compounded_script",
        "params": {
            "utcTimestamp": 17630261838681530000,
            "SOURCES": [0, 2, 2, 1],
            "DIGESTS": ["age", 35, "u35", "email"]
        }
    }
}
```
There are usually 3 parts in the params:

1. `utcTimestamp`: The timestamp when the query is executed, it's used by some time-related functions.
2. `SOURCES`: The source of the parameters, implies which source to retrieve the value for the i-th parameter. See class CalciteScriptEngine::Source, 0 stands for `DOC_VALUE`, 1 stands for `SOURCE`, 2 stands for `LITERAL`.
3. `DIGESTS`: The digest of each parameter, it's used as the key to retrieve the value from the corresponding sources. It will be field name for `DOC_VALUE` and `SOURCE`, while the literal value itself for `LITERAL`.


Example2:

Sort script will add another specific parameter `MISSING_MAX`, which decides using MIN or MAX value for the missing value(i.e. NULL). The value of this parameter is derived from the combination of sort direction and null direction, see detail in SortExprDigest::isMissingMax.

Sort command `sort (age + balance)` will generate a script query like below.

```json
{
    "script": {
        "source": "{\"langType\":\"calcite\", \"script\":\"<V3_SCRIPT_ENCODED_BYTE_STRING>\"}",
        "lang": "opensearch_compounded_script",
        "params": {
            "MISSING_MAX": false,
            "utcTimestamp": 17630261838681530000,
            "SOURCES": [0, 0],
            "DIGESTS": ["age", "balance"]
        }
    }
}
```
