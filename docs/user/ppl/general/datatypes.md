# Data Types  

## Overview  

### PPL Data Types  

The PPL support the following data types.
  
| PPL Data Type |
| --- |
| boolean |
| tinyint |
| smallint |
| int |
| bigint |
| float |
| double |
| string |
| timestamp |
| date |
| time |
| interval |
| ip |
| geo_point |
| binary |
| struct |
| array |
| vector |
  
### Data Types Mapping  

The table below list the mapping between OpenSearch Data Type, PPL Data Type and SQL Type.
  
| OpenSearch Type | PPL Type | SQL Type |
| --- | --- | --- |
| boolean | boolean | BOOLEAN |
| byte | tinyint | TINYINT |
| short | smallint | SMALLINT |
| integer | int | INTEGER |
| long | bigint | BIGINT |
| float | float | REAL |
| half_float | float | FLOAT |
| scaled_float | float | DOUBLE |
| double | double | DOUBLE |
| keyword | string | VARCHAR |
| constant_keyword | string | VARCHAR |
| text | string | VARCHAR |
| match_only_text | string | VARCHAR |
| date | timestamp | TIMESTAMP |
| ip | ip | VARCHAR |
| binary | binary | VARBINARY |
| object | struct | STRUCT |
| nested | array | STRUCT |
| knn_vector | vector | ARRAY |
  
Notes: Not all the PPL Type has correspond OpenSearch Type. e.g. data and time. To use function which required such data type, user should explicit convert the data type.
## Numeric Data Types  

Numeric values ranged from -2147483648 to +2147483647 are recognized as integer with type name `int`. For others outside the range, `bigint` integer will be the data type after parsed.
## Date and Time Data Types  

The date and time data types are the types that represent temporal values and PPL plugin supports types including DATE, TIME, TIMESTAMP and INTERVAL. By default, the OpenSearch DSL uses date type as the only date and time related type, which has contained all information about an absolute time point. To integrate with PPL language, each of the types other than timestamp is holding part of temporal or timezone information, and the usage to explicitly clarify the date and time types is reflected in the datetime functions (see [Functions](../functions/datetime.md) for details), where some functions might have restrictions in the input argument type.
### Date  

Date represents the calendar date regardless of the time zone. A given date value represents a 24-hour period, or say a day, but this period varies in different timezones and might have flexible hours during Daylight Savings Time programs. Besides, the date type does not contain time information as well. The supported range is '1000-01-01' to '9999-12-31'.
  
| Type | Syntax | Range |
| --- | --- | --- |
| Date | 'yyyy-MM-dd' | '0001-01-01' to '9999-12-31' |
  
### Time  

Time represents the time on the clock or watch with no regard for which timezone it might be related with. Time type data does not have date information.
  
| Type | Syntax | Range |
| --- | --- | --- |
| Time | 'hh:mm:ss[.fraction]' | '00:00:00.000000' to '23:59:59.999999' |
  
### Timestamp  

A timestamp instance is an absolute instant independent of timezone or convention. For example, for a given point of time, if we set the timestamp of this time point into another timezone, the value should also be different accordingly. Besides, the storage of timestamp type is also different from the other types. The timestamp is converted from the current timezone to UTC for storage, and is converted back to the set timezone from UTC when retrieving.
  
| Type | Syntax | Range |
| --- | --- | --- |
| Timestamp | 'yyyy-MM-dd hh:mm:ss[.fraction]' | '0001-01-01 00:00:01.000000' UTC to '9999-12-31 23:59:59.999999' |
  
### Interval  

Interval data type represents a temporal duration or a period. The syntax is as follows:
  
| Type | Syntax |
| --- | --- |
| Interval | INTERVAL expr unit |
  
The expr is any expression that can be iterated to a quantity value eventually, see [Expressions](../functions/expressions.md) for details. The unit represents the unit for interpreting the quantity, including MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER and YEAR.The INTERVAL keyword and the unit specifier are not case sensitive. Note that there are two classes of intervals. Year-week intervals can store years, quarters, months and weeks. Day-time intervals can store days, hours, minutes, seconds and microseconds. Year-week intervals are comparable only with another year-week intervals. These two types of intervals can only comparable with the same type of themselves.
### Conversion between date and time types  

Basically the date and time types except interval can be converted to each other, but might suffer some alteration of the value or some information loss, for example extracting the time value from a timestamp value, or convert a date value to a timestamp value and so forth. Here lists the summary of the conversion rules that PPL plugin supports for each of the types:
#### Conversion from DATE  

- Since the date value does not have any time information, conversion to [Time](#time) type is not useful, and will always return a zero time value '00:00:00'.  
- Conversion to timestamp is to alternate both the time value and the timezone information, and it attaches the zero time value '00:00:00' and the session timezone (UTC by default) to the date. For example, the result to covert date '2020-08-17' to timestamp type with session timezone UTC is timestamp '2020-08-17 00:00:00' UTC.  
  
#### Conversion from TIME  

- Time value cannot be converted to any other date and time types since it does not contain any date information, so it is not meaningful to give no date info to a date/timestamp instance.  
  
#### Conversion from TIMESTAMP  

- Conversion from timestamp is much more straightforward. To convert it to date is to extract the date value, and conversion to time is to extract the time value. For example, the result to convert timestamp '2020-08-17 14:09:00' UTC to date is date '2020-08-17', to time is '14:09:00'.  
  
## String Data Types  

A string is a sequence of characters enclosed in either single or double quotes. For example, both 'text' and "text" will be treated as string literal.
## Vector Data Type  

A `knn_vector` field of the [k-NN plugin](https://docs.opensearch.org/latest/field-types/supported-field-types/knn-vector/) is read as the `vector` type on the Calcite engine. There, PPL responses report its type as `vector`, and so does the `describe` command while the Calcite engine is enabled (also if a query falls back to the V2 engine). With the Calcite engine disabled, PPL reports `array`, as before. SQL keeps its previous names: `nested` in responses and `knn_vector` in `DESCRIBE TABLES`. An `alias` field to a `knn_vector` field is a `vector` on the Calcite engine and keeps its previous names elsewhere (`knn_vector` in PPL `describe` with the Calcite engine disabled). The type keeps the mapping's `dimension`, `data_type` and `space_type`; a model-based field without a `dimension` is still readable. When the indices of a query map the field differently, the type keeps only what the mappings share: the dimension is unknown if they disagree on it or on the data type, except that `float` and `half_float` read their elements the same way and keep a shared dimension; the data type is the shared one, else `byte` if every index has byte elements (`byte` or `binary`), else `float`; the space type is the one the k-NN plugin uses in every index (a mapping that declares none uses the k-NN default, `l2`, or `hamming` for `binary`; on a k-NN 2.x remote cluster of a cross-cluster query, the index setting `index.knn.space_type` when the index has one, which k-NN 2.17 and 2.18 ignored for a mapping without a `method`), else unknown.

A value is an array of numbers. On the Calcite engine the elements are 32-bit floats for the `float` (default) and `half_float` data types, and signed bytes for the `byte` and `binary` data types; a `binary` vector holds `dimension / 8` bytes, since its mapping dimension counts bits. Elements are therefore returned with float precision, e.g. `-0.0069292835` for a stored `-0.006929283495992422`. This holds for a vector inside an object or `nested` field too. SQL and PPL with the Calcite engine disabled return the elements as stored, as before.

Values are read from `_source`, also by `first`, `last`, `earliest`, `latest` and `dedup` when the Calcite engine pushes them down to OpenSearch, so they return the whole vector. On an index with k-NN derived source, the k-NN plugin rebuilds each vector when it is fetched, so a vector costs a read per returned row; the Calcite engine names each vector of a projected object or `nested` field in the request, as the k-NN plugin rebuilds a vector only for a request that names its path. Use `fields - <vector field>` when the vector itself is not needed. A field excluded from `_source` reads as `null`. Without derived source, `_source` keeps a vector as it was indexed; besides an array, the k-NN plugin (3.8 and later) accepts a base64 string (little-endian 32-bit floats for `float` and `half_float` vectors, one byte per element for `byte` and `binary` vectors). The Calcite engine decodes it to the vector the k-NN plugin indexed, also inside an object or `nested` field; SQL and PPL with the Calcite engine disabled read it as an empty array, as before. When the indices of a query disagree on whether the field's elements are bytes (for example `float` in one index and `byte` in another), the elements are 32-bit floats and a base64 value is decoded with the data type of the index it comes from, also in the results of `first`, `last`, `earliest`, `latest` and `dedup`. A known limitation: a script that the Calcite engine pushes down to OpenSearch, such as a filter on `mvindex(<field>, 0)`, does not know the index of a document, so such a value fails there with `cannot decode the base64 vector of [<field>]` (HTTP 500, with the message in the shard failure) rather than be decoded with the wrong data type.

A vector column can be projected, renamed, tested with `isnull` / `isnotnull`, read with array functions such as `mvindex`, counted with `count`, and passed through `join`, `lookup`, `append`, `multisearch` and `union`. Vector values cannot be compared, sorted or grouped. A vector fails with `VECTOR values cannot be compared, sorted or grouped` when it is used in a comparison (including `in`, `between`, join conditions, join keys and `lookup` keys), as a `sort` or `trendline sort` key, in a `by` clause (`stats`, `top`, `rare`, `chart`, `eventstats`, `streamstats`, and `patterns` in the modes that group), as a `dedup` field, as a column that `mvcombine` groups by, as an `xyseries` field, as a `graphlookup` start, connect-from or connect-to field, or as the argument of `min`, `max`, `dc` / `distinct_count` or `values`. These errors never fall back to the V2 engine, even when `plugins.calcite.fallback.allowed` is true.

A `join` without keys uses every column both inputs share as a key, so it fails when the inputs share a vector column; remove the vector from one input with `fields - <vector field>`, or name the join keys. With named keys, a vector column both inputs have follows the `overwrite` option like any other shared column: by default (`overwrite=true`) the right input's value is kept, which is `null` for an unmatched row of a left join; with `overwrite=false` the left input's value is kept.

Commands that combine branches into one column (`append`, `multisearch`, `union`) require the same dimension and data type in every branch, otherwise they fail with `cannot combine VECTOR(d1, …) and VECTOR(d2, …)`.

## Query Struct Data Types  

In PPL, the Struct Data Types corresponding to the [Object field type in OpenSearch](https://opensearch.org/docs/latest/field-types/supported-field-types/object/). The "." is used as the path selector when access the inner attribute of the struct data.

A [flat_object field](https://docs.opensearch.org/latest/mappings/supported-field-types/flat-object/) is also presented as a struct, keyed by the dotted path of each leaf. A flat_object declares no sub-fields, so a leaf has no type of its own: every value reads as text, and a path holding more than one value reads as their JSON. This is the rule `spath` already applies to a JSON string, and `typeof` on a leaf reports `string`.

Both a nested object and a literal dotted key reach the same leaf: `{"a": {"b": 1}}` and `{"a.b": 1}` each give the entry `a.b` with the value `"1"`. A path receives more than one value when it comes from an array, from several elements of an array of objects, or from both spellings of the same path appearing in one document; it then reads as a JSON array of those texts, in the order the document wrote them.

A leaf is text, so a query does with it what it does with any text column, the same as a field `spath` extracted from a JSON string. What differs between one operation and the next is the cost, and the difference is large.

**Answered by the index.** Each leaf value is filed as one keyword term with the path folded in, so these are a lookup of that term and no record is opened:

| on a leaf | reaches the index as |
|-----------|----------------------|
| `=` | `term` |
| `!=` | `exists` with the term excluded |
| `in` | `terms` |
| `like` | `wildcard` |
| `isnull`, `isnotnull` | `exists` |

Reading a leaf, or the whole field, costs no more than reading a keyword field, and `cast` is applied to the values already read: `eval ms = cast(attributes.duration_ms as double)`.

**Everything else opens every record it is asked about, and is correspondingly slower.** A flat_object gives its leaves no doc values -- the term holds the path and the value folded together -- so nothing but a term lookup can be answered from the index. These operations are planned as a script that reads the leaf out of `_source` for each candidate record, which means the work grows with the number of records scanned rather than with the number returned:

- an ordered comparison (`>`, `<`, `>=`, `<=`) -- the terms are text, and a range over them would compare text, so `"200" < "90"`; the comparison the query asked for has to be done per record instead;
- `sort` by a leaf;
- `stats` over a leaf, and `stats ... by` a leaf;
- `dedup` by a leaf;
- any value computed from a leaf, such as a function or an arithmetic expression.

This is the cost the field type exists to avoid, and it is the same cost the same operation carries on a `spath` field. On a large index, put a supported filter before one of these so the script runs over a narrow set rather than the whole scan; `explain` shows which of the two a query got -- a `term`, `terms`, `wildcard` or `exists` clause, or a `script` clause.

### Example: People  

There are three fields in test index `people`: 1) deep nested object field `city`; 2) object field of array value `account`; 3) nested field `projects`
  
```bash
{
  "mappings": {
    "properties": {
      "city": {
        "properties": {
          "name": {
            "type": "keyword"
          },
          "location": {
            "properties": {
              "latitude": {
                "type": "double"
              }
            }
          }
        }
      },
      "account": {
        "properties": {
          "id": {
            "type": "keyword"
          }
        }
      },
      "projects": {
        "type": "nested",
        "properties": {
          "name": {
            "type": "keyword"
          }
        }
      }
    }
  }
}

```
  
### Example: Employees  

Here is the mapping for test index `employees_nested`. Note that field `projects` is a nested field
  
```bash
{
  "mappings": {
    "properties": {
      "id": {
        "type": "long"
      },
      "name": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "projects": {
        "type": "nested",
        "properties": {
          "name": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword"
              }
            },
            "fielddata": true
          },
          "started_year": {
            "type": "long"
          }
        }
      },
      "title": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      }
    }
  }
}


```
  
```bash
{
  "employees_nested" : [
    {
      "id" : 3,
      "name" : "Bob Smith",
      "title" : null,
      "projects" : [
        {
          "name" : "AWS Redshift Spectrum querying",
          "started_year" : 1990
        },
        {
          "name" : "AWS Redshift security",
          "started_year" : 1999
        },
        {
          "name" : "AWS Aurora security",
          "started_year" : 2015
        }
      ]
    },
    {
      "id" : 4,
      "name" : "Susan Smith",
      "title" : "Dev Mgr",
      "projects" : [ ]
    },
    {
      "id" : 6,
      "name" : "Jane Smith",
      "title" : "Software Eng 2",
      "projects" : [
        {
          "name" : "AWS Redshift security",
          "started_year" : 1998
        },
        {
          "name" : "AWS Hello security",
          "started_year" : 2015,
          "address" : [
            {
              "city" : "Dallas",
              "state" : "TX"
            }
          ]
        }
      ]
    }
  ]
}


```
  
### Example 1: Select struct inner attribute  

The example show fetch city (top level), city.name (second level), city.location.latitude (deeper level) struct type data from people results.
  
```ppl
source=people
| fields city, city.name, city.location.latitude
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------------------------------------------------+-----------+------------------------+
| city                                                | city.name | city.location.latitude |
|-----------------------------------------------------+-----------+------------------------|
| {'name': 'Seattle', 'location': {'latitude': 10.5}} | Seattle   | 10.5                   |
+-----------------------------------------------------+-----------+------------------------+
```
  
### Example 2: Group by struct inner attribute  

The example show group by object field inner attribute.
  
```ppl
source=people
| stats count() by city.name
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------+-----------+
| count() | city.name |
|---------+-----------|
| 1       | Seattle   |
+---------+-----------+
```
  
### Example 3: Selecting Field of Array Value  

Select deeper level for object fields of array value which returns the first element in the array. For example, because inner field `accounts.id` has three values instead of a tuple in this document, the first entry is returned.
  
```ppl
source = people
| fields accounts, accounts.id
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------------------+-------------+
| accounts              | accounts.id |
|-----------------------+-------------|
| [{'id': 1},{'id': 2}] | 1           |
+-----------------------+-------------+
```
  