
# describe

The `describe` command queries index metadata. The `describe` command can only be used as the first command in the PPL query.

## Syntax

The `describe` command has the following syntax. The argument to the command is a dot-separated path to the table consisting of an optional data source, optional schema, and required table name:

```syntax
describe [<data-source>.][<schema>.]<table-name>
```

## Parameters

The `describe` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<table-name>` | Required | The table to query. |  
| `<data-source>` | Optional | The data source to use. Default is the OpenSearch `datasource`. |
| `<schema>` | Optional | The schema to use. Default is the default schema. |

## Example 1: Fetch all metadata  

This example describes the `accounts` index:
  
```ppl
describe accounts
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 11/11
+----------------+-------------+------------+----------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------+
| TABLE_CAT      | TABLE_SCHEM | TABLE_NAME | COLUMN_NAME    | DATA_TYPE | TYPE_NAME | COLUMN_SIZE | BUFFER_LENGTH | DECIMAL_DIGITS | NUM_PREC_RADIX | NULLABLE | REMARKS | COLUMN_DEF | SQL_DATA_TYPE | SQL_DATETIME_SUB | CHAR_OCTET_LENGTH | ORDINAL_POSITION | IS_NULLABLE | SCOPE_CATALOG | SCOPE_SCHEMA | SCOPE_TABLE | SOURCE_DATA_TYPE | IS_AUTOINCREMENT | IS_GENERATEDCOLUMN |
|----------------+-------------+------------+----------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------|
| docTestCluster | null        | accounts   | account_number | null      | bigint    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 0                |             | null          | null         | null        | null             | NO               |                    |
| docTestCluster | null        | accounts   | firstname      | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 1                |             | null          | null         | null        | null             | NO               |                    |
| docTestCluster | null        | accounts   | address        | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 2                |             | null          | null         | null        | null             | NO               |                    |
| docTestCluster | null        | accounts   | balance        | null      | bigint    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 3                |             | null          | null         | null        | null             | NO               |                    |
| docTestCluster | null        | accounts   | gender         | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 4                |             | null          | null         | null        | null             | NO               |                    |
| docTestCluster | null        | accounts   | city           | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 5                |             | null          | null         | null        | null             | NO               |                    |
| docTestCluster | null        | accounts   | employer       | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 6                |             | null          | null         | null        | null             | NO               |                    |
| docTestCluster | null        | accounts   | state          | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 7                |             | null          | null         | null        | null             | NO               |                    |
| docTestCluster | null        | accounts   | age            | null      | bigint    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 8                |             | null          | null         | null        | null             | NO               |                    |
| docTestCluster | null        | accounts   | email          | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 9                |             | null          | null         | null        | null             | NO               |                    |
| docTestCluster | null        | accounts   | lastname       | null      | string    | null        | null          | null           | 10             | 2        | null    | null       | null          | null             | null              | 10               |             | null          | null         | null        | null             | NO               |                    |
+----------------+-------------+------------+----------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------+
```
  

## Example 2: Fetch metadata with a condition and filter  

This example retrieves columns of the type `bigint` from the `accounts` index:
  
```ppl
describe accounts
| where TYPE_NAME="bigint"
| fields COLUMN_NAME
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+----------------+
| COLUMN_NAME    |
|----------------|
| account_number |
| balance        |
| age            |
+----------------+
```
  

## Example 3: Fetch table metadata for a Prometheus data source

See [Fetch metadata for table in Prometheus datasource](../admin/datasources.md) for more context.