
# highlight

The `highlight` command adds search result highlighting metadata to the query response. When used, matching terms are wrapped in highlight tags (for example, `<em>Holmes</em>`) and returned in a separate `highlights` array alongside the data rows.

> **Note**: The `highlight` command is supported by the Calcite engine only. Using it with the V2 engine returns an error.

## Syntax

The `highlight` command has the following syntax:

```syntax
highlight <arg> [, <arg>]*
```

## Parameters

The `highlight` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `*` | Required (one of `*` or `"<term>"`) | Highlights all matches from the search query across all fields. |
| `"<term>"` | Required (one of `*` or `"<term>"`) | Highlights a specific term across all fields, regardless of the search query. Multiple terms can be specified, separated by commas. |

## Example 1: Highlight search query matches

The following query searches for `"Holmes"` and highlights all matches using the wildcard `*`:

```ppl
source=accounts "Holmes" | highlight * | fields account_number, firstname, address
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------------+-----------+-----------------+
| account_number | firstname | address         |
|----------------+-----------+-----------------|
| 1              | Amber     | 880 Holmes Lane |
+----------------+-----------+-----------------+
```

The tabular output shows the data rows only. The highlight metadata is returned in the JSON response as a parallel `highlights` array:

```json ignore
{
  "highlights": [
    {
      "address": ["880 <em>Holmes</em> Lane"]
    }
  ]
}
```

## Example 2: Highlight a specific term

The following query highlights the term `"Duke"` across all fields, independent of any search query:

```ppl
source=accounts | highlight "Duke" | fields account_number, firstname, lastname | sort account_number
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+-----------+----------+
| account_number | firstname | lastname |
|----------------+-----------+----------|
| 1              | Amber     | Duke     |
| 6              | Hattie    | Bond     |
| 13             | Nanette   | Bates    |
| 18             | Dale      | Adams    |
+----------------+-----------+----------+
```

All rows are returned because `highlight` does not filter results. Only the row containing `"Duke"` will have highlight metadata in the response.

## Example 3: Highlight multiple terms with a filter

The following query highlights multiple terms and filters results using `where`:

```ppl
source=accounts | highlight "Bond", "Duke" | where age > 30 | fields account_number, firstname, lastname | sort account_number
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+-----------+----------+
| account_number | firstname | lastname |
|----------------+-----------+----------|
| 1              | Amber     | Duke     |
| 6              | Hattie    | Bond     |
| 18             | Dale      | Adams    |
+----------------+-----------+----------+
```

## Response format

The `highlight` command adds a `highlights` array to the JSON response, parallel to `datarows`. Each entry maps field names to a list of highlighted fragments. Rows with no matches have a `null` entry. The `highlights` field is omitted entirely when no rows have highlight data.

```json ignore
{
  "schema": [
    { "name": "firstname", "type": "string" },
    { "name": "lastname", "type": "string" }
  ],
  "datarows": [
    ["Amber", "Duke"],
    ["Hattie", "Bond"]
  ],
  "highlights": [
    {
      "lastname": ["<em>Duke</em>"],
      "lastname.keyword": ["<em>Duke</em>"]
    },
    null
  ],
  "total": 2,
  "size": 2
}
```

## Comparison with Splunk

Both Splunk and OpenSearch PPL use `| highlight` as a pipe command. However, they differ in implementation:

| | Splunk | OpenSearch PPL |
|---|---|---|
| Arguments | Literal string values (`ERROR`, `WARNING`) | Terms (`"login"`) or wildcard (`*`) |
| What gets highlighted | The exact strings you pass | `*`: search query matches; terms: those specific terms |
| Output | UI colorization on Events tab only | `highlights` array in JSON response |
| Compatible commands | Breaks with any transforming command (`sort`, `table`, `fields`) | Works with all non-aggregating commands (`sort`, `where`, `fields`, `dedup`, `eval`) |

## Limitations

The `highlight` command has the following limitations:

* Aggregation commands (`stats`, `eventstats`, `streamstats`, `top`, `rare`, `chart`) eliminate per-document results. Highlight data is lost after aggregation.
* For joins, only the left-side (first) table gets highlighting.
* Highlight tags are not customizable via the `| highlight` command. OpenSearch defaults (`<em>`, `</em>`) are used.
