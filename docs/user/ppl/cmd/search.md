
# search

The `search` command retrieves documents from the index. The `search` command can only be used as the first command in a PPL query.

## Syntax

The `search` command has the following syntax:

```syntax
search source=[<remote-cluster>:]<index> [<search-expression>]
```

## Parameters

The `search` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<index>` | Required | The index to query. The index name can be prefixed with `<remote-cluster>:` (the remote cluster name) for cross-cluster search. |
| `<search-expression>` | Optional | A search expression that is converted to an OpenSearch [query string](https://docs.opensearch.org/latest/query-dsl/full-text/query-string/) query. |
  

## Search expression  

The search expression syntax supports:
* **Full-text search**: `error` or `"error message"` -- Searches the default field configured in the `index.query.default_field` setting (default is `*`, which specifies all fields). For more information, see [Default field configuration](#default-field-configuration). 
* **Field-value comparisons**: `field=value`, `field!=value`, `field>value`, `field>=value`, `field<value`, or `field<=value`.  
* **Time modifiers**: `earliest=timeModifier`, `latest=timeModifier` -- Filter results by time range using the implicit `@timestamp` field. For more information, see [Time modifiers](#time-modifiers). 
* **Boolean operators**: `AND`, `OR`, or `NOT`. Default is `AND`.  
* **Grouping using parentheses**: `(expression)`.  
* **The `IN` operator for multiple values**: `field IN (value1, value2, value3)`.  
* **Wildcards**: `*` (zero or more characters), `?` (exactly one character).  
  
### Full-text search

Unlike other PPL commands, the `search` command supports both quoted and unquoted strings. Unquoted terms are limited to alphanumeric characters, hyphens, underscores, and wildcards. Any other characters require double quotation marks.

The following queries show both syntax types:

* **Unquoted**: `search error`, `search user-123`, `search log_*`
* **Quoted**: `search "error message"`, `search "user@example.com"`
  
### Field values

Field values follow the same quoting rules as search text.

Examples of field value syntax:

* **Unquoted**: `status=active`, `code=ERR-401`
* **Quoted**: `email="user@example.com"`, `message="server error"`  
  
### Time modifiers

Time modifiers filter search results by a time range using the implicit `@timestamp` field. Time modifiers support the following formats.

| Format | Syntax | Description | Example |
| --- | --- | --- | --- |
| Current time | `now` or `now()` | The current time | `earliest=now` |
| Absolute time | `MM/dd/yyyy:HH:mm:ss` or `yyyy-MM-dd HH:mm:ss` | A specific date and time | `latest='2024-12-31 23:59:59'` |
| Unix timestamp | Numeric values | Seconds since the epoch | `latest=1754020060.123` |
| Relative time | `[(+/-)<time_integer><time_unit>][@<round_to_unit>]` | A time offset relative to the current time. See [Relative time components](#relative-time-components). | `earliest=-7d`, `latest='+1d@d'` |

#### Relative time components

Relative time modifiers use multiple components that can be combined. The following table describes each component.

| Component | Syntax | Description | Examples |
| --- | --- | --- | --- |
| Time offset | `+` or `-` | Direction: `+` (future) or `-` (past) | `+7d`, `-1h` |
| Amount of time | `<time_integer><time_unit>` | Numeric value + time unit | `7d`, `1h`, `30m` |
| Round to unit | `@<round_to_unit>` | Round to nearest unit | `@d` (day), `@h` (hour), `@m` (minute) | 
  
The following are examples of common time modifier patterns:

* `earliest=now` -- Start from the current time.
* `latest='2024-12-31 23:59:59'` -- End at a specific date and time.
* `earliest=-7d` -- Start from 7 days ago.
* `latest='+1d@d'` -- End at the start of tomorrow.
* `earliest='-1month@month'` -- Start from the beginning of the previous month.
* `latest=1754020061` -- End at the Unix timestamp `1754020061` (August 1, 2025, 03:47:41 UTC).

The following considerations apply when using time modifiers in the `search` command:

* **Column name conflicts**: If your data contains columns named `earliest` or `latest`, use backticks to access them as regular fields (for example, `` `earliest`="value"``) to avoid conflicts with time modifier syntax.  
* **Time round syntax**: Time modifiers with chained time offsets must be wrapped in quotation marks (for example, `latest='+1d@month-10h'`) for proper query parsing.  

## Default field configuration  

When a search is performed without specifying a field, it uses the default field configured by the `index.query.default_field` index setting. By default, this is set to `*`, which searches all fields.

To retrieve the default field setting, use the following request:

```bash ignore
GET /accounts/_settings/index.query.default_field
```

To modify the default field setting, use the following request:

```bash ignore
PUT /accounts/_settings
{
  "index.query.default_field": "firstname,lastname,email"
}
```

## Search behavior by field type

Different field types have specific search capabilities and limitations. The following table summarizes how search expressions work with each field type.

| Field type | Supported operations | Example | Limitations |
| --- | --- | --- | --- |
| Text | Full-text search, phrase search | `search message="error occurred" source=logs` | Wildcards apply to terms after analysis, not the entire field value |
| Keyword | Exact matching, wildcard patterns | `search status="ACTIVE" source=logs` | No text analysis; matching is case sensitive |
| Numeric | Range queries, exact matching, `IN` operator | `search age>=18 AND balance<50000 source=accounts` | No wildcard or text search support |
| Date | Range queries, exact matching, `IN` operator | `search timestamp>="2024-01-01" source=logs` | Must follow index mapping date format; wildcards not supported |
| Boolean | Exact matching, `true` and `false` values, `IN` operator | `search active=true source=users` | No wildcards or range queries |
| IP | Exact matching, CIDR notation | `search client_ip="192.168.1.0/24" source=logs` | Partial IP wildcard matching not supported. For wildcard search, use multi-field with keyword: `search ip_address.keyword='1*' source=logs` or WHERE clause: `source=logs | where cast(ip_address as string) like '1%'` |

Consider the following performance optimizations when working with different field types:

* Each field type has specific search capabilities and limitations. Choosing an inappropriate field type during ingestion can negatively affect performance and query accuracy.
* For wildcard searches on non-keyword fields, create a `keyword` subfield to improve performance. For example, for wildcard searches on a `message` field of type `text`, add a `message.keyword` field.

## Cross-cluster search  

Cross-cluster search lets any node in a cluster execute search requests against other clusters. Refer to [Cross-Cluster Search](../admin/cross_cluster_search.md/) for configuration.

## Example 1: Fetch all data

Retrieve all documents from an index:

```ppl
source=otellogs
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------+------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------------+--------------+--------------------------------------------------------------------------------------------------------------------------------------+-------+------------+------------------------+----------------+---------------------+-----------------------------------------------------------------------------------------------+
| spanId   | traceId          | @timestamp          | instrumentationScope                                                                                                                      | severityText | resource                                                                                                                             | flags | attributes | droppedAttributesCount | severityNumber | time                | body                                                                                          |
|----------+------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------------+--------------+--------------------------------------------------------------------------------------------------------------------------------------+-------+------------+------------------------+----------------+---------------------+-----------------------------------------------------------------------------------------------|
| span0001 | abcd1234efgh5678 | 2024-02-01 09:10:00 | {'name': '@opentelemetry/instrumentation-http', 'droppedAttributesCount': 0, 'version': '0.57.0'}                                         | INFO         | {'attributes': {'service': {'name': 'frontend'}, 'host': {'name': 'frontend-6b7b4c9f-x2kl9'}}, 'droppedAttributesCount': 0}          | 0     | {}         | 0                      | 9              | 2024-02-01 09:10:00 | [2024-02-01T09:10:00.123Z] "GET /api/products HTTP/1.1" 200 - 1024 45 frontend-6b7b4c9f-x2kl9 |
| span0002 | abcd1234efgh5678 | 2024-02-01 09:11:00 | {'name': 'Microsoft.Extensions.Hosting', 'droppedAttributesCount': 0, 'version': '9.0.0'}                                                 | INFO         | {'attributes': {'service': {'name': 'cart'}, 'host': {'name': 'cart-5d8f7b-mk29s'}}, 'droppedAttributesCount': 0}                    | 0     | {}         | 0                      | 9              | 2024-02-01 09:11:00 | Order #1234 placed successfully by user U100                                                  |
| span0003 | abcd1234efgh5678 | 2024-02-01 09:12:00 | {'name': 'go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc', 'droppedAttributesCount': 0, 'version': '0.49.0'} | WARN         | {'attributes': {'service': {'name': 'product-catalog'}, 'host': {'name': 'productcatalog-7c9d-zn4p2'}}, 'droppedAttributesCount': 0} | 0     | {}         | 0                      | 13             | 2024-02-01 09:12:00 | Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms        |
+----------+------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------------+--------------+--------------------------------------------------------------------------------------------------------------------------------------+-------+------------+------------------------+----------------+---------------------+-----------------------------------------------------------------------------------------------+
```


## Example 2: Text search

For basic text search, use an unquoted single term:
  
```ppl
search ERROR source=otellogs
| sort `resource.attributes.service.name`
| fields severityText, body
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------+----------------------------------------------------------------+
| severityText | body                                                           |
|--------------+----------------------------------------------------------------|
| ERROR        | NullPointerException in CheckoutService.placeOrder at line 142 |
+--------------+----------------------------------------------------------------+
```
  
Phrase search requires quotation marks for multi-word exact matching:
  
```ppl
search "Payment failed" source=otellogs
| sort `resource.attributes.service.name` | fields body | head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------------------------------------------------------+
| body                                                                |
|---------------------------------------------------------------------|
| Payment failed: connection timeout to payment gateway after 30000ms |
+---------------------------------------------------------------------+
```

Multiple search terms (unquoted string literals) are automatically combined using the `AND` operator:
  
```ppl
search connection timeout source=otellogs
| fields body
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------------------------------------------------------+
| body                                                                |
|---------------------------------------------------------------------|
| Payment failed: connection timeout to payment gateway after 30000ms |
+---------------------------------------------------------------------+
```
  
> **Note**: `search connection timeout` is equivalent to `search connection AND timeout`. 

### Combined phrase and Boolean search

Combine quoted phrases with Boolean operators for more precise searches:

```ppl
search "connection timeout" OR "heap space" source=otellogs
| sort `resource.attributes.service.name`
| fields body
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+-------------------------------------------------------------------------+
| body                                                                    |
|-------------------------------------------------------------------------|
| Payment failed: connection timeout to payment gateway after 30000ms     |
| Out of memory: Java heap space - shutting down pod payment-6f8d4b-ht7q3 |
+-------------------------------------------------------------------------+
```
  

## Example 3: Boolean logic and operator precedence  

The following queries demonstrate Boolean operators and precedence.

### Boolean operators

Use `OR` to match documents containing any of the specified conditions:

```ppl
search severityText="ERROR" OR severityText="WARN" source=otellogs
| sort severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 11/11
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| WARN         | frontend-proxy                   |
| WARN         | frontend-proxy                   |
| WARN         | product-catalog                  |
| WARN         | product-catalog                  |
| ERROR        | checkout                         |
| ERROR        | checkout                         |
| ERROR        | frontend-proxy                   |
| ERROR        | payment                          |
| ERROR        | payment                          |
| ERROR        | product-catalog                  |
| ERROR        | recommendation                   |
+--------------+----------------------------------+
```

Combine conditions with `AND` to require all criteria to match:

```ppl
search severityText="INFO" AND `resource.attributes.service.name`="cart-service" source=otellogs
| fields body
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------------------------------+
| body                                         |
|----------------------------------------------|
| Order #1234 placed successfully by user U100 |
+----------------------------------------------+
```
  
### Operator precedence

The operators are evaluated using the following precedence:

```
Parentheses > NOT > OR > AND
```

The following query demonstrates operator precedence. Because `AND` binds tighter than `OR`, this is evaluated as `severityText="ERROR" OR (resource.attributes.service.name="frontend" AND severityText="INFO")`:

```ppl
search severityText="ERROR" OR `resource.attributes.service.name`="frontend" AND severityText="INFO" source=otellogs
| fields severityText, `resource.attributes.service.name`
| head 5
```
  
```text
fetched rows / total rows = 4/4
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| INFO         | frontend                         |
| INFO         | frontend                         |
| INFO         | frontend                         |
| INFO         | frontend                         |
+--------------+----------------------------------+
```

## Example 4: NOT compared to != semantics

Both `!=` and `NOT` operators find documents in which the field value is not equal to the specified value. However, the `!=` operator excludes documents containing null or missing fields, while the `NOT` operator includes them. The following queries show this difference using `instrumentationScope.name`, which is null for most records.

**!= operator**

Excludes null values — only returns rows where the field exists and is not the specified value:

```ppl
search instrumentationScope.name!="@opentelemetry/instrumentation-http" source=otellogs
| fields instrumentationScope.name
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+------------------------------+
| instrumentationScope.name    |
|------------------------------|
| Microsoft.Extensions.Hosting |
+------------------------------+
```

**`NOT` operator**

Includes null values — returns rows where the field is null or not the specified value:

```ppl
search NOT instrumentationScope.name="@opentelemetry/instrumentation-http" source=otellogs
| fields instrumentationScope.name
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+------------------------------+
| instrumentationScope.name    |
|------------------------------|
| Microsoft.Extensions.Hosting |
| null                         |
| null                         |
| null                         |
| null                         |
+------------------------------+
```

## Example 5: Range queries

Use comparison operators (`>,` `<,` `>=` and `<=`) to filter numeric and date fields within specific ranges. Range queries are particularly useful for filtering by age, price, timestamps, or any numeric metrics:

```ppl
search severityNumber>13 AND severityNumber<=21 source=otellogs
| sort severityNumber, `resource.attributes.service.name`
| fields severityNumber
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+
| severityNumber |
|----------------|
| 17             |
| 17             |
| 17             |
+----------------+
```



## Example 6: Wildcards

The following queries demonstrate wildcard pattern matching. In wildcard patterns, `*` matches zero or more characters, while `?` matches exactly one character.

Use `*` to match any number of characters at the end of a term:

```ppl
search severityText=ERR* source=otellogs
| sort severityNumber, `resource.attributes.service.name`
| fields severityText
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+--------------+
| severityText |
|--------------|
| ERROR        |
| ERROR        |
| ERROR       |
+--------------+
```

Wildcard searches also work within text fields to find partial matches:

```ppl
search body=connection* source=otellogs
| sort `resource.attributes.service.name`
| fields body
| head 2
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+---------------------------------------------------------------------+
| body                                                                |
|---------------------------------------------------------------------|
| Payment failed: connection timeout to payment gateway after 30000ms |
| Connection pool 80% utilized on database replica db-replica-02      |
+---------------------------------------------------------------------+
```

Use `?` to match exactly one character in specific positions:

```ppl
search severityText=ERR?R source=otellogs
| fields severityText, `resource.attributes.service.name`
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 0/0
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
+--------------+----------------------------------+
```


## Example 7: Wildcard patterns in service name searches

When searching in text or keyword fields, wildcards enable partial matching, which is useful when you only know part of a service name. Wildcards work best on keyword fields, for which they match the exact value using patterns. Using wildcards on text fields may produce unexpected results because they apply to individual tokens after analysis, not the entire field value. Wildcards in keyword fields are case sensitive unless normalized at indexing.

> **Note**: Leading wildcards (for example, `*-service`) can decrease query speed compared to trailing wildcards.

Find logs for services when you only know the beginning of the service name:

```ppl
search `resource.attributes.service.name`=payment* source=otellogs
| fields severityText, `resource.attributes.service.name`, body
| head 2
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+--------------+----------------------------------+-------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                    |
|--------------+----------------------------------+-------------------------------------------------------------------------|
| ERROR        | payment                          | Payment failed: connection timeout to payment gateway after 30000ms     |
| ERROR        | payment                          | Out of memory: Java heap space - shutting down pod payment-6f8d4b-ht7q3 |
+--------------+----------------------------------+-------------------------------------------------------------------------+
```

Combine wildcard patterns with other conditions for more precise filtering:

```ppl
search firstname=A* AND age>30 source=accounts
| fields firstname, age, city
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------+-----+--------+
| firstname | age | city   |
|-----------+-----+--------|  
| Amber     | 32  | Brogan | 
+-----------+-----+--------+ 
```

## Example 8: Field value matching  

The `IN` operator efficiently checks whether a field matches any value in a list, providing a more concise and more performant alternative to chaining multiple `OR` conditions on the same field.

Check whether a field matches any value from a predefined list:

```ppl
search severityText IN ("ERROR", "WARN") source=otellogs
| fields severityText, `resource.attributes.service.name`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 11/11
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| WARN         | product-catalog                  |
| WARN         | product-catalog                  |
| WARN         | frontend-proxy                   |
| WARN         | frontend-proxy                   |
| ERROR        | payment                          |
| ERROR        | checkout                         |
| ERROR        | payment                          |
| ERROR        | frontend-proxy                   |
| ERROR        | recommendation                   |
| ERROR        | product-catalog                  |
| ERROR        | checkout                         |
+--------------+----------------------------------+
```


Filter logs by `severityNumber` to find errors with a specific numeric severity level:

```ppl
search severityNumber=17 source=otellogs
| fields body, `resource.attributes.service.name`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 7/7
+----------------------------------------------------------------------------------------------+----------------------------------+
| body                                                                                         | resource.attributes.service.name |
|----------------------------------------------------------------------------------------------+----------------------------------|
| Payment failed: connection timeout to payment gateway after 30000ms                          | payment                          |
| NullPointerException in CheckoutService.placeOrder at line 142                               | checkout                         |
| Out of memory: Java heap space - shutting down pod payment-6f8d4b-ht7q3                      | payment                          |
| [2024-02-01T09:20:00.456Z] "POST /api/checkout HTTP/1.1" 503 - 0 30000 checkout-8d4f7b-mk2p9 | frontend-proxy                   |
| Failed to process recommendation request: invalid product ID from 203.0.113.50               | recommendation                   |
| Database primary node unreachable: connection refused to db-primary-01:5432                  | product-catalog                  |
| Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) | checkout                         |
+----------------------------------------------------------------------------------------------+----------------------------------+
```

## Example 9: Complex expressions  

To create sophisticated search queries, combine multiple conditions using Boolean operators and parentheses:
  
```ppl
search (severityText="ERROR" OR severityText="WARN") AND severityNumber>13 source=otellogs
| sort severityNumber, `resource.attributes.service.name`
| fields severityText
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+
| severityText |
|--------------|
| ERROR        |
| ERROR        |
| ERROR        |
+--------------+
```

## Example 10: Time modifiers  

Time modifiers filter search results by time range using the implicit `@timestamp` field. They support various time formats for precise temporal filtering.

### Absolute time filtering

Filter logs within a specific time window using absolute timestamps:

```ppl
search earliest='2024-02-01 09:13:00' latest='2024-02-01 09:16:00' source=otellogs
| sort severityNumber
| fields `@timestamp`, severityText
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+---------------------+--------------+
| @timestamp          | severityText |
|---------------------+--------------|
| 2024-02-01 09:14:00 | DEBUG        |
| 2024-02-01 09:16:00 | INFO         |
| 2024-02-01 09:13:00 | ERROR        |
| 2024-02-01 09:15:00 | ERROR        |
+---------------------+--------------+
```
  
### Relative time filtering

Filter logs using relative time expressions, such as those that occurred before 30 seconds ago:

```ppl
search latest=-30s source=otellogs
| sort severityNumber
| fields `@timestamp`, severityText
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+---------------------+--------------+
| @timestamp          | severityText |
|---------------------+--------------|
| 2024-02-01 09:14:00 | DEBUG        |
| 2024-02-01 09:21:00 | DEBUG        |
| 2024-02-01 09:28:00 | DEBUG        |
+---------------------+--------------+
```
  
### Time rounding

Use time rounding expressions to filter events relative to time boundaries, such as those before the start of the current minute:

```ppl
search latest='@m' source=otellogs
| sort severityNumber
| fields `@timestamp`, severityText
| head 2
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+---------------------+--------------+
| @timestamp          | severityText |
|---------------------+--------------|
| 2024-02-01 09:14:00 | DEBUG        |
| 2024-02-01 09:21:00 | DEBUG        |
+---------------------+--------------+
```
  
### Unix timestamp filtering

Filter logs using Unix epoch timestamps for precise time ranges:

```ppl
search earliest=1706778600 latest=1706778960 source=otellogs
| sort severityNumber
| fields `@timestamp`, severityText
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 7/7
+---------------------+--------------+
| @timestamp          | severityText |
|---------------------+--------------|
| 2024-02-01 09:14:00 | DEBUG        |
| 2024-02-01 09:10:00 | INFO         |
| 2024-02-01 09:11:00 | INFO         |
| 2024-02-01 09:16:00 | INFO         |
| 2024-02-01 09:12:00 | WARN         |
| 2024-02-01 09:13:00 | ERROR        |
| 2024-02-01 09:15:00 | ERROR        |
+---------------------+--------------+
```
  

## Escaping special characters

Special characters fall into two categories, depending on whether they must always be escaped or only when you want to search for their literal value:

- The following characters must always be escaped to be interpreted literally:
    * **Backslash (`\`)**: Escape as `\\`.
    * **Quotation mark (`"`)**: Escape as `\"` when used inside a quoted string.

- These characters act as wildcards by default and should be escaped only when you want to match them literally:
    * **Asterisk (`*`)**: Use as `*` for wildcard matching; escape as `\\*` for a literal asterisk.
    * **Question mark (`?`)**: Use as `?` for wildcard matching; escape as `\\?` for a literal question mark.

The following table compares wildcard and literal character matching.

| Intent | PPL syntax | Result |
| ---| --- | --- |
| Wildcard search | `field=user*` | Matches `user`, `user123`, `userABC` |
| Literal `user*` | `field="user\\*"` | Matches only `user*` |
| Wildcard search | `field=log?` | Matches `log1`, `logA`, `logs` |
| Literal `log?` | `field="log\\?"`  | Matches only `log?`|