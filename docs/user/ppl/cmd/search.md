
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

## Example 1: Fetching all data

Retrieve all documents from an index by specifying only the source without any search conditions. This is useful for exploring small datasets or verifying data ingestion:

```ppl
source=accounts
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
| account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname |
|----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------|
| 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     |
| 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     |
| 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    |
| 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    |
+----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
```


## Example 2: Text search

For basic text search, use an unquoted single term:
  
```ppl
search ERROR source=otellogs
| sort @timestamp
| fields severityText, body
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------+---------------------------------------------------------+
| severityText | body                                                    |
|--------------+---------------------------------------------------------|
| ERROR        | Payment failed: Insufficient funds for user@example.com |
+--------------+---------------------------------------------------------+
```
  
Phrase search requires quotation marks for multi-word exact matching:
  
```ppl
search "Payment failed" source=otellogs
| fields body
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------------------------------------------+
| body                                                    |
|---------------------------------------------------------|
| Payment failed: Insufficient funds for user@example.com |
+---------------------------------------------------------+
```

Multiple search terms (unquoted string literals) are automatically combined using the `AND` operator:
  
```ppl
search user email source=otellogs
| sort @timestamp
| fields body
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------------------------------------------------------------------------------------------------------+
| body                                                                                                               |
|--------------------------------------------------------------------------------------------------------------------|
| Executing SQL: SELECT * FROM users WHERE email LIKE '%@gmail.com' AND status != 'deleted' ORDER BY created_at DESC |
+--------------------------------------------------------------------------------------------------------------------+
```
  
> **Note**: `search user email` is equivalent to `search user AND email`. 

Enclose terms containing special characters in double quotation marks:
  
```ppl
search "john.doe+newsletter@company.com" source=otellogs
| fields body
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------------------------------------------------------------------------------------------------------+
| body                                                                                                               |
|--------------------------------------------------------------------------------------------------------------------|
| Email notification sent to john.doe+newsletter@company.com with subject: 'Welcome! Your order #12345 is confirmed' |
+--------------------------------------------------------------------------------------------------------------------+
```
  
### Combined phrase and Boolean search

Combine quoted phrases with Boolean operators for more precise searches:

```ppl
search "User authentication" OR OAuth2 source=otellogs
| sort @timestamp
| fields body
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------------------------------------------------------------------------------------------+
| body                                                                                                     |
|----------------------------------------------------------------------------------------------------------|
| [2024-01-15 10:30:09] production.INFO: User authentication successful for admin@company.org using OAuth2 |
+----------------------------------------------------------------------------------------------------------+
```
  

## Example 3: Boolean logic and operator precedence  

The following queries demonstrate Boolean operators and precedence.

### Boolean operators

Use `OR` to match documents containing any of the specified conditions:

```ppl
search severityText="ERROR" OR severityText="FATAL" source=otellogs
| sort @timestamp
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
| FATAL        |
| ERROR        |
+--------------+
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
+----------------------------------------------------------------------------------+
| body                                                                             |
|----------------------------------------------------------------------------------|
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart |
+----------------------------------------------------------------------------------+
```
  
### Operator precedence

The operators are evaluated using the following precedence:

```
Parentheses > NOT > OR > AND
```

The following query demonstrates operator precedence:

```ppl
search severityText="ERROR" OR severityText="WARN" AND severityNumber>15 source=otellogs
| sort @timestamp
| fields severityText, severityNumber
| head 2
```
  
The preceding expression is evaluated as `(severityText="ERROR" OR severityText="WARN") AND severityNumber>15`. The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| ERROR        | 17             |
| ERROR        | 17             |
+--------------+----------------+
```

## Example 4: NOT compared to != semantics  

Both `!=` and `NOT` operators find documents in which the field value is not equal to the specified value. However, the `!=` operator excludes documents containing null or missing fields, while the `NOT` operator includes them. The following query shows this difference.

**!= operator**

Find all accounts for which the `employer` field exists and is not `Quility`:

```ppl
search employer!="Quility" source=accounts
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
| account_number | firstname | address            | balance | gender | city   | employer | state | age | email                 | lastname |
|----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------|
| 1              | Amber     | 880 Holmes Lane    | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     |
| 6              | Hattie    | 671 Bristol Street | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     |
+----------------+-----------+--------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
```
  
**`NOT` operator** 

Find all accounts that do not specify `Quility` as the employer (including those with null employer values):

```ppl
search NOT employer="Quility" source=accounts
```
  
The query returns the following results. Dale Adams appears in the search results because his `employer` field is `null`:
  
```text
fetched rows / total rows = 3/3
+----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
| account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname |
|----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------|
| 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     |
| 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     |
| 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    |
+----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
```

## Example 5: Range queries

Use comparison operators (`>,` `<,` `>=` and `<=`) to filter numeric and date fields within specific ranges. Range queries are particularly useful for filtering by age, price, timestamps, or any numeric metrics:

```ppl
search severityNumber>15 AND severityNumber<=20 source=otellogs
| sort @timestamp
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
| 18             |
+----------------+
```

The following query filters by decimal values within a specific range:

```ppl
search `attributes.payment.amount`>=1000.0 AND `attributes.payment.amount`<=2000.0 source=otellogs
| fields body
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------------------------------------------------------+
| body                                                    |
|---------------------------------------------------------|
| Payment failed: Insufficient funds for user@example.com |
+---------------------------------------------------------+
```


## Example 6: Wildcards

The following queries demonstrate wildcard pattern matching. In wildcard patterns, `*` matches zero or more characters, while `?` matches exactly one character.

Use `*` to match any number of characters at the end of a term:

```ppl
search severityText=ERR* source=otellogs
| sort @timestamp
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
| ERROR2       |
+--------------+
```

Wildcard searches also work within text fields to find partial matches:

```ppl
search body=user* source=otellogs
| sort @timestamp
| fields body
| head 2
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+----------------------------------------------------------------------------------+
| body                                                                             |
|----------------------------------------------------------------------------------|
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart |
| Payment failed: Insufficient funds for user@example.com                          |
+----------------------------------------------------------------------------------+
```

Use `?` to match exactly one character in specific positions:

```ppl
search severityText="INFO?" source=otellogs
| sort @timestamp
| fields severityText
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+--------------+
| severityText |
|--------------|
| INFO2        |
| INFO3        |
| INFO4        |
+--------------+
```


## Example 7: Wildcard patterns in field searches  

When searching in text or keyword fields, wildcards enable partial matching, which is useful when you only know part of a value. Wildcards work best on keyword fields, for which they match the exact value using patterns. Using wildcards on text fields may produce unexpected results because they apply to individual tokens after analysis, not the entire field value. Wildcards in keyword fields are case sensitive unless normalized at indexing.

> **Note**: Leading wildcards (for example, `*@example.com`) can decrease query speed compared to trailing wildcards.

Find records for which you only know the beginning of a field value:

```ppl
search employer=Py* source=accounts
| fields firstname, employer
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------+----------+
| firstname | employer |
|-----------+----------|
| Amber     | Pyrami   |
+-----------+----------+
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
search severityText IN ("ERROR", "WARN", "FATAL") source=otellogs
| sort @timestamp
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
| WARN         |
| FATAL        |
+--------------+
```


Filter logs by `severityNumber` to find errors with a specific numeric severity level:

```ppl
search severityNumber=17 source=otellogs
| sort @timestamp
| fields body
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------------------------------------------+
| body                                                    |
|---------------------------------------------------------|
| Payment failed: Insufficient funds for user@example.com |
+---------------------------------------------------------+
```

Search for logs containing a specific user email address in the attributes:

```ppl
search `attributes.user.email`="user@example.com" source=otellogs
| fields body
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------------------------------------------+
| body                                                    |
|---------------------------------------------------------|
| Payment failed: Insufficient funds for user@example.com |
+---------------------------------------------------------+
```
  

## Example 9: Complex expressions  

To create sophisticated search queries, combine multiple conditions using Boolean operators and parentheses:
  
```ppl
search (severityText="ERROR" OR severityText="WARN") AND severityNumber>10 source=otellogs
| sort @timestamp
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
| WARN         |
| ERROR        |
+--------------+
```

Combine multiple conditions with `OR` and `AND` operators to search for logs matching either a specific user or high-severity fund errors:

```ppl
search `attributes.user.email`="user@example.com" OR (`attributes.error.code`="INSUFFICIENT_FUNDS" AND severityNumber>15) source=otellogs
| fields body
```
  
The query returns the following results:
  
```
fetched rows / total rows = 1/1
+---------------------------------------------------------+
| body                                                    |
|---------------------------------------------------------|
| Payment failed: Insufficient funds for user@example.com |
+---------------------------------------------------------+
```
  

## Example 10: Time modifiers  

Time modifiers filter search results by time range using the implicit `@timestamp` field. They support various time formats for precise temporal filtering.

### Absolute time filtering

Filter logs within a specific time window using absolute timestamps:

```ppl
search earliest='2024-01-15 10:30:05' latest='2024-01-15 10:30:10' source=otellogs
| fields @timestamp, severityText
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 6/6
+-------------------------------+--------------+
| @timestamp                    | severityText |
|-------------------------------+--------------|
| 2024-01-15 10:30:05.678901234 | FATAL        |
| 2024-01-15 10:30:06.789012345 | TRACE        |
| 2024-01-15 10:30:07.890123456 | ERROR        |
| 2024-01-15 10:30:08.901234567 | WARN         |
| 2024-01-15 10:30:09.012345678 | INFO         |
| 2024-01-15 10:30:10.123456789 | TRACE2       |
+-------------------------------+--------------+
```
  
### Relative time filtering

Filter logs using relative time expressions, such as those that occurred before 30 seconds ago:

```ppl
search latest=-30s source=otellogs
| sort @timestamp
| fields @timestamp, severityText
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-------------------------------+--------------+
| @timestamp                    | severityText |
|-------------------------------+--------------|
| 2024-01-15 10:30:00.123456789 | INFO         |
| 2024-01-15 10:30:01.23456789  | ERROR        |
| 2024-01-15 10:30:02.345678901 | WARN         |
+-------------------------------+--------------+
```
  
### Time rounding

Use time rounding expressions to filter events relative to time boundaries, such as those before the start of the current minute:

```ppl
search latest='@m' source=otellogs
| fields @timestamp, severityText
| head 2
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+-------------------------------+--------------+
| @timestamp                    | severityText |
|-------------------------------+--------------|
| 2024-01-15 10:30:00.123456789 | INFO         |
| 2024-01-15 10:30:01.23456789  | ERROR        |
+-------------------------------+--------------+
```
  
### Unix timestamp filtering

Filter logs using Unix epoch timestamps for precise time ranges:

```ppl
search earliest=1705314600 latest=1705314605 source=otellogs
| fields @timestamp, severityText
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+-------------------------------+--------------+
| @timestamp                    | severityText |
|-------------------------------+--------------|
| 2024-01-15 10:30:00.123456789 | INFO         |
| 2024-01-15 10:30:01.23456789  | ERROR        |
| 2024-01-15 10:30:02.345678901 | WARN         |
| 2024-01-15 10:30:03.456789012 | DEBUG        |
| 2024-01-15 10:30:04.567890123 | INFO         |
+-------------------------------+--------------+
```
  

## Example 11: Escaping special characters

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

### Escaping backslash characters

Each backslash in the search value must be escaped with another backslash. For example, the following query searches for Windows file paths by properly escaping backslashes:

```ppl
search `attributes.error.type`="C:\\\\Users\\\\admin" source=otellogs
| fields `attributes.error.type`
```


The query returns the following results:

```text
fetched rows / total rows = 1/1
+-----------------------+
| attributes.error.type |
|-----------------------|
| C:\Users\admin        |
+-----------------------+
```

> **Note**: When using the REST API with JSON, additional JSON escaping is required.

### Quotation marks within strings

Search for text containing quotation marks by escaping them with backslashes:

```ppl
search body="\"exact phrase\"" source=otellogs
| sort @timestamp
| fields body
| head 1
```


The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------------------------------------------------------------------------------------------------------------------------------------------------------+
| body                                                                                                                                                   | 
|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| Query contains Lucene special characters: +field:value -excluded AND (grouped OR terms) NOT "exact phrase" wildcard* fuzzy~2 /regex/ [range TO search] |
+--------------------------------------------------------------------------------------------------------------------------------------------------------+
```

### Text containing special characters

Search for literal text containing wildcard characters by escaping them:

```ppl
search "wildcard\\* fuzzy~2" source=otellogs
| fields body
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------------------------------------------------------------------------------------------------------------------------------------------------------+
| body                                                                                                                                                   |
|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| Query contains Lucene special characters: +field:value -excluded AND (grouped OR terms) NOT "exact phrase" wildcard* fuzzy~2 /regex/ [range TO search] |
+--------------------------------------------------------------------------------------------------------------------------------------------------------+
```