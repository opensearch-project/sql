# fields  

## Description  

The `fields` command keeps or removes fields from the search result.
## Syntax  

fields [+\|-] \<field-list\>
* +\|-: optional. If the plus (+) is used, only the fields specified in the field list will be kept. If the minus (-) is used, all the fields specified in the field list will be removed. **Default:** +.  
* field-list: mandatory. Comma-delimited or space-delimited list of fields to keep or remove. Supports wildcard patterns.  
  
## Example 1: Select specified fields from result  

This example shows selecting account_number, firstname and lastname fields from search results.
  
```ppl
source=accounts
| fields account_number, firstname, lastname
```
  
Expected output:
  
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
  
## Example 2: Remove specified fields from result  

This example shows removing the account_number field from search results.
  
```ppl
source=accounts
| fields account_number, firstname, lastname
| fields - account_number
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----------+----------+
| firstname | lastname |
|-----------+----------|
| Amber     | Duke     |
| Hattie    | Bond     |
| Nanette   | Bates    |
| Dale      | Adams    |
+-----------+----------+
```
  
## Example 3: Space-delimited field selection  

Fields can be specified using spaces instead of commas, providing a more concise syntax.
**Syntax**: `fields field1 field2 field3`
  
```ppl
source=accounts
| fields firstname lastname age
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----------+----------+-----+
| firstname | lastname | age |
|-----------+----------+-----|
| Amber     | Duke     | 32  |
| Hattie    | Bond     | 36  |
| Nanette   | Bates    | 28  |
| Dale      | Adams    | 33  |
+-----------+----------+-----+
```
  
## Example 4: Prefix wildcard pattern  

Select fields starting with a pattern using prefix wildcards.
  
```ppl
source=accounts
| fields account*
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------+
| account_number |
|----------------|
| 1              |
| 6              |
| 13             |
| 18             |
+----------------+
```
  
## Example 5: Suffix wildcard pattern  

Select fields ending with a pattern using suffix wildcards.
  
```ppl
source=accounts
| fields *name
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----------+----------+
| firstname | lastname |
|-----------+----------|
| Amber     | Duke     |
| Hattie    | Bond     |
| Nanette   | Bates    |
| Dale      | Adams    |
+-----------+----------+
```
  
## Example 6: Contains wildcard pattern  

Select fields containing a pattern using contains wildcards.
  
```ppl
source=accounts
| fields *a*
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------------+-----------+-----------------+---------+-------+-----+----------------------+----------+
| account_number | firstname | address         | balance | state | age | email                | lastname |
|----------------+-----------+-----------------+---------+-------+-----+----------------------+----------|
| 1              | Amber     | 880 Holmes Lane | 39225   | IL    | 32  | amberduke@pyrami.com | Duke     |
+----------------+-----------+-----------------+---------+-------+-----+----------------------+----------+
```
  
## Example 7: Mixed delimiter syntax  

Combine spaces and commas for flexible field specification.
  
```ppl
source=accounts
| fields firstname, account* *name
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----------+----------------+----------+
| firstname | account_number | lastname |
|-----------+----------------+----------|
| Amber     | 1              | Duke     |
| Hattie    | 6              | Bond     |
| Nanette   | 13             | Bates    |
| Dale      | 18             | Adams    |
+-----------+----------------+----------+
```
  
## Example 8: Field deduplication  

Automatically prevents duplicate columns when wildcards expand to already specified fields.
  
```ppl
source=accounts
| fields firstname, *name
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----------+----------+
| firstname | lastname |
|-----------+----------|
| Amber     | Duke     |
| Hattie    | Bond     |
| Nanette   | Bates    |
| Dale      | Adams    |
+-----------+----------+
```
  
Note: Even though `firstname` is explicitly specified and would also match `*name`, it appears only once due to automatic deduplication.
## Example 9: Full wildcard selection  

Select all available fields using `*` or `` `*` ``. This selects all fields defined in the index schema, including fields that may contain null values.
  
```ppl
source=accounts
| fields `*`
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------------+-----------+-----------------+---------+--------+--------+----------+-------+-----+----------------------+----------+
| account_number | firstname | address         | balance | gender | city   | employer | state | age | email                | lastname |
|----------------+-----------+-----------------+---------+--------+--------+----------+-------+-----+----------------------+----------|
| 1              | Amber     | 880 Holmes Lane | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com | Duke     |
+----------------+-----------+-----------------+---------+--------+--------+----------+-------+-----+----------------------+----------+
```
  
Note: The `*` wildcard selects fields based on the index schema, not on data content. Fields with null values are included in the result set. Use backticks `` `*` ` if the plain `*`` doesn't return all expected fields.
## Example 10: Wildcard exclusion  

Remove fields using wildcard patterns with the minus (-) operator.
  
```ppl
source=accounts
| fields - *name
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+
| account_number | address              | balance | gender | city   | employer | state | age | email                 |
|----------------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------|
| 1              | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  |
| 6              | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com |
| 13             | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  |
| 18             | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   |
+----------------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+
```
  
## See Also  

- [table](table.md) - Alias command with identical functionality  