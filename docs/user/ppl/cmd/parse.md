
# parse

The `parse` command extracts information from a text field using a regular expression and adds the extracted information to the search results. It uses Java regex patterns. For more information, see the [Java regular expression documentation](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html).

## The rex and parse commands compared

The `rex` and `parse` commands both extract information from text fields using Java regular expressions with named capture groups. To compare the capabilities of the `rex` and `parse` commands, see the [`rex` command documentation](rex.md).

## Syntax

The `parse` command has the following syntax:

```syntax
parse <field> <pattern>
```

## Parameters

The `parse` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The text field to parse. |
| `<pattern>` | Required | The regular expression pattern used to extract new fields from the specified text field. If a field with the same name already exists, its values are replaced. |

## Regular expression

The regular expression pattern is used to match the whole text field of each document based on the [Java regular expression syntax](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html). Each named capture group in the expression becomes a new `STRING` field.  

## Example 1: Create a new field  

The following query extracts the hostname from email addresses. The regex pattern `.+@(?<host>.+)` captures all characters after the `@` symbol and creates a new `host` field. When parsing a null field, the result is an empty string:
  
```ppl
source=accounts
| parse email '.+@(?<host>.+)'
| fields email, host
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------------------+------------+
| email                 | host       |
|-----------------------+------------|
| amberduke@pyrami.com  | pyrami.com |
| hattiebond@netagy.com | netagy.com |
| null                  |            |
| daleadams@boink.com   | boink.com  |
+-----------------------+------------+
```
  

## Example 2: Override an existing field  

The following query replaces the `address` field with only the street name, removing the street number. The regex pattern `\d+ (?<address>.+)` matches digits followed by a space, then captures the remaining text as the new `address` value:
  
```ppl
source=accounts
| parse address '\d+ (?<address>.+)'
| fields address
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+------------------+
| address          |
|------------------|
| Holmes Lane      |
| Bristol Street   |
| Madison Street   |
| Hutchinson Court |
+------------------+
```
  

## Example 3: Parse, filter, and sort address components

The following query extracts street numbers and names from addresses, then filters for street numbers greater than 500 and sorts them numerically. The regex pattern `(?<streetNumber>\d+) (?<street>.+)` captures the numeric part as `streetNumber` and the remaining text as `street`:
  
```ppl
source=accounts
| parse address '(?<streetNumber>\d+) (?<street>.+)'
| where cast(streetNumber as int) > 500
| sort num(streetNumber)
| fields streetNumber, street
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+----------------+
| streetNumber | street         |
|--------------+----------------|
| 671          | Bristol Street |
| 789          | Madison Street |
| 880          | Holmes Lane    |
+--------------+----------------+
```
  

## Limitations

The `parse` command has the following limitations:

- Fields created by the `parse` command cannot be parsed again. For example, the following command does not function as intended:

    ```sql
    source=accounts | parse address '\d+ (?<street>.+)' | parse street '\w+ (?<road>\w+)'
    ```

- Fields created by the `parse` command cannot be overridden by other commands. For example, in the following query, the `where` clause does not match any documents because `street` cannot be overridden:

    ```sql
    source=accounts | parse address '\d+ (?<street>.+)' | eval street='1' | where street='1'
    ```

- The source text field used by the `parse` command cannot be overridden. For example, in the following query, the `street` field is not parsed correctly because `address` is overridden:

    ```sql
    source=accounts | parse address '\d+ (?<street>.+)' | eval address='1'
    ```

- Fields created by the `parse` command cannot be filtered or sorted after they are used in the `stats` command. For example, in the following query, the `where` clause does not function as intended:

    ```sql
    source=accounts | parse email '.+@(?<host>.+)' | stats avg(age) by host | where host=pyrami.com
    ```

- Fields created by the `parse` command do not appear in the final results unless the original source field is included in the `fields` command. For example, the following query does not return the parsed field `host` unless the source field `email` is explicitly included:

    ```sql
    source=accounts | parse email '.+@(?<host>.+)' | fields email, host
    ```


