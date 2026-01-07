
# rex

The `rex` command extracts fields from a raw text field using regular expression named capture groups. It uses Java regex patterns. For more information, see the [Java regular expression documentation](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html).

## The rex and parse commands compared

The `rex` and [`parse`](parse.md) commands both extract information from text fields using Java regular expressions with named capture groups. The following table compares the capabilities of the `rex` and `parse` commands. 

| Feature | `rex` | `parse` |
| --- | --- | --- |
| Pattern type | Java regex | Java regex |
| Named groups required | Yes | Yes |
| Multiple named groups | Yes | No |
| Multiple matches | Yes | No |
| Text substitution | Yes | No |
| Offset tracking | Yes | No |
| Special characters in group names | No | No |

## Syntax

The `rex` command has the following syntax:

```syntax
rex [mode=<mode>] field=<field> <pattern> [max_match=<int>] [offset_field=<string>]
```

## Parameters

The `rex` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `field` | Required | The field to extract data from. The field must be a string. |
| `<pattern>` | Required | The regular expression pattern with named capture groups used to extract new fields. The pattern must contain at least one named capture group using the `(?<name>pattern)` syntax. Group names must start with a letter and contain only letters and digits. |
| `mode` | Optional | The pattern-matching mode. Valid values are `extract` and `sed`. The `extract` mode creates new fields from regular expression named capture groups. The `sed` mode performs text substitution using sed-style patterns (supports `s/pattern/replacement/` with flags, `y/from_chars/to_chars/` transliteration, and backreferences). |
| `max_match` | Optional | The maximum number of matches to extract. If the value is greater than `1`, the extracted fields are returned as arrays. A value of `0` indicates unlimited matches; however, the effective number of matches is automatically limited by the configured maximum. The default maximum is `10` and can be configured using `plugins.ppl.rex.max_match.limit` (see the [note](rex.md/#note)). Default is `1`. |
| `offset_field` | Optional | Valid in `extract` mode only. The name of the field in which to store the character offset positions of the matches. |

<p id="note"></p>

You can set the `max_match` limit in the `plugins.ppl.rex.max_match.limit` cluster setting. For more information, see [SQL settings](../../admin/settings.rst). Setting this limit to a large value is not recommended because it can lead to excessive memory consumption, especially with patterns that match empty strings (for example, `\d*` or `\w*`).
{: .note}


## Example 1: Basic text extraction  

The following query extracts the username and domain from email addresses using named capture groups. Both extracted fields are returned as strings:
  
```ppl
source=accounts
| rex field=email "(?<username>[^@]+)@(?<domain>[^.]+)"
| fields email, username, domain
| head 2
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+-----------------------+------------+--------+
| email                 | username   | domain |
|-----------------------+------------+--------|
| amberduke@pyrami.com  | amberduke  | pyrami |
| hattiebond@netagy.com | hattiebond | netagy |
+-----------------------+------------+--------+
```
  

## Example 2: Handle non-matching patterns  

The following query shows that the rex command returns all events, setting extracted fields to null for non-matching patterns. When matches are found, the extracted fields are returned as strings:
  
```ppl
source=accounts
| rex field=email "(?<user>[^@]+)@(?<domain>gmail\\.com)"
| fields email, user, domain
| head 2
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+-----------------------+------+--------+
| email                 | user | domain |
|-----------------------+------+--------|
| amberduke@pyrami.com  | null | null   |
| hattiebond@netagy.com | null | null   |
+-----------------------+------+--------+
```
  

## Example 3: Extract multiple words using max_match  

The following query uses the `rex` command with the `max_match` parameter to extract multiple words from the `address` field. The extracted field is returned as an array of strings:
  
```ppl
source=accounts
| rex field=address "(?<words>[A-Za-z]+)" max_match=2
| fields address, words
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------------+------------------+
| address            | words            |
|--------------------+------------------|
| 880 Holmes Lane    | [Holmes,Lane]    |
| 671 Bristol Street | [Bristol,Street] |
| 789 Madison Street | [Madison,Street] |
+--------------------+------------------+
```
  

## Example 4: Replace text using sed mode  

The following query uses the `rex` command in `sed` mode to replace email domains through text substitution. The extracted field is returned as a string:
  
```ppl
source=accounts
| rex field=email mode=sed "s/@.*/@company.com/"
| fields email
| head 2
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+------------------------+
| email                  |
|------------------------|
| amberduke@company.com  |
| hattiebond@company.com |
+------------------------+
```
  

## Example 5: Track match positions using offset_field  

The following query tracks the character positions where matches occur. The extracted fields are returned as strings, and the `offset_field` is also returned as a string:
  
```ppl
source=accounts
| rex field=email "(?<username>[^@]+)@(?<domain>[^.]+)" offset_field=matchpos
| fields email, username, domain, matchpos
| head 2
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+-----------------------+------------+--------+---------------------------+
| email                 | username   | domain | matchpos                  |
|-----------------------+------------+--------+---------------------------|
| amberduke@pyrami.com  | amberduke  | pyrami | domain=10-15&username=0-8 |
| hattiebond@netagy.com | hattiebond | netagy | domain=11-16&username=0-9 |
+-----------------------+------------+--------+---------------------------+
```
  

## Example 6: Extract a complex email pattern  

The following query extracts complete email components, including the top-level domain. All extracted fields are returned as strings:
  
```ppl
source=accounts
| rex field=email "(?<user>[a-zA-Z0-9._%+-]+)@(?<domain>[a-zA-Z0-9.-]+)\\.(?<tld>[a-zA-Z]{2,})"
| fields email, user, domain, tld
| head 2
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+-----------------------+------------+--------+-----+
| email                 | user       | domain | tld |
|-----------------------+------------+--------+-----|
| amberduke@pyrami.com  | amberduke  | pyrami | com |
| hattiebond@netagy.com | hattiebond | netagy | com |
+-----------------------+------------+--------+-----+
```
  

## Example 7: Chain multiple rex commands  

The following query extracts initial letters from both first and last names. All extracted fields are returned as strings:
  
```ppl
source=accounts
| rex field=firstname "(?<firstinitial>^.)"
| rex field=lastname "(?<lastinitial>^.)"
| fields firstname, lastname, firstinitial, lastinitial
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----------+----------+--------------+-------------+
| firstname | lastname | firstinitial | lastinitial |
|-----------+----------+--------------+-------------|
| Amber     | Duke     | A            | D           |
| Hattie    | Bond     | H            | B           |
| Nanette   | Bates    | N            | B           |
+-----------+----------+--------------+-------------+
```
  

## Example 8: Capture group naming restrictions  

The following query shows naming restrictions for capture groups. Group names cannot contain underscores because of [Java regex](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html) limitations.

**Invalid PPL query with underscores**:
  
```ppl
source=accounts
| rex field=email "(?<user_name>[^@]+)@(?<email_domain>[^.]+)"
| fields email, user_name, email_domain
```
  
The query returns the following results:
  
```text
{'reason': 'Invalid Query', 'details': "Invalid capture group name 'user_name'. Java regex group names must start with a letter and contain only letters and digits.", 'type': 'IllegalArgumentException'}
Error: Query returned no data
```

**Correct PPL query without underscores**:
  
```ppl
source=accounts
| rex field=email "(?<username>[^@]+)@(?<emaildomain>[^.]+)"
| fields email, username, emaildomain
| head 2
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+-----------------------+------------+-------------+
| email                 | username   | emaildomain |
|-----------------------+------------+-------------|
| amberduke@pyrami.com  | amberduke  | pyrami      |
| hattiebond@netagy.com | hattiebond | netagy      |
+-----------------------+------------+-------------+
```
  

## Example 9: max_match limit enforcement  

The following query shows the `max_match` limit protection mechanism. When `max_match` is set to `0` (unlimited), the system automatically enforces a maximum limit on the number of matches to prevent memory exhaustion.

**PPL query with `max_match=0` automatically limited to the default of 10**:
  
```ppl
source=accounts
| rex field=address "(?<digit>\\d*)" max_match=0
| eval digit_count=array_length(digit)
| fields address, digit_count
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------+-------------+
| address         | digit_count |
|-----------------+-------------|
| 880 Holmes Lane | 10          |
+-----------------+-------------+
```

**A PPL query exceeding the configured limit results in an error**:
  
```ppl
source=accounts
| rex field=address "(?<digit>\\d*)" max_match=100
| fields address, digit
| head 1
```
  
The query returns the following results:
  
```text
{'reason': 'Invalid Query', 'details': 'Rex command max_match value (100) exceeds the configured limit (10). Consider using a smaller max_match value or adjust the plugins.ppl.rex.max_match.limit setting.', 'type': 'IllegalArgumentException'}
Error: Query returned no data
```

For detailed Java regex pattern syntax and usage, refer to the [official Java Pattern documentation](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html).
