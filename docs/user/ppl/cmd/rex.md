
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

> **Note**: You can set the `max_match` limit in the `plugins.ppl.rex.max_match.limit` cluster setting. For more information, see [SQL settings](../../admin/settings.rst). Setting this limit to a large value is not recommended because it can lead to excessive memory consumption, especially with patterns that match empty strings (for example, `\d*` or `\w*`).


## Example 1: Extract service name and error type from log messages  

The following query extracts the error type from Java exception log messages. Non-matching rows return `null` for the extracted field:
  
```ppl
source=otellogs
| where severityText = 'ERROR'
| rex field=body "(?<errtype>[A-Z][a-zA-Z]+Exception)"
| fields body, errtype
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-------------------------------------------------------------------------+----------------------+
| body                                                                    | errtype              |
|-------------------------------------------------------------------------+----------------------|
| Payment failed: connection timeout to payment gateway after 30000ms     | null                 |
| NullPointerException in CheckoutService.placeOrder at line 142          | NullPointerException |
| Out of memory: Java heap space - shutting down pod payment-6f8d4b-ht7q3 | null                 |
+-------------------------------------------------------------------------+----------------------+
```
  

## Example 2: Extract multiple words using max_match  

The following query uses the `rex` command with the `max_match` parameter to extract multiple words from the `body` field. The extracted field is returned as an array of strings:
  
```ppl
source=otellogs
| where severityText = 'WARN'
| rex field=body "(?<word>[A-Za-z]+)" max_match=3
| fields body, word
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-------------------------------------------------------------------------------------------+----------------------------+
| body                                                                                      | word                       |
|-------------------------------------------------------------------------------------------+----------------------------|
| Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms    | [Slow,query,detected]      |
| Connection pool 80% utilized on database replica db-replica-02                            | [Connection,pool,utilized] |
| SSL certificate for api.example.com expires in 14 days                                    | [SSL,certificate,for]      |
| Rate limit threshold reached: 450/500 requests per minute for API key ending in ...abc789 | [Rate,limit,threshold]     |
+-------------------------------------------------------------------------------------------+----------------------------+
```
  

## Example 3: Replace text using sed mode  

The following query uses `sed` mode to mask IP addresses in log messages for privacy compliance:

```ppl
source=otellogs
| where LIKE(body, '%authenticated%') OR LIKE(body, '%credentials%')
| rex field=body mode=sed "s/[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+/xxx.xxx.xxx.xxx/"
| fields body
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------------------------------------------------------+
| body                                                    |
|---------------------------------------------------------|
| User U300 authenticated via OAuth2 from xxx.xxx.xxx.xxx |
+---------------------------------------------------------+
```

## Example 4: Track match positions using offset_field  

The following query tracks the character positions where matches occur, useful for highlighting matches in a UI:
  
```ppl
source=otellogs
| where severityText = 'ERROR'
| rex field=body "(?<errtype>[A-Z][a-zA-Z]+Exception)" offset_field=pos
| where NOT ISNULL(errtype)
| fields body, errtype, pos
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------------------------------------------------+----------------------+--------------+
| body                                                           | errtype              | pos          |
|----------------------------------------------------------------+----------------------+--------------|
| NullPointerException in CheckoutService.placeOrder at line 142 | NullPointerException | errtype=0-19 |
+----------------------------------------------------------------+----------------------+--------------+
```
  

For detailed Java regex pattern syntax and usage, refer to the official [Java Pattern documentation](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html).

> **Note**: Capture group names cannot contain underscores because of [Java regex](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html) limitations. For example, `(?<error_type>\w+)` is invalid; use `(?<errortype>\w+)` instead.
