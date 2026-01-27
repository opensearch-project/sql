# String Functions  

## CONCAT  

### Description  

Usage: `CONCAT(str1, str2, ...., str_9)` adds up to 9 strings together.
**Argument type:** `STRING, STRING, ...., STRING`
**Return type:** `STRING`
### Example
  
```ppl
source=people
| eval `CONCAT('hello', 'world')` = CONCAT('hello', 'world'), `CONCAT('hello ', 'whole ', 'world', '!')` = CONCAT('hello ', 'whole ', 'world', '!')
| fields `CONCAT('hello', 'world')`, `CONCAT('hello ', 'whole ', 'world', '!')`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------------------------+------------------------------------------+
| CONCAT('hello', 'world') | CONCAT('hello ', 'whole ', 'world', '!') |
|--------------------------+------------------------------------------|
| helloworld               | hello whole world!                       |
+--------------------------+------------------------------------------+
```
  
## CONCAT_WS  

### Description  

Usage: `CONCAT_WS(sep, str1, str2)` returns str1 concatenated with str2 using sep as a separator between them.
**Argument type:** `STRING, STRING, STRING`
**Return type:** `STRING`
### Example
  
```ppl
source=people
| eval `CONCAT_WS(',', 'hello', 'world')` = CONCAT_WS(',', 'hello', 'world')
| fields `CONCAT_WS(',', 'hello', 'world')`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------------------------------+
| CONCAT_WS(',', 'hello', 'world') |
|----------------------------------|
| hello,world                      |
+----------------------------------+
```
  
## LENGTH  

### Description  

Specifications:
1. LENGTH(STRING) -> INTEGER  
  
Usage: `length(str)` returns length of string measured in bytes.
**Argument type:** `STRING`
**Return type:** `INTEGER`
### Example
  
```ppl
source=people
| eval `LENGTH('helloworld')` = LENGTH('helloworld')
| fields `LENGTH('helloworld')`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------------------+
| LENGTH('helloworld') |
|----------------------|
| 10                   |
+----------------------+
```
  
## LIKE  

### Description  

Usage: `like(string, PATTERN[, case_sensitive])` return true if the string match the PATTERN. `case_sensitive` is optional. When set to `true`, PATTERN is **case-sensitive**. **Default:** Determined by `plugins.ppl.syntax.legacy.preferred`.
 * When `plugins.ppl.syntax.legacy.preferred=true`, `case_sensitive` defaults to `false`  
 * When `plugins.ppl.syntax.legacy.preferred=false`, `case_sensitive` defaults to `true`  
  
There are two wildcards often used in conjunction with the LIKE operator:
* `%` - The percent sign represents zero, one, or multiple characters  
* `_` - The underscore represents a single character  
  
**Argument type:** `STRING, STRING [, BOOLEAN]`
**Return type:** `INTEGER`
### Example
  
```ppl
source=people
| eval `LIKE('hello world', '_ello%')` = LIKE('hello world', '_ello%'), `LIKE('hello world', '_ELLo%', true)` = LIKE('hello world', '_ELLo%', true), `LIKE('hello world', '_ELLo%', false)` = LIKE('hello world', '_ELLo%', false)
| fields `LIKE('hello world', '_ello%')`, `LIKE('hello world', '_ELLo%', true)`, `LIKE('hello world', '_ELLo%', false)`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-------------------------------+-------------------------------------+--------------------------------------+
| LIKE('hello world', '_ello%') | LIKE('hello world', '_ELLo%', true) | LIKE('hello world', '_ELLo%', false) |
|-------------------------------+-------------------------------------+--------------------------------------|
| True                          | False                               | True                                 |
+-------------------------------+-------------------------------------+--------------------------------------+
```
  
Limitation: The pushdown of the LIKE function to a DSL wildcard query is supported only for keyword fields.
## ILIKE  

### Description  

Usage: `ilike(string, PATTERN)` return true if the string match the PATTERN, PATTERN is **case-insensitive**.
There are two wildcards often used in conjunction with the ILIKE operator:
* `%` - The percent sign represents zero, one, or multiple characters  
* `_` - The underscore represents a single character  
  
**Argument type:** `STRING, STRING`
**Return type:** `INTEGER`
### Example
  
```ppl
source=people
| eval `ILIKE('hello world', '_ELLo%')` = ILIKE('hello world', '_ELLo%')
| fields `ILIKE('hello world', '_ELLo%')`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------------------------------+
| ILIKE('hello world', '_ELLo%') |
|--------------------------------|
| True                           |
+--------------------------------+
```
  
Limitation: The pushdown of the ILIKE function to a DSL wildcard query is supported only for keyword fields.
## LOCATE  

### Description  

Usage: `locate(substr, str[, start])` returns the position of the first occurrence of substring substr in string str, starting searching from position start. If start is not specified, it defaults to 1 (the beginning of the string). Returns 0 if substr is not found. If any argument is NULL, the function returns NULL.
**Argument type:** `STRING, STRING[, INTEGER]`
**Return type:** `INTEGER`
### Example
  
```ppl
source=people
| eval `LOCATE('world', 'helloworld')` = LOCATE('world', 'helloworld'), `LOCATE('invalid', 'helloworld')` = LOCATE('invalid', 'helloworld'), `LOCATE('world', 'helloworld', 6)` = LOCATE('world', 'helloworld', 6)
| fields `LOCATE('world', 'helloworld')`, `LOCATE('invalid', 'helloworld')`, `LOCATE('world', 'helloworld', 6)`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-------------------------------+---------------------------------+----------------------------------+
| LOCATE('world', 'helloworld') | LOCATE('invalid', 'helloworld') | LOCATE('world', 'helloworld', 6) |
|-------------------------------+---------------------------------+----------------------------------|
| 6                             | 0                               | 6                                |
+-------------------------------+---------------------------------+----------------------------------+
```
  
## LOWER  

### Description  

Usage: `lower(string)` converts the string to lowercase.
**Argument type:** `STRING`
**Return type:** `STRING`
### Example
  
```ppl
source=people
| eval `LOWER('helloworld')` = LOWER('helloworld'), `LOWER('HELLOWORLD')` = LOWER('HELLOWORLD')
| fields `LOWER('helloworld')`, `LOWER('HELLOWORLD')`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------------------+---------------------+
| LOWER('helloworld') | LOWER('HELLOWORLD') |
|---------------------+---------------------|
| helloworld          | helloworld          |
+---------------------+---------------------+
```
  
## LTRIM  

### Description  

Usage: `ltrim(str)` trims leading space characters from the string.
**Argument type:** `STRING`
**Return type:** `STRING`
### Example
  
```ppl
source=people
| eval `LTRIM('   hello')` = LTRIM('   hello'), `LTRIM('hello   ')` = LTRIM('hello   ')
| fields `LTRIM('   hello')`, `LTRIM('hello   ')`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-------------------+-------------------+
| LTRIM('   hello') | LTRIM('hello   ') |
|-------------------+-------------------|
| hello             | hello             |
+-------------------+-------------------+
```
  
## POSITION  

### Description  

Usage: The syntax POSITION(substr IN str) returns the position of the first occurrence of substring substr in string str. Returns 0 if substr is not in str. Returns NULL if any argument is NULL.
**Argument type:** `STRING, STRING`
Return type INTEGER
(STRING IN STRING) -> INTEGER
### Example
  
```ppl
source=people
| eval `POSITION('world' IN 'helloworld')` = POSITION('world' IN 'helloworld'), `POSITION('invalid' IN 'helloworld')`= POSITION('invalid' IN 'helloworld')
| fields `POSITION('world' IN 'helloworld')`, `POSITION('invalid' IN 'helloworld')`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------------------------------+-------------------------------------+
| POSITION('world' IN 'helloworld') | POSITION('invalid' IN 'helloworld') |
|-----------------------------------+-------------------------------------|
| 6                                 | 0                                   |
+-----------------------------------+-------------------------------------+
```
  
## REPLACE  

### Description  

Usage: `replace(str, pattern, replacement)` returns a string with all occurrences of the pattern replaced by the replacement string in str. If any argument is NULL, the function returns NULL.
**Regular Expression Support**: The pattern argument supports Java regex syntax, including:
**Argument type:** `STRING, STRING (regex pattern), STRING (replacement)`
**Return type:** `STRING`
**Important - Regex Special Characters**: The pattern is interpreted as a regular expression. Characters like `.`, `*`, `+`, `[`, `]`, `(`, `)`, `{`, `}`, `^`, `$`, `|`, `?`, and `\` have special meaning in regex. To match them literally, escape with backslashes:
* To match `example.com`: use `'example\\.com'` (escape the dots)  
* To match `value*`: use `'value\\*'` (escape the asterisk)  
* To match `price+tax`: use `'price\\+tax'` (escape the plus)  
  
For strings with many special characters, use `\\Q...\\E` to quote the entire literal string (e.g., `'\\Qhttps://example.com/path?id=123\\E'` matches that exact URL).
Literal String Replacement Examples
  
```ppl
source=people
| eval `REPLACE('helloworld', 'world', 'universe')` = REPLACE('helloworld', 'world', 'universe'), `REPLACE('helloworld', 'invalid', 'universe')` = REPLACE('helloworld', 'invalid', 'universe')
| fields `REPLACE('helloworld', 'world', 'universe')`, `REPLACE('helloworld', 'invalid', 'universe')`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------------------------------------------+----------------------------------------------+
| REPLACE('helloworld', 'world', 'universe') | REPLACE('helloworld', 'invalid', 'universe') |
|--------------------------------------------+----------------------------------------------|
| hellouniverse                              | helloworld                                   |
+--------------------------------------------+----------------------------------------------+
```
  
Escaping Special Characters Examples
  
```ppl
source=people
| eval `Replace domain` = REPLACE('api.example.com', 'example\\.com', 'newsite.org'), `Replace with quote` = REPLACE('https://api.example.com/v1', '\\Qhttps://api.example.com\\E', 'http://localhost:8080')
| fields `Replace domain`, `Replace with quote`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------------+--------------------------+
| Replace domain  | Replace with quote       |
|-----------------+--------------------------|
| api.newsite.org | http://localhost:8080/v1 |
+-----------------+--------------------------+
```
  
Regex Pattern Examples
  
```ppl
source=people
| eval `Remove digits` = REPLACE('test123', '\\d+', ''), `Collapse spaces` = REPLACE('hello  world', ' +', ' '), `Remove special` = REPLACE('hello@world!', '[^a-zA-Z]', '')
| fields `Remove digits`, `Collapse spaces`, `Remove special`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------------+-----------------+----------------+
| Remove digits | Collapse spaces | Remove special |
|---------------+-----------------+----------------|
| test          | hello world     | helloworld     |
+---------------+-----------------+----------------+
```
  
Capture Group and Backreference Examples
  
```ppl
source=people
| eval `Swap date` = REPLACE('1/14/2023', '^(\\d{1,2})/(\\d{1,2})/', '$2/$1/'), `Reverse words` = REPLACE('Hello World', '(\\w+) (\\w+)', '$2 $1'), `Extract domain` = REPLACE('user@example.com', '.*@(.+)', '$1')
| fields `Swap date`, `Reverse words`, `Extract domain`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------+---------------+----------------+
| Swap date | Reverse words | Extract domain |
|-----------+---------------+----------------|
| 14/1/2023 | World Hello   | example.com    |
+-----------+---------------+----------------+
```
  
Advanced Regex Examples
  
```ppl
source=people
| eval `Clean phone` = REPLACE('(555) 123-4567', '[^0-9]', ''), `Remove vowels` = REPLACE('hello world', '[aeiou]', ''), `Add prefix` = REPLACE('test', '^', 'pre_')
| fields `Clean phone`, `Remove vowels`, `Add prefix`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-------------+---------------+------------+
| Clean phone | Remove vowels | Add prefix |
|-------------+---------------+------------|
| 5551234567  | hll wrld      | pre_test   |
+-------------+---------------+------------+
```
  
**Note**: When using regex patterns in PPL queries:
* Backslashes must be escaped (use `\\` instead of `\`) - e.g., `\\d` for digit pattern, `\\w+` for word characters  
* Backreferences support both PCRE-style (`\1`, `\2`, etc.) and Java-style (`$1`, `$2`, etc.) syntax. PCRE-style backreferences are automatically converted to Java-style internally.  
  
## REVERSE  

### Description  

Usage: `REVERSE(str)` returns reversed string of the string supplied as an argument.
**Argument type:** `STRING`
**Return type:** `STRING`
### Example
  
```ppl
source=people
| eval `REVERSE('abcde')` = REVERSE('abcde')
| fields `REVERSE('abcde')`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+------------------+
| REVERSE('abcde') |
|------------------|
| edcba            |
+------------------+
```
  
## RIGHT  

### Description  

Usage: `right(str, len)` returns the rightmost len characters from the string str, or NULL if any argument is NULL.
**Argument type:** `STRING, INTEGER`
**Return type:** `STRING`
### Example
  
```ppl
source=people
| eval `RIGHT('helloworld', 5)` = RIGHT('helloworld', 5), `RIGHT('HELLOWORLD', 0)` = RIGHT('HELLOWORLD', 0)
| fields `RIGHT('helloworld', 5)`, `RIGHT('HELLOWORLD', 0)`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+------------------------+------------------------+
| RIGHT('helloworld', 5) | RIGHT('HELLOWORLD', 0) |
|------------------------+------------------------|
| world                  |                        |
+------------------------+------------------------+
```
  
## RTRIM  

### Description  

Usage: `rtrim(str)` trims trailing space characters from the string.
**Argument type:** `STRING`
**Return type:** `STRING`
### Example
  
```ppl
source=people
| eval `RTRIM('   hello')` = RTRIM('   hello'), `RTRIM('hello   ')` = RTRIM('hello   ')
| fields `RTRIM('   hello')`, `RTRIM('hello   ')`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-------------------+-------------------+
| RTRIM('   hello') | RTRIM('hello   ') |
|-------------------+-------------------|
| hello             | hello             |
+-------------------+-------------------+
```
  
## SUBSTRING  

### Description  

Usage: `substring(str, start)` or substring(str, start, length) returns substring using start and length. With no length, entire string from start is returned.
**Argument type:** `STRING, INTEGER, INTEGER`
**Return type:** `STRING`
Synonyms: SUBSTR
### Example
  
```ppl
source=people
| eval `SUBSTRING('helloworld', 5)` = SUBSTRING('helloworld', 5), `SUBSTRING('helloworld', 5, 3)` = SUBSTRING('helloworld', 5, 3)
| fields `SUBSTRING('helloworld', 5)`, `SUBSTRING('helloworld', 5, 3)`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------------------------+-------------------------------+
| SUBSTRING('helloworld', 5) | SUBSTRING('helloworld', 5, 3) |
|----------------------------+-------------------------------|
| oworld                     | owo                           |
+----------------------------+-------------------------------+
```
  
## TRIM  

### Description  

Argument Type: STRING
**Return type:** `STRING`
### Example
  
```ppl
source=people
| eval `TRIM('   hello')` = TRIM('   hello'), `TRIM('hello   ')` = TRIM('hello   ')
| fields `TRIM('   hello')`, `TRIM('hello   ')`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+------------------+------------------+
| TRIM('   hello') | TRIM('hello   ') |
|------------------+------------------|
| hello            | hello            |
+------------------+------------------+
```
  
## UPPER  

### Description  

Usage: `upper(string)` converts the string to uppercase.
**Argument type:** `STRING`
**Return type:** `STRING`
### Example
  
```ppl
source=people
| eval `UPPER('helloworld')` = UPPER('helloworld'), `UPPER('HELLOWORLD')` = UPPER('HELLOWORLD')
| fields `UPPER('helloworld')`, `UPPER('HELLOWORLD')`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------------------+---------------------+
| UPPER('helloworld') | UPPER('HELLOWORLD') |
|---------------------+---------------------|
| HELLOWORLD          | HELLOWORLD          |
+---------------------+---------------------+
```
  
## REGEXP_REPLACE  

### Description  

Usage: `regexp_replace(str, pattern, replacement)` replace all substrings of the string value that match pattern with replacement and returns modified string value.
**Argument type:** `STRING, STRING, STRING`
**Return type:** `STRING`
Synonyms: [REPLACE](#replace)
### Example
  
```ppl
source=people
| eval `DOMAIN` = REGEXP_REPLACE('https://opensearch.org/downloads/', '^https?://(?:www\.)?([^/]+)/.*$', '\1')
| fields `DOMAIN`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------------+
| DOMAIN         |
|----------------|
| opensearch.org |
+----------------+
```
  