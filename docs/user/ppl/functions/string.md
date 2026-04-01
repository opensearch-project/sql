# String functions

The following string functions are supported in PPL.

## CONCAT

**Usage**: `CONCAT(str1, str2, ...., str_9)`

Concatenates up to 9 strings.

**Parameters**:

- `str1, str2, ..., str_9` (Required): Up to 9 strings to concatenate.

**Return type**: `STRING`

### Example
  
```ppl
source=people
| eval `CONCAT('hello', 'world')` = CONCAT('hello', 'world'), `CONCAT('hello ', 'whole ', 'world', '!')` = CONCAT('hello ', 'whole ', 'world', '!')
| fields `CONCAT('hello', 'world')`, `CONCAT('hello ', 'whole ', 'world', '!')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------------+------------------------------------------+
| CONCAT('hello', 'world') | CONCAT('hello ', 'whole ', 'world', '!') |
|--------------------------+------------------------------------------|
| helloworld               | hello whole world!                       |
+--------------------------+------------------------------------------+
```
  
## CONCAT_WS

**Usage**: `CONCAT_WS(sep, str1, str2)`

Returns `str1` concatenated with `str2`, using `sep` as a separator between them.

**Parameters**:

- `sep` (Required): The separator string to place between concatenated strings.
- `str1` (Required): The first string to concatenate.
- `str2` (Required): The second string to concatenate.

**Return type**: `STRING`

### Example
  
```ppl
source=people
| eval `CONCAT_WS(',', 'hello', 'world')` = CONCAT_WS(',', 'hello', 'world')
| fields `CONCAT_WS(',', 'hello', 'world')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------------------+
| CONCAT_WS(',', 'hello', 'world') |
|----------------------------------|
| hello,world                      |
+----------------------------------+
```
  
## LENGTH

**Usage**: `length(str)`

Returns the length of the string measured in bytes.

**Parameters**:

- `str` (Required): The string for which to calculate the length.

**Return type**: `INTEGER`

### Example
  
```ppl
source=people
| eval `LENGTH('helloworld')` = LENGTH('helloworld')
| fields `LENGTH('helloworld')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------+
| LENGTH('helloworld') |
|----------------------|
| 10                   |
+----------------------+
```
  
## LIKE

**Usage**: `like(string, PATTERN[, case_sensitive])`

Returns `TRUE` if the string matches the pattern, `FALSE` otherwise.

**Parameters**:

- `string` (Required): The string to match against the pattern.
- `PATTERN` (Required): The pattern to match, supporting wildcards.
- `case_sensitive` (Optional): Whether the pattern matching is case-sensitive. Default determined by `plugins.ppl.syntax.legacy.preferred`.

**Wildcards**:
- `%` - Represents zero, one, or multiple characters.
- `_` - Represents a single character.

**Configuration**:
- When `plugins.ppl.syntax.legacy.preferred=true`, `case_sensitive` defaults to `false`.
- When `plugins.ppl.syntax.legacy.preferred=false`, `case_sensitive` defaults to `true`.

**Return type**: `BOOLEAN`

### Example
  
```ppl
source=people
| eval `LIKE('hello world', '_ello%')` = LIKE('hello world', '_ello%'), `LIKE('hello world', '_ELLo%', true)` = LIKE('hello world', '_ELLo%', true), `LIKE('hello world', '_ELLo%', false)` = LIKE('hello world', '_ELLo%', false)
| fields `LIKE('hello world', '_ello%')`, `LIKE('hello world', '_ELLo%', true)`, `LIKE('hello world', '_ELLo%', false)`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------------+-------------------------------------+--------------------------------------+
| LIKE('hello world', '_ello%') | LIKE('hello world', '_ELLo%', true) | LIKE('hello world', '_ELLo%', false) |
|-------------------------------+-------------------------------------+--------------------------------------|
| True                          | False                               | True                                 |
+-------------------------------+-------------------------------------+--------------------------------------+
```
  
Limitation: The pushdown of the `LIKE` function to a DSL wildcard query is supported only for keyword fields.

## ILIKE

**Usage**: `ilike(string, PATTERN)`

Returns `TRUE` if the string matches the pattern (case-insensitive), `FALSE` otherwise.

**Parameters**:

- `string` (Required): The string to match against the pattern.
- `PATTERN` (Required): The case-insensitive pattern to match, supporting wildcards.

**Wildcards**:
- `%` - Represents zero, one, or multiple characters.
- `_` - Represents a single character.

**Return type**: `BOOLEAN`

### Example
  
```ppl
source=people
| eval `ILIKE('hello world', '_ELLo%')` = ILIKE('hello world', '_ELLo%')
| fields `ILIKE('hello world', '_ELLo%')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------------------+
| ILIKE('hello world', '_ELLo%') |
|--------------------------------|
| True                           |
+--------------------------------+
```
  
Limitation: The pushdown of the `ILIKE` function to a DSL wildcard query is supported only for keyword fields.

## LOCATE

**Usage**: `locate(substr, str[, start])`

Returns the position of the first occurrence of `substr` in `str`, starting at position `start`. If `start` is not specified, the search begins at position 1. Returns 0 if `substr` is not found. If any argument is `NULL`, the function returns `NULL`.

**Parameters**:

- `substr` (Required): The substring to search for.
- `str` (Required): The string to search within.
- `start` (Optional): The position to start searching from. Defaults to 1.

**Return type**: `INTEGER`

### Example
  
```ppl
source=people
| eval `LOCATE('world', 'helloworld')` = LOCATE('world', 'helloworld'), `LOCATE('invalid', 'helloworld')` = LOCATE('invalid', 'helloworld'), `LOCATE('world', 'helloworld', 6)` = LOCATE('world', 'helloworld', 6)
| fields `LOCATE('world', 'helloworld')`, `LOCATE('invalid', 'helloworld')`, `LOCATE('world', 'helloworld', 6)`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------------+---------------------------------+----------------------------------+
| LOCATE('world', 'helloworld') | LOCATE('invalid', 'helloworld') | LOCATE('world', 'helloworld', 6) |
|-------------------------------+---------------------------------+----------------------------------|
| 6                             | 0                               | 6                                |
+-------------------------------+---------------------------------+----------------------------------+
```
  
## LOWER

**Usage**: `lower(string)`

Converts the string to lowercase.

**Parameters**:

- `string` (Required): The string to convert to lowercase.

**Return type**: `STRING`

### Example
  
```ppl
source=people
| eval `LOWER('helloworld')` = LOWER('helloworld'), `LOWER('HELLOWORLD')` = LOWER('HELLOWORLD')
| fields `LOWER('helloworld')`, `LOWER('HELLOWORLD')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------+---------------------+
| LOWER('helloworld') | LOWER('HELLOWORLD') |
|---------------------+---------------------|
| helloworld          | helloworld          |
+---------------------+---------------------+
```
  
## LTRIM

**Usage**: `ltrim(str)`

Trims leading space characters from the string.

**Parameters**:

- `str` (Required): The string from which to remove leading spaces.

**Return type**: `STRING`

### Example
  
```ppl
source=people
| eval `LTRIM('   hello')` = LTRIM('   hello'), `LTRIM('hello   ')` = LTRIM('hello   ')
| fields `LTRIM('   hello')`, `LTRIM('hello   ')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------+-------------------+
| LTRIM('   hello') | LTRIM('hello   ') |
|-------------------+-------------------|
| hello             | hello             |
+-------------------+-------------------+
```
  
## POSITION

**Usage**: `POSITION(substr IN str)`

Returns the position of the first occurrence of `substr` in `str`. Returns 0 if `substr` is not found. Returns `NULL` if any argument is `NULL`.

**Parameters**:

- `substr` (Required): The substring to search for.
- `str` (Required): The string to search within.

**Return type**: `INTEGER`

### Example
  
```ppl
source=people
| eval `POSITION('world' IN 'helloworld')` = POSITION('world' IN 'helloworld'), `POSITION('invalid' IN 'helloworld')`= POSITION('invalid' IN 'helloworld')
| fields `POSITION('world' IN 'helloworld')`, `POSITION('invalid' IN 'helloworld')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------------+-------------------------------------+
| POSITION('world' IN 'helloworld') | POSITION('invalid' IN 'helloworld') |
|-----------------------------------+-------------------------------------|
| 6                                 | 0                                   |
+-----------------------------------+-------------------------------------+
```
  
## REPLACE

**Usage**: `replace(str, pattern, replacement)`

Returns a string in which all occurrences of the pattern in `str` are replaced with the replacement string. Returns `NULL` if any argument is `NULL`.

**Parameters**:

- `str` (Required): The input string to perform replacements on.
- `pattern` (Required): The regex pattern to match (supports Java regex syntax).
- `replacement` (Required): The replacement string.

**Return type**: `STRING`

**Regular expression support**: The pattern argument supports Java regex syntax.

**Regular expression special characters**: The pattern is interpreted as a regular expression (regex). The following characters have special meaning in regex: `.`, `*`, `+`, `[`, `]`, `(`, `)`, `{`, `}`, `^`, `$`, `|`, `?`, and `\`. To match these characters literally, escape them with backslashes:
- `example.com` becomes `'example\\.com'` (escaped dots).
- `value*` becomes `'value\\*'` (escaped asterisk).
- `price+tax` becomes `'price\\+tax'` (escaped plus).

Strings containing multiple special characters can be quoted using `\\Q...\\E` to treat the entire string literally. For example, `'\\Qhttps://example.com/path?id=123\\E'` treats the entire URL as a literal string.

### Example: Literal string replacement
  
```ppl
source=people
| eval `REPLACE('helloworld', 'world', 'universe')` = REPLACE('helloworld', 'world', 'universe'), `REPLACE('helloworld', 'invalid', 'universe')` = REPLACE('helloworld', 'invalid', 'universe')
| fields `REPLACE('helloworld', 'world', 'universe')`, `REPLACE('helloworld', 'invalid', 'universe')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------------------------------+----------------------------------------------+
| REPLACE('helloworld', 'world', 'universe') | REPLACE('helloworld', 'invalid', 'universe') |
|--------------------------------------------+----------------------------------------------|
| hellouniverse                              | helloworld                                   |
+--------------------------------------------+----------------------------------------------+
```
  
### Example: Escaping special characters
  
```ppl
source=people
| eval `Replace domain` = REPLACE('api.example.com', 'example\\.com', 'newsite.org'), `Replace with quote` = REPLACE('https://api.example.com/v1', '\\Qhttps://api.example.com\\E', 'http://localhost:8080')
| fields `Replace domain`, `Replace with quote`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------+--------------------------+
| Replace domain  | Replace with quote       |
|-----------------+--------------------------|
| api.newsite.org | http://localhost:8080/v1 |
+-----------------+--------------------------+
```
  
### Example: Regex patterns
  
```ppl
source=people
| eval `Remove digits` = REPLACE('test123', '\\d+', ''), `Collapse spaces` = REPLACE('hello  world', ' +', ' '), `Remove special` = REPLACE('hello@world!', '[^a-zA-Z]', '')
| fields `Remove digits`, `Collapse spaces`, `Remove special`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------+-----------------+----------------+
| Remove digits | Collapse spaces | Remove special |
|---------------+-----------------+----------------|
| test          | hello world     | helloworld     |
+---------------+-----------------+----------------+
```
  
### Example: Capture groups and backreference
  
```ppl
source=people
| eval `Swap date` = REPLACE('1/14/2023', '^(\\d{1,2})/(\\d{1,2})/', '$2/$1/'), `Reverse words` = REPLACE('Hello World', '(\\w+) (\\w+)', '$2 $1'), `Extract domain` = REPLACE('user@example.com', '.*@(.+)', '$1')
| fields `Swap date`, `Reverse words`, `Extract domain`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------+---------------+----------------+
| Swap date | Reverse words | Extract domain |
|-----------+---------------+----------------|
| 14/1/2023 | World Hello   | example.com    |
+-----------+---------------+----------------+
```
  
### Example: Advanced regex
  
```ppl
source=people
| eval `Clean phone` = REPLACE('(555) 123-4567', '[^0-9]', ''), `Remove vowels` = REPLACE('hello world', '[aeiou]', ''), `Add prefix` = REPLACE('test', '^', 'pre_')
| fields `Clean phone`, `Remove vowels`, `Add prefix`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------+---------------+------------+
| Clean phone | Remove vowels | Add prefix |
|-------------+---------------+------------|
| 5551234567  | hll wrld      | pre_test   |
+-------------+---------------+------------+
```
  
**Notes for regex patterns in PPL queries**:
* Backslashes must be escaped by doubling them: `\\` instead of `\`. Examples: `\\d` for digit patterns, `\\w+` for word characters.
* Backreferences support both PCRE-style (`\1`, `\2`) and Java-style (`$1`, `$2`) syntax. PCRE-style backreferences are automatically converted to Java-style internally.
  
## REVERSE

**Usage**: `REVERSE(str)`

Returns the reverse of the provided string.

**Parameters**:

- `str` (Required): The string to reverse.

**Return type**: `STRING`

### Example
  
```ppl
source=people
| eval `REVERSE('abcde')` = REVERSE('abcde')
| fields `REVERSE('abcde')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------------------+
| REVERSE('abcde') |
|------------------|
| edcba            |
+------------------+
```
  
## RIGHT

**Usage**: `right(str, len)`

Returns the last `len` number of characters of `str`. Returns `NULL` if any argument is `NULL`.

**Parameters**:

- `str` (Required): The input string.
- `len` (Required): The number of characters to return from the right side.

**Return type**: `STRING`

### Example
  
```ppl
source=people
| eval `RIGHT('helloworld', 5)` = RIGHT('helloworld', 5), `RIGHT('HELLOWORLD', 0)` = RIGHT('HELLOWORLD', 0)
| fields `RIGHT('helloworld', 5)`, `RIGHT('HELLOWORLD', 0)`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------------------------+------------------------+
| RIGHT('helloworld', 5) | RIGHT('HELLOWORLD', 0) |
|------------------------+------------------------|
| world                  |                        |
+------------------------+------------------------+
```
  
## RTRIM

**Usage**: `rtrim(str)`

Trims trailing space characters from the string.

**Parameters**:

- `str` (Required): The string from which to remove trailing spaces.

**Return type**: `STRING`

### Example
  
```ppl
source=people
| eval `RTRIM('   hello')` = RTRIM('   hello'), `RTRIM('hello   ')` = RTRIM('hello   ')
| fields `RTRIM('   hello')`, `RTRIM('hello   ')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------+-------------------+
| RTRIM('   hello') | RTRIM('hello   ') |
|-------------------+-------------------|
| hello             | hello             |
+-------------------+-------------------+
```
  
## SUBSTRING

**Usage**: `substring(str, start[, length])`

Returns a substring of `str` starting at `start` for `length` characters. If `length` is not specified, returns the substring from `start` to the end of the string.

**Parameters**:

- `str` (Required): The input string.
- `start` (Required): The starting position for the substring.
- `length` (Optional): The length of the substring. If not specified, returns from `start` to the end.

**Return type**: `STRING`

**Synonyms**: `SUBSTR`

### Example
  
```ppl
source=people
| eval `SUBSTRING('helloworld', 5)` = SUBSTRING('helloworld', 5), `SUBSTRING('helloworld', 5, 3)` = SUBSTRING('helloworld', 5, 3)
| fields `SUBSTRING('helloworld', 5)`, `SUBSTRING('helloworld', 5, 3)`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------------+-------------------------------+
| SUBSTRING('helloworld', 5) | SUBSTRING('helloworld', 5, 3) |
|----------------------------+-------------------------------|
| oworld                     | owo                           |
+----------------------------+-------------------------------+
```
  
## TRIM

**Usage**: `trim(str)`

Trims leading and trailing space characters from the string.

**Parameters**:

- `str` (Required): The string from which to remove leading and trailing spaces.

**Return type**: `STRING`

### Example
  
```ppl
source=people
| eval `TRIM('   hello')` = TRIM('   hello'), `TRIM('hello   ')` = TRIM('hello   ')
| fields `TRIM('   hello')`, `TRIM('hello   ')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------------------+------------------+
| TRIM('   hello') | TRIM('hello   ') |
|------------------+------------------|
| hello            | hello            |
+------------------+------------------+
```
  
## UPPER

**Usage**: `upper(string)`

Converts the string to uppercase.

**Parameters**:

- `string` (Required): The string to convert to uppercase.

**Return type**: `STRING`

### Example
  
```ppl
source=people
| eval `UPPER('helloworld')` = UPPER('helloworld'), `UPPER('HELLOWORLD')` = UPPER('HELLOWORLD')
| fields `UPPER('helloworld')`, `UPPER('HELLOWORLD')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------+---------------------+
| UPPER('helloworld') | UPPER('HELLOWORLD') |
|---------------------+---------------------|
| HELLOWORLD          | HELLOWORLD          |
+---------------------+---------------------+
```
  
## REGEXP_REPLACE

**Usage**: `regexp_replace(str, pattern, replacement)`

Replaces all substrings in `str` that match `pattern` with `replacement` and returns the resulting string.

**Parameters**:

- `str` (Required): The input string to perform replacements on.
- `pattern` (Required): The regular expression pattern to match.
- `replacement` (Required): The replacement string.

**Return type**: `STRING`

**Synonyms**: [REPLACE](#replace)

### Example
  
```ppl
source=people
| eval `DOMAIN` = REGEXP_REPLACE('https://opensearch.org/downloads/', '^https?://(?:www\.)?([^/]+)/.*$', '\1')
| fields `DOMAIN`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------+
| DOMAIN         |
|----------------|
| opensearch.org |
+----------------+
```
  