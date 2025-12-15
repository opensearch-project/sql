# grok


The `grok` command parses a text field with a grok pattern and appends the results to the search results.

## Syntax

Use the following syntax:

`grok <field> <pattern>`
* `field`: mandatory. The field must be a text field.  
* `pattern`: mandatory. The grok pattern used to extract new fields from the given text field. If a new field name already exists, it will replace the original field.  
  

## Example 1: Create the new field  

The following example PPL query shows how to use `grok` to create new field `host` for each document. `host` will be the hostname after `@` in `email` field. Parsing a null field will return an empty string.
  
```ppl
source=accounts
| grok email '.+@%{HOSTNAME:host}'
| fields email, host
```
  
Expected output:
  
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
  

## Example 2: Override the existing field  

The following example PPL query shows how to use `grok` to override the existing `address` field with street number removed.
  
```ppl
source=accounts
| grok address '%{NUMBER} %{GREEDYDATA:address}'
| fields address
```
  
Expected output:
  
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
  

## Example 3: Using grok to parse logs  

The following example PPL query shows how to use `grok` to parse raw logs.
  
```ppl
source=apache
| grok message '%{COMMONAPACHELOG}'
| fields COMMONAPACHELOG, timestamp, response, bytes
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----------------------------------------------------------------------------------------------------------------------------+----------------------------+----------+-------+
| COMMONAPACHELOG                                                                                                             | timestamp                  | response | bytes |
|-----------------------------------------------------------------------------------------------------------------------------+----------------------------+----------+-------|
| 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | 28/Sep/2022:10:15:57 -0700 | 404      | 19927 |
| 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | 28/Sep/2022:10:15:57 -0700 | 100      | 28722 |
| 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | 28/Sep/2022:10:15:57 -0700 | 401      | 27439 |
| 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | 28/Sep/2022:10:15:57 -0700 | 301      | 9481  |
+-----------------------------------------------------------------------------------------------------------------------------+----------------------------+----------+-------+
```
  

## Limitations  

The grok command has the same limitations as the parse command, see [parse limitations](./parse.md#Limitations) for details.