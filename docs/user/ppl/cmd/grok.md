
# grok

The `grok` command parses a text field using a Grok pattern and appends the extracted results to the search results.

## Syntax

The `grok` command has the following syntax:

```syntax
grok <field> <pattern>
```

## Parameters

The `grok` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The text field to parse. |
| `<pattern>` | Required | The Grok pattern used to extract new fields from the specified text field. If a new field name already exists, it overwrites the original field. |  
  

## Example 1: Create a new field

The following query shows how to use the `grok` command to create a new field, `host`, for each document. The `host` field captures the hostname following `@` in the `email` field. Parsing a null field returns an empty string:
  
```ppl
source=accounts
| grok email '.+@%{HOSTNAME:host}'
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

The following query shows how to use the `grok` command to override the existing `address` field, removing the street number:
  
```ppl
source=accounts
| grok address '%{NUMBER} %{GREEDYDATA:address}'
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
  

## Example 3: Using grok to parse logs  

The following query parses raw logs:
  
```ppl
source=apache
| grok message '%{COMMONAPACHELOG}'
| fields COMMONAPACHELOG, timestamp, response, bytes
```
  
The query returns the following results:
  
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

The `grok` command has the following limitations:

* The `grok` command has the same [limitations](./parse.md#limitations) as the `parse` command. 