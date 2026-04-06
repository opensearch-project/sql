
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
  

## Example 1: Parse Apache access logs  

The following query parses raw Apache access logs using the built-in `COMMONAPACHELOG` grok pattern:
  
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

## Example 2: Extract fields from Envoy access logs

The following query parses Envoy access log entries, extracting the HTTP method, path, and status code:

```ppl
source=otellogs
| where LIKE(body, '%HTTP/1.1%')
| grok body '\[%{DATA:ts}\] \"%{WORD:method} %{DATA:path} HTTP/%{DATA:ver}\" %{POSINT:status}'
| fields method, path, status
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+--------+---------------+--------+
| method | path          | status |
|--------+---------------+--------|
| GET    | /api/products | 200    |
| POST   | /api/checkout | 503    |
+--------+---------------+--------+
```

## Example 3: Extract durations from log messages

The following query uses grok to extract numeric durations from log messages:

```ppl
source=otellogs
| where LIKE(body, '%ms%')
| grok body '%{NUMBER:duration}ms'
| fields body, duration
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------------------------------------------------------------------------------+----------+
| body                                                                                   | duration |
|----------------------------------------------------------------------------------------+----------|
| Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms | 3200     |
| Payment failed: connection timeout to payment gateway after 30000ms                     | 30000    |
| gRPC call /ProductCatalogService/GetProduct completed in 12ms                           | 12       |
+----------------------------------------------------------------------------------------+----------+
```

## Limitations

The `grok` command has the following limitations:

* The `grok` command has the same [limitations](./parse.md#limitations) as the `parse` command. 