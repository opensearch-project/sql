
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
  

## Example 1: Extract IP addresses from log messages

The following query uses grok to extract IP addresses from log messages, useful for identifying client sources during security investigations:
  
```ppl
source=otellogs
| where LIKE(body, '%from%')
| grok body '%{IP:clientip}'
| fields body, clientip
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+---------------------------------------------------------------------------+--------------+
| body                                                                      | clientip     |
|---------------------------------------------------------------------------+--------------|
| Slow query detected: SELECT * FROM inventory WHERE stock < 10 took 3200ms |              |
| User U300 authenticated via OAuth2 from 10.0.0.5                          | 10.0.0.5     |
| Failed to authenticate user U400: invalid credentials from 203.0.113.50   | 203.0.113.50 |
+---------------------------------------------------------------------------+--------------+
```
  

## Example 2: Extract durations from log messages

The following query uses grok to extract numeric durations from log messages, useful for identifying slow operations:
  
```ppl
source=otellogs
| where LIKE(body, '%ms%')
| grok body '%{NUMBER:duration}ms'
| fields body, duration
| head 2
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+---------------------------------------------------------------------------+----------+
| body                                                                      | duration |
|---------------------------------------------------------------------------+----------|
| HTTP GET /api/products 200 45ms                                           | 45       |
| Slow query detected: SELECT * FROM inventory WHERE stock < 10 took 3200ms | 3200     |
+---------------------------------------------------------------------------+----------+
```
  

## Limitations

The `grok` command has the following limitations:

* The `grok` command has the same [limitations](./parse.md#limitations) as the `parse` command. 
