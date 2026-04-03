
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
  

## Example 1: Extract IP addresses from error logs

The following query uses grok to extract IP addresses from error log messages:
  
```ppl
source=otellogs
| where severityText = 'ERROR'
| grok body '%{IP:clientip}'
| fields body, clientip
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-------------------------------------------------------------------------+----------+
| body                                                                    | clientip |
|-------------------------------------------------------------------------+----------|
| Payment failed: connection timeout to payment gateway after 30000ms     |          |
| NullPointerException in CheckoutService.placeOrder at line 142          |          |
| Out of memory: Java heap space - shutting down pod payment-6f8d4b-ht7q3 |          |
+-------------------------------------------------------------------------+----------+
```
  

## Example 2: Extract durations from log messages

The following query uses grok to extract numeric durations from INFO log messages:
  
```ppl
source=otellogs
| where severityText = 'INFO'
| grok body '%{NUMBER:duration}ms'
| fields body, duration
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------------------------------------------+----------+
| body                                             | duration |
|--------------------------------------------------+----------|
| HTTP GET /api/products 200 45ms                  | 45       |
| Order #1234 placed successfully by user U100     |          |
| User U300 authenticated via OAuth2 from 10.0.0.5 |          |
+--------------------------------------------------+----------+
```
  

## Limitations

The `grok` command has the following limitations:

* The `grok` command has the same [limitations](./parse.md#limitations) as the `parse` command. 
