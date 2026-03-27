# Protocol  

## Introduction  

For the protocol, PPL endpoint provides response formats in the JDBC format. JDBC format is widely used because it provides schema information and more functionality such as pagination. Besides JDBC driver, various clients can benefit from the detailed and well formatted response.
## Request/Response Format  

### Description  

The body of HTTP POST request can take PPL query.
### Example 1  
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl \
-d '{"query" : "source=accounts | fields firstname, lastname"}'
```
  
Expected output:
  
```json
{
  "schema": [
    {
      "name": "firstname",
      "type": "string"
    },
    {
      "name": "lastname",
      "type": "string"
    }
  ],
  "datarows": [
    [
      "Amber",
      "Duke"
    ],
    [
      "Hattie",
      "Bond"
    ],
    [
      "Nanette",
      "Bates"
    ],
    [
      "Dale",
      "Adams"
    ]
  ],
  "total": 4,
  "size": 4
}
```
  
## JDBC Format  

### Description  

By default the plugin return JDBC format. JDBC format is provided for JDBC driver and client side that needs both schema and result set well formatted.
### Example 1  

Here is an example for normal response. The `schema` includes field name and its type and `datarows` includes the result set.
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl \
-d '{"query" : "source=accounts | fields firstname, lastname"}'
```
  
Expected output:
  
```json
{
  "schema": [
    {
      "name": "firstname",
      "type": "string"
    },
    {
      "name": "lastname",
      "type": "string"
    }
  ],
  "datarows": [
    [
      "Amber",
      "Duke"
    ],
    [
      "Hattie",
      "Bond"
    ],
    [
      "Nanette",
      "Bates"
    ],
    [
      "Dale",
      "Adams"
    ]
  ],
  "total": 4,
  "size": 4
}
```
  
### Example 2  

If any error occurred, error message and the cause will be returned instead.
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl \
-d '{"query" : "source=unknown | fields firstname, lastname"}'
```
  
Expected output:
  
```json
{
  "error": {
    "context": {
      "stage": "analyzing",
      "index_name": "unknown",
      "stage_description": "Parsing and validating the query"
    },
    "details": "no such index [unknown]",
    "location": [
      "while preparing and validating the query plan",
      "while fetching index mappings"
    ],
    "code": "INDEX_NOT_FOUND",
    "type": "IndexNotFoundException"
  },
  "status": 404
}
```
  