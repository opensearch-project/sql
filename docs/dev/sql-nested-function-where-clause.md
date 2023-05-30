## Description

The `nested` function when used in the `WHERE` clause of an SQL statement filters documents of `nested` object type based on field properties. Two syntax options are available to users, see section [1.1 Syntax](#11-syntax). Both syntax options achieve the goal of filtering a `nested` field with an `Operator` and a `Literal` value. Although semantically the syntax options can produce the same query, they will produce different DSL to push to OpenSearch. This difference is based on the `AND/OR/NOT` boolean queries residing inside a `nested` query or vice versa. See section [2.1](#21-syntax-option-1-object-tree-to-dsl) and [2.2](#22-syntax-option-2-object-tree-to-dsl) for examples depicting this alternate DSL creation. Testing large groups of push down execution times for identical queries using the different syntax options showed a nominal difference of approximately 3% for Syntax option 2.

## Table of Contents
1. [Overview](#1-overview)
2. [Syntax](#11-syntax)
3. [Changes To Core](#12-changes-to-core)
4. [Example Data And Usage](#13-example-data-and-usage)
5. [Example Queries](#14-example-queries)
6. [Architecture Diagrams](#2-architecture-diagrams)
8. [Syntax Option 1 Object Tree to DSL](#21-syntax-option-1-object-tree-to-dsl)
9. [Syntax Option 2 Object Tree to DSL](#22-syntax-option-2-object-tree-to-dsl)
10. [Additional Info](#additional-info)
11. [Release Schedule](#release-schedule)

## 1 Overview
### 1.1 Syntax

The nested function has two syntax options when used in the `WHERE` clause of an SQL statement. Both syntax options can produce the same OpenSearch response, but will form different DSL used in the query.
1. `nested(path, condition)`
2. `nested(field | field, path) OPERATOR LITERAL`


### 1.2 Changes To Core
- **FilterQueryBuilder:** Added logic to handle `nested` functions in `WHERE` clause as predicate expression and `FunctionExpression`.
- **LuceneQuery:** Added logic to handle `nested` functions and `FunctionExpression`'s on both sides of `OPERATOR` in `WHERE` clause.


### 1.3 Example Data and Usage

A basic example from data mapping to response from SQL plugin.

**Mapping:**
```json
{
  "mappings": {
    "properties": {
      "message": {
        "type": "nested",
        "properties": {
          "info": {
            "type": "keyword",
            "index": "true"
          }
        }
      }
    }
  }
}
```

**Dataset:**
```json
{"index":{"_id":"1"}}
{"message":{"info":"a"}}
{"index":{"_id":"2"}}
{"message":{"info":"c"}}
```

**Query:**
- `SELECT * FROM nested_objects WHERE nested(message, message.info = 'a');`
- `SELECT * FROM nested_objects WHERE nested(message.info) = 'a';`

Both queries produce same response from OpenSearch.

**Response:**
```json
{
  "schema": [
    {
      "name": "message",
      "type": "nested"
    }
  ],
  "datarows": [
    [
      [
        {
          "info": "a"
        }
      ]
    ]
  ],
  "total": 1,
  "size": 1,
  "status": 200
}
```


### 1.4 Example Queries

A basic nested function in the `SELECT` clause. This example filters the `nested` object `message` and the inner field `info` with the value of `a`.
- `SELECT * FROM nested_objects WHERE nested(message.info) = 'a' OR nested(message.author) = 'elm';`

Using the same query as above but with syntax option 1, the response from OpenSearch will be the same, but the DSL used in the query will differ.
- `SELECT * FROM nested_objects WHERE nested(message, message.info = 'a' OR messate.author = 'elm');`

This example has a nested function in the `SELECT` clause and the `WHERE` clause of the query. Using the `nested` function in the `SELECT` clause will flatten the response nested objects from OpenSearch. See [Nested In Select](sql-nested-function-select-clause.md) for more details on flattening.
- `SELECT nested(message.info) FROM nested_objects WHERE nested(message.info) = 'a';`

When using syntax option 1 we need separate function calls when the `path` is not identical. This query exemplifies how a user can use multiple queries with differing `path` values. 
- `SELECT * FROM nested_objects WHERE nested(message, message.info = 'a') OR nested(comment, comment.data = '123');`


## 2 Architecture Diagrams
### 2.1 Syntax Option 1 Object Tree to DSL
The following example illustrates the object tree that is built from the example query. This query's object tree will impact the structure of the DSL to be pushed to OpenSearch.

Example Query: `SELECT *  FROM nested_objects WHERE nested(message, message.info = 'a' OR message.info = 'b' AND message.dayOfWeek > 4);`

```mermaid
graph TB;
    A[Function: NESTED\n<hr>arguments]-->B1[ReferenceExpression:\nmessage]
    A-->B2[Function: OR\n<hr>arguments]
    B2-->C1[Function: =\n<hr>arguments]
    B2-->C2[Function: AND\n<hr>arguments]
    
    C1-->D1[ReferenceExpression:\nmessage.info]
    C1-->D2[LiteralExpression:\na]
    
    C2-->D3[Function: =\n<hr>arguments]
    C2-->D4[Function: >\n<hr>arguments]
    
    D3-->E1[ReferenceExpression:\nmessage.info]
    D3-->E2[LiteralExpression:\nb]
    D4-->E3[ReferenceExpression:\nmessage.dayOfWeek]
    D4-->E4[LiteralExpression:\n4]
```


#### Syntax Option 1 Filter Push Down Sequence
This diagram illustrates the steps the SQL plugin does to translate the object tree to DSL for execution in OpenSearch. Notice that the `nested` function forms the outer portion of the DSL query.

```mermaid
stateDiagram-v2
  direction LR

  NestedNode --> OrFuncNode
  OrFuncNode --> OrFuncNode1
  OrFuncNode --> OrFuncNode2
  OrFuncNode2 --> AndFuncNode1
  OrFuncNode2 --> AndFuncNode2

  state "Function: NESTED" as NestedNode {
    state "visitFunction(NESTED)" as NestedNodeVisitFunc
    state "'nested': {\n&nbsp'query': {\n...\n}}" as NestedNodeDSL
    state "visitFunction(condition)" as NestedNodeVisitCondition

    NestedNodeVisitFunc --> NestedNodeDSL
    NestedNodeVisitFunc --> NestedNodeVisitCondition
  }

  state "Function: OR" as OrFuncNode {
    state "visitFunction(OR)" as OrFuncNodeVisitFunc
    state "'bool': {\n&nbsp'should': {\n...\n}}" as OrFuncNodeDSL
    state "arguments.accept()" as OrFuncNodeArgs

    OrFuncNodeVisitFunc --> OrFuncNodeDSL
    OrFuncNodeDSL --> OrFuncNodeArgs
  }

  state "Function: =" as OrFuncNode1 {
    state "visitFunction(=)" as OrFuncNode1VisitFunc
    state "'term': {\n&nbsp'message.info': {\n&nbsp&nbsp'value':'a'\n}}" as OrFuncNode1DSL

    OrFuncNode1VisitFunc --> OrFuncNode1DSL
  }

  state "Function: AND" as OrFuncNode2 {
    state "visitFunction(AND)" as OrFuncNode2VisitFunc
    state "'bool': {\n&nbsp'filter': {\n...\n}}" as OrFuncNode2DSL
    state "arguments.accept()" as OrFuncNode2Args

    OrFuncNode2VisitFunc --> OrFuncNode2DSL
    OrFuncNode2DSL --> OrFuncNode2Args
  }

  state "Function: =" as AndFuncNode1 {
    state "visitFunction(=)" as AndFuncNode1VisitFunc
    state "'term': {\n&nbsp'message.info': {\n&nbsp&nbsp'value':'b'\n}}" as AndFuncNode1DSL

    AndFuncNode1VisitFunc --> AndFuncNode1DSL
  }

  state "Function: >" as AndFuncNode2 {
    state "visitFunction(>)" as AndFuncNode2VisitFunc
    state "'range': {\n&nbsp'message.dayOfWeek': {\n&nbsp&nbsp'from':4\n}}" as AndFuncNode2DSL

    AndFuncNode2VisitFunc --> AndFuncNode2DSL
  }
```


#### Syntax Option 1 Output Query
After pushing down the filter object tree we have a DSL query to push to OpenSearch. This query forms the boolean logic inside the `nested` query. This mirrors the syntax option chosen which has the condition specified inside the `nested` function.

```json
{
  "nested" : {
    "query" : {
      "bool" : {
        "should" : [
          {
            "term" : {
              "message.info" : {
                "value" : "a",
                "boost" : 1.0
              }
            }
          },
          {
            "bool" : {
              "filter" : [
                {
                  "term" : {
                    "message.info" : {
                      "value" : "b",
                      "boost" : 1.0
                    }
                  }
                },
                {
                  "range" : {
                    "message.dayOfWeek" : {
                      "from" : 4,
                      ...
                    }
                  }
                }
              ]
            }
          }
        ]
      }
    }
  }
}
```


### 2.2 Syntax option 2 Object Tree to DSL
The following example illustrates the object tree that is built from the example query. Rather than have the boolean logic specified within the `nested` function, we have the `nested` function used in a predicate expression.

Example Query: `SELECT * FROM nested_objects WHERE nested(message.info) = 'a' OR nested(message.info) = 'b' AND nested(message.dayOfWeek) > 4;`

```mermaid
graph TB;
    A[Function: OR\n<hr>arguments]-->B1[Function: =\n<hr>arguments]
    A-->B2[Function: AND\n<hr>arguments]
    
    B1-->C1[Function: NESTED\n<hr>arguments]
    B1-->C2[LiteralExpression:\na]
    
    B2-->C3[Function: =\n<hr>arguments]
    B2-->C4[Function: >\n<hr>arguments]
    
    C1-->D1[ReferenceExpression:\nmessage.info]
    C3-->D2[Function: NESTED\n<hr>arguments]
    C3-->D3[LiteralExpression:\nb]
    C4-->D4[Function: NESTED\n<hr>arguments]
    C4-->D5[LiteralExpression:\n4]
    
    D2-->E1[ReferenceExpression:\nmessage.info]
    D4-->E2[ReferenceExpression:\nmessage.dayOfWeek]
```


#### Syntax Option 2 Filter Push Down Sequence
This diagram illustrates the steps the SQL plugin goes through to translate the object tree to DSL for execution in OpenSearch. Notice that the boolean operations(`should/filter`) form the outer potion of the DSL query. 

```mermaid
stateDiagram-v2
  direction LR

  OrFuncNode --> OrFuncNode1
  OrFuncNode --> OrFuncNode2
  OrFuncNode2 --> AndFuncNode1
  OrFuncNode2 --> AndFuncNode2

  state "Function: OR" as OrFuncNode {
    state "visitFunction(OR)" as NestedNodeVisitFunc
    state "'bool': {\n&nbsp'should': {\n...\n}}" as NestedNodeDSL
    state "arguments.accept()" as NestedNodeVisitCondition

    NestedNodeVisitFunc --> NestedNodeDSL
    NestedNodeVisitFunc --> NestedNodeVisitCondition
  }


  state "Function: =" as OrFuncNode1 {
    state "visitFunction(=)" as OrFuncNode1VisitFunc
    state "'nested': {\n&nbsp'query': {\n&nbsp&nbsp&nbsp'term'\n&nbsp&nbsp&nbsp&nbsp'message.info': {\n&nbsp&nbsp&nbsp&nbsp&nbsp'value': 'a',\n&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp}}}" as OrFuncNode1DSL


    OrFuncNode1VisitFunc --> OrFuncNode1DSL
  }


  state "Function: AND" as OrFuncNode2 {
    state "visitFunction(AND)" as OrFuncNode2VisitFunc
    state "'bool': {\n&nbsp'filter': {\n...\n}}" as OrFuncNode2DSL
    state "arguments.accept()" as OrFuncNode2Args

    OrFuncNode2VisitFunc --> OrFuncNode2DSL
    OrFuncNode2DSL --> OrFuncNode2Args
  }

  state "Function: =" as AndFuncNode1 {
    state "visitFunction(=)" as AndFuncNode1VisitFunc
    state "'nested': {\n&nbsp'query': {\n&nbsp&nbsp&nbsp'term'\n&nbsp&nbsp&nbsp&nbsp'message.info': {\n&nbsp&nbsp&nbsp&nbsp&nbsp'value': 'b',\n&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp}}}" as AndFuncNode1DSL

    AndFuncNode1VisitFunc --> AndFuncNode1DSL
  }

  state "Function: >" as AndFuncNode2 {
    state "visitFunction(>)" as AndFuncNode2VisitFunc
    state "'nested': {\n&nbsp'query': {\n&nbsp&nbsp&nbsp'range'\n&nbsp&nbsp&nbsp&nbsp'message.dayOfWeek': {\n&nbsp&nbsp&nbsp&nbsp&nbsp'from': 4,\n&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp}}}" as AndFuncNode2DSL

    AndFuncNode2VisitFunc --> AndFuncNode2DSL
  }
```


#### Syntax Option 2 Output Query
After pushing down the filter object tree we have a DSL query to push to OpenSearch. This query forms the boolean logic as the outer portion of the query and has nested queries used inside. This mirrors the syntax option chosen which has the boolean operators forming a predicate expression with the `nested` functions in the SQL query.

```json
{
  "bool" : {
    "should" : [
      {
        "nested" : {
          "query" : {
            "term" : {
              "message.info" : {
                "value" : "a",
                "boost" : 1.0
              }
            }
          },
          ...
        }
      },
      {
        "bool" : {
          "filter" : [
            {
              "nested" : {
                "query" : {
                  "term" : {
                    "message.info" : {
                      "value" : "b",
                      "boost" : 1.0
                    }
                  }
                },
                ...
              }
            },
            {
              "nested" : {
                "query" : {
                  "range" : {
                    "message.dayOfWeek" : {
                      "from" : 4,
                      ...
                    }
                  }
                },
                ...
              }
            }
          ]
        }
      }
    ]
  }
}
```


## Additional Info
### Demo Video
TBD

### Normalizing Strategy
A normalization strategy could produce identical DSL independent on the syntax option used in the query. Testing so far hasn't highlighted any major improvements with query execution that necessitate identical DSL. Additional benchmarking and planning will be done to determine a viable normalization strategy and if implementation is required.

### Release Schedule
See Issues Tracked under [Issue 1111](https://github.com/opensearch-project/sql/issues/1111) for related PR's and information.
