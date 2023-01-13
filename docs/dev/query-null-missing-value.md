## Description
+ Auto add FIELDS * for PPL command.
+ In Analyzer, expend SELECT * to SELECT all fields.
+ Extract type info from QueryPlan and add to QueryResponse.
+ Support NULL and MISSING value in response. https://github.com/penghuo/sql/blob/pr-select-all/docs/user/general/values.rst

## Problem Statements
### Background
Before explain the current issue, firstly, let’s setup the context.

#### Sample Data
Let’s explain the problem with an example, the bank index which has 2 fields.

```
    "mappings" : {
      "properties" : {
        "account_number" : {
          "type" : "long"
        },
        "age" : {
          "type" : "integer"
        }
      }
    }
```
Then we add some data to the index.

```
POST bank/_doc/1
{"account_number":1,"age":31}

// age is null
POST bank/_doc/2
{"account_number":2,"age":null}

// age is missing
POST bank/_doc/3
{"account_number":3}
```

#### JDBC and JSON data format
Then, we define the response data format for query “SELECT account_number FROM bank” as follows.

JDBC Format. There are mandatory fields, “field name”, “field type” and “data”. e.g.
```
{
  "schema": [{
    "name": "account_number",
    "type": "long"
  }],
  "total": 3,
  "datarows": [
    [1],
    [2],
    [3]
  ],
  "size": 3
}
```

JSON Format, comparing with JDBC format, it doesn’t have schema field

```
{
  "datarows": [
    {"account_number": 1},
    {"account_number": 2},
    {"account_number": 3}
  ]
}
```

### Issue 1. Represent NULL and MISSING in Response
With these sample data and response data format in mind, let go through more query and examine their results.
Considering the query:** SELECT age, account_number FROM bank. **
The JDBC format doesn’t have MISSING value. If the field exist in the schema but missed in the document, it should be considered as NULL value.
The JSON format could represent the MISSING value properly.

* JDBC Format
```
  {
    "schema": [
    {
      "name": "age",
      "type": "integer"
    },
    {
      "name": "account_number",
      "type": "long"
    }
    ],
    "total": 3,
    "datarows": [
      [
        31,
        1
      ],
      [
        null,
        2
      ],
      [
        null,
        3
      ],
    ],
    "size": 3
  }

* JSON Format
  {
    "datarows": [
      {"age": 1, "account_number": 1},
      {"age": null, "account_number": 2},
      {"account_number": 3}
    ]
  }
```

### Issue 2. ExprValue to JDBC format

Based on our current implementation, all the SQL operator is translated to chain of PhysicalOperator. Each PhysicalOperator provide the ExprValue as the return value. The protocol pull the result from PhysicalOperator and translate to the expected format. e.g. Taking the above query as example, the result of the PhysicalOpeartor is a list of ExprValues.

[
{"age": ExprIntegerValue(1), "account_number": ExprIntegerValue(1)},
{"age": ExprNullValue(), "account_number": ExprIntegerValue(2)},
{"account_number": ExprIntegerValue(3)}
]
The current solution is extract field name and field type from the data itself. This solution has two problems

It is impossible to derive the type from NULL value.
If the field is missing in the ExprValue, there is no way to derive it.

### Issue 3. The Type info is missing
In current design, the Protocol is a separate module which work independently with QueryEngine. The Protocol module receive the list of ExprTupleValue from QueryEngine, then the Protocol module format the result based on the type of ExprValue. the problem is ExprNullValue and ExprMissingValue doesn’t have type assosicate with it. thus the Protocol module can’t derive the type info from input ExprTupleValue directly.

### Issue 4. What is *(all field) means in SELECT
In current design, the SELECT * clause ignored in the AST builder logic, because it means select all the data from input operator. The issue is similar as Issue 3 that if the input operator produce NULL or MISSING value, then the Protocol have no idea to derive type info from it.

#### Requirements
The JDBC format should be supported. The MISSING and NULL value should be represented as NULL.
The JSON format should be supported.
The Protocol module should receive the QueryResponse which include schema and data.

#### Solution
##### Include NULL and MISSING value in the QueryResult (Issue 1, 2)
The SELECT operator will be translated to PhysicalOpeartor with a list of expression to resolve ExprValue from input data. With the above example, when handling NULL and MISSING value, the expected output data should be as follows.

```
[
  {"age": ExprIntegerValue(1), "account_number": ExprIntegerValue(1)},
  {"age": ExprNullValue(), "account_number": ExprIntegerValue(2)},
  {"age": ExprMissingValue(), "account_number": ExprIntegerValue(3)}
]
```

An additional list of Schema is also required to when protocol is JDBC.

```
{
  "schema": [
    {
      "name": "age",
      "type": "integer"
    },
    {
      "name": "account_number",
      "type": "long"
    }
  ]    
}
```
Then the protocol module could easily translate the JDBC format or JSON format.

##### Expend SELECT * to SELECT ...fields (Issue 4)
In our current implementation, in SQL, the SELECT * is ignored and in PPL there even no fields * command. This solution works fine for JSON format which doesn’t require schema, but it doesn’t work for JDBC format.
The proposal in here is

+ Automatically add the fields command to PPL query
+ Expand SELECT * to SELECT ...fields.
 
##### Automatically add fields * to PPL query
Comparing with SQL, the PPL grammer doesn’t require the Fields command is the last command. Thus, the fields * command should be automatically added.
The automatically added logic is if the last operator is not Fields command, the Fields * command will be added.

##### Expand SELECT * to SELECT ...fields
In Analyzer, we should expend the * to all fields in the current scope. There are two issues we need to address,

No all the fields in the current scope should been used to expand *. The original scope is from Elasticsearch mapping which include nested mapping. In current design, only the top fields will be retrived from the current scope, all the nested fields will been ignored.
The scope should been dynamtic maintain in the Analyzer. For example, the stats command will define the new scope.

#### Retrieve Type Info from ProjectOperator and Expose to Protocol (Issue 3)
After expending the * and automatically add fields, the type info could been retrieved from ProjectOperator. Then the Protocol could get schema and data from QueryEngine.