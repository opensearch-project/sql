# SQL Functions Implementation by a New Expression System

## 1.Overview

### 1.1 Problem Statement

Currently, there were quite a few issues regarding SQL functions reported earlier. Those issues were critical but not easy to fix in the old codebase. Recently this important problem was brought up again in ODBC driver test results. As reported, the SQL function issues, especially missing support for nested function call, failed a large number of test in the total 800+ test cases. This makes SQL function issue top priority and have to be planned to fix soon.

### 1.2 Expression System in New Query Engine

This year we have been working on a new query engine architecture. At this moment, we have implemented a few common relational operators and expressions. In future, all the arithmetic operators, logical operators and scalar functions will be built on top of the expression system. These expressions form a closure system mathematically which can be composed flexibly and recursively.


>From wikipedia, “A set is **closed** under an operation if performance of that operation on members of the set always produces a member of that set... Similarly, a set is said to be closed under a collection of operations if it is closed under each of the operations individually... (the set) is said to satisfy **closure property**.” In our case, our expression values (ExprValue class) is closed under expression operators defined, including unary/binary logical operator, arithmetic operator and functions. In other word, each of these operators can only output expression value which makes possible composing them in any way to form an expression tree.


Given consideration to the advantages of the new architecture, the complexity of old codebase as well as our future plan, we favor supporting SQL functions in new query engine directly over struggling to patch the old codebase. However, one important challenge needs to take is to figure out how to push down expression to Elasticsearch DSL query.

* * *

## 2.Requirements

### 2.1 Functionalities

According to the GitHub issues reported as well as ODBC driver test results, the following functionalities needs to be taken into account:

1. **Semantically Consistency**: Each function should have exactly the same meaning as other SQL implementation. This includes the semantics of function argument and return value and their semantic check.
2. **Missing/Null Value Handling**: Each function should be able to handle missing/null value in Elasticsearch document.
3. **Nested Function Call Support**: Each function should be able to be nested with other functions. Tableau seems to rely on this capability heavily.

### 2.2 Performance

As aforementioned, function expressions need to be pushed down to Elasticsearch DSL to run inside Elasticsearch computation engine. In relational theory, this push down optimization, especially selection push down, is probably the most common and effective one.


>In Elasticsearch, this process is called **Query-Then-Fetch**: “The request is processed in two phases. In the first phase, the query is forwarded to all involved shards. Each shard executes the search and generates a sorted list of results, local to that shard. Each shard returns **just enough information** to the coordinating node to allow it to merge and re-sort the shard level results into a globally sorted set of results...”. So in our case, if expressions pushed down, it would be executed within query phase and reduce the size of result returned significantly.


The reason why is this required mostly is obvious that all data would be pulled out otherwise. This either causes out of memory error without circuit breaker protection or ends up with poor performance.

### 2.3 Security

When pushed down, we need to be careful about the possibility that attacker injects malicious code in SQL query which in turn executed without permission control. In this case, we need to make use of Elasticsearch script sandbox to rule out security risk.

* * *

## 3.Design

### 3.1 Expression Pushdown

Similar as the existing operators, it is very straightforward to integrate functions with expression system in the new query engine. Therefore, in this section, the attention is put on approaches for expression push down optimization.

**Approach 1: Translating to Painless Script**

This is how we optimized expression in old codebase.

* Pros:
    * Elasticsearch built-in script language.
    * Can be used in different places in DSL.
* Cons:
    * Need to maintain pipelining Java code or pushed down painless script for each expression.
    * Inline Painless script in JAVA code is messy.
    * Painless API limitation. (Need to confirm if any SQL function cannot be implemented)

**Approach 2 (Chosen): Expression as a New Script Language**

In this approach, we propose to registering a new script language in Elasticsearch. This is feasible and supported by Elasticsearch `ScriptEngine` API.

* Pros:
    * Implement a function expression in JAVA only and get the pushed down version automatically.
    * No JAVA byte code generated at runtime which seems how Painless compiles before run.
* Cons:
    * Serialized expression may be longer than painless script. Need to check if any limit on max length of DSL.
    * CPU consumption may be higher under the assumption that (de-)serialization is more expensive than concatenating Painless script string.
    * Built-in JDK serialization requires code changes on all expression classes and lambda. (Need to find better 3rd-party library if necessary).

### 3.2 Elasticsearch DSL Queries Pushdown

In the design above, we register our expression as new script language and optimize expression evaluation by pushdown to script query in Elasticsearch DSL. However, in this case, we are not leveraging the Elasticsearch DSL for Lucene queries. For certain filter expressions, it can be optimized further by pushing down to Elasticsearch queries fully or partially.

This problem can be solved by visiting functions/operators in top down fashion and handle in the following way:

1. `AND`, `OR`, `NOT`: Translate to bool query and visit left/right side recursively
2. Functions that Lucene may support: Translate to Lucene query if left side is a reference (field name) and right is a literal. Otherwise go to 3.
3. Functions that Elasticsearch query doesn't support or argument is not valid as mentioned in 2: Serialize the expression (subtree with current function node as root) and translate to script query.

Here is an example to help understand. `AND` is translated to bool filter query directly (case [#1](https://github.com/opendistro-for-elasticsearch/sql/pull/1)). The `name = 'John'` is eligible because `=` can be translated to Lucene term query and left side (first argument) is a reference and right side is a literal (case [#2](https://github.com/opendistro-for-elasticsearch/sql/pull/2)). `ABS(age) = 30` is translated to a script query (case [#3](https://github.com/opendistro-for-elasticsearch/sql/issues/3)).
[Image: lucene-pushdown.png]

* * *

## 4.Proof of Concept

### 4.1 Overview

Here is the diagram showing how WHERE condition in a SQL query pushed down and handled by our own script engine:
[Image: expressions (3)-Page-3.png]

### 4.2 Conclusion

Now let’s look back to see if what we’ve done can meet all the requirements in 2.1 Functionalities:

1. In base class `SQLFunction`, we handle missing and NULL argument value properly to be consistent with SQL standards.
2. In `Substring` implementation, we perform semantic check.
3. The only remaining item is nested function call. Luckily this is already built in new expression system. Each function we add gets this ability automatically and for free.

For push down optimization, what we achieved is as follows. Elasticsearch is aware of and able to interpret serialized expression tree in DSL when our own language name “`opendistro_expression`” is specified.

```
// Request
{
   "query": "SELECT firstname FROM accounts WHERE **SUBSTRING(SUBSTRING(lastname,0,3),0,1) = 'w'**"
}

**===>

**// DSL
{
   "query":{
      "script":{
         "boost":1,
         "script":{
            // Serialized expression tree
            "source":"rO0ABXNyAF9jb20uYW1hem9uLm9wZW5kaXN0...",
            "lang":"opendistro_expression"
         }
      }
   }
}
```

### 4.3 Expression

In this section we implement SQL function `SUBSTRING` for example. This demonstrates that all the functionality required can be taken care of in new expression system.

**4.3.1 Function Expression Implementation**

First, we need a base class `SQLFunction` for logic shared by all SQL functions. For example, each SQL function needs to check if any input argument is missing or NULL. If found any, expression evaluation is skipped and instead a missing or NULL is returned respectively.

```
public abstract class **SQLFunction** extends FunctionExpression {
    ...

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
        List<Expression> argExprs = getArguments();
        ExprValue[] argValues = new ExprValue[argExprs.size()];

        for (int i = 0; i < argExprs.size(); i++) {
            **ExprValue argValue = argExprs.get(i).valueOf(valueEnv);**
            if (argValue.isMissing()) {
                return ExprValueUtils.missingValue();
            }
            if (argValue.isNull()) {
                return ExprValueUtils.nullValue();
            }
            argValues[i] = argValue;
        }
        return valueOf(argValues);
    }

    protected abstract ExprValue valueOf(ExprValue[] argValues);

}
```

Now we implement `Substring` as subclass of SQLFunction:

```
public class **Substring** extends SQLFunction {
    ...

    @Override
    protected ExprValue valueOf(ExprValue[] argValues) {
        String str = (String) argValues[0].value();
        int start = (int) argValues[1].value();
        int length = (int) argValues[2].value();
        int end = start + length;

        if (!(0 <= start && end <= str.length())) {
            throw new SemanticCheckException(String.format(
                "Semantic error on arguments: %s." +
                    "Expect: start is non-negative and start + length is no greater than string length.",
                        Arrays.toString(argValues)));
        }

        String result = str.substring(start, end);
        return ExprValueUtils.stringValue(result);
    }

    @Override
    public ExprType type(Environment<Expression, ExprType> typeEnv) {
        return ExprType.STRING;
    }

}
```

**4.3.2 Register in Function Repository**

Before using substring function, we need to register this `Substring` in function repository. Accordingly ANTLR grammar needs to be changed too to avoid syntactic error. ***Note that in this PoC we consider SQL functions are dynamic extension to query engine from SQL parser***. That is why we register substring function in `SQLService`.

```
    public SQLService(SQLSyntaxParser parser, Analyzer analyzer,
                      StorageEngine storageEngine, ExecutionEngine executionEngine,
                      BuiltinFunctionRepository functionRepository) {
        ...

        registerSQLFunctions();
    }
    
    private void **registerSQLFunctions**() {
        functionRepository.register(substring());
    }
    
    private FunctionResolver substring() {
        return new FunctionResolver(
            Substring.FUNCTION_NAME,
            ImmutableMap.of(
                new FunctionSignature(Substring.FUNCTION_NAME, Arrays.asList(STRING, INTEGER, INTEGER)),
                Substring::new // Function builder
            )
        );
    }
```

### 4.4 Custom Script Engine

The next challenge is to implement a custom script language engine in Elasticsearch.

**4.4.1 Expression Serialization**

Since we can only pass string to Elasticsearch DSL, we need some format to encode the entire expression tree which is a tree of Java object. In this PoC, JDK serialization is in use. A big drawback is it requires everything implements `Serializable` interface which is hassle especially for inner class and lambda.

```
public class **ElasticsearchIndexScan** extends TableScanOperator {
    ...

    public PhysicalPlan pushDown(LogicalFilter filter) {
        request.getSourceBuilder().query(
            QueryBuilders.scriptQuery(
                new Script(
                    Script.DEFAULT_SCRIPT_TYPE,
                    ExpressionScriptEngine.EXPRESSION_LANG_NAME,
                    **serialize****(filter.getCondition****())**,
                    emptyMap()
                )
            )
        );
        return this; // Assume everything pushed down and nothing needed for physical filter operator
    }

    private String serialize(Expression expression) {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            ObjectOutputStream objectOutput = new ObjectOutputStream(output);
            objectOutput.writeObject(expression);
            objectOutput.flush();
            return Base64.getEncoder().encodeToString(output.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize expression: " + expression, e);
        }
    }
```

**4.4.2 Script Factory**

Because a script is execute on each document in an Elasticsearch index, we need to compile it ahead and store for reuse. ***Note that in fact our expression is already compiled in Analyzer, so what is needed here is deserialization only***.

```
public class **ExpressionScriptEngine** implements ScriptEngine {

    public static final String EXPRESSION_LANG_NAME = "opendistro_expression";
    ...

    @Override
    public <FactoryType> FactoryType compile(String templateName,
                                             String templateSource,
                                             ScriptContext<FactoryType> context,
                                             Map<String, String> params) {

        Expression expression = compile(templateSource);
        ExpressionScriptFactory factory = new ExpressionScriptFactory(expression);
        return context.factoryClazz.cast(factory);
    }
    
    private Expression compile(String source) {
        try {
            ByteArrayInputStream input = new ByteArrayInputStream(Base64.getDecoder().decode(source));
            ObjectInputStream objectInput = new ObjectInputStream(input);
            return (Expression) objectInput.readObject();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize source: " + source, e);
        }
    }
```

**4.4.3 Script Execution**

Finally, we implement execute script logic. In our case it is evaluating expression with a value environment. Basically we check which fields are present in expression tree. And then we extract its value from the document being processed. At last we convert the field name and value map to binding tuple and evaluate expression tree on it.

```
    private static class **ExpressionScript** extends FilterScript {
        private final Expression expression;
        ...

        @Override
        public boolean execute() {
            // Check we ourselves are not being called by unprivileged code.
            SpecialPermission.check();

            return AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {

                // 1) getDoc() is not iterable; 2) Doc value is array; 3) Get text field ends up with exception
                Set<String> fieldNames = extractInputFieldNames();
                Map<String, Object> values = extractFieldNameAndValues(fieldNames);
                ExprValue result = evaluateExpression(values);
                return (Boolean) result.value();
            });
        }
        
        private ExprValue **evaluateExpression**(Map<String, Object> values) {
            ExprValue tupleValue = ExprValueUtils.tupleValue(values);
            ExprValue result = expression.valueOf(tupleValue.bindingTuples());

            if (result.type() != ExprType.BOOLEAN) {
                throw new IllegalStateException("Expression has wrong result type: " + result);
            }
            return result;
        }
```

**4.4.4 Register Script Engine**

We have all pieces ready and we can register our script language engine to Elasticsearch now.

```
public class SQLPlugin extends Plugin implements ActionPlugin, **ScriptPlugin** {
    ...

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new **ExpressionScriptEngine**();
    }
}
```
