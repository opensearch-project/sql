# Developing PPL Functions

This guide explains how to develop and implement functions for PPL with Calcite.

## Prerequisites

- [ ] Create an issue describing the purpose and expected behavior of the function
- [ ] Ensure the function name is recognized by PPL syntax by checking ``OpenSearchPPLLexer.g4``,
  ``OpenSearchPPLParser.g4``, and ``BuiltinFunctionName.java``
- [ ] Plan the documentation of the function under ``docs/user/ppl/functions/`` directory

## User-Defined Functions (UDFs)

A user-defined function is an instance
of [SqlOperator](https://calcite.apache.org/javadocAggregate/org/apache/calcite/sql/SqlOperator.html) that transforms
input row expressions ([RexNode](https://calcite.apache.org/javadocAggregate/org/apache/calcite/rex/RexNode.html)) into
a new one.

### Creating UDFs

There are mainly three approaches to implementing UDFs:

#### 1. Use existing Calcite operators

Leverage operators already declared in
Calcite's [SqlStdOperatorTable](https://calcite.apache.org/javadocAggregate/org/apache/calcite/sql/fun/SqlStdOperatorTable.html)
or [SqlLibraryOperators](https://calcite.apache.org/javadocAggregate/org/apache/calcite/sql/fun/SqlLibraryOperators.html),
and defined
in [RexImpTable.java](https://calcite.apache.org/javadocAggregate/org/apache/calcite/adapter/enumerable/RexImpTable.html).
For example, `SqlStdOperatorTable.PLUS` is used as one of the implementations for `+` in PPL.

This approach is useful when the function you need to implement already exists in Apache Calcite and you just need to
expose it through your PPL interface.

#### 2. Adapt existing static methods

Adapt Java static methods to UDFs using utility functions like `UserDefinedFunctionUtils.adapt*ToUDF`.

This approach allows you to leverage existing Java methods by wrapping them as UDFs, which can be more straightforward
than implementing from scratch.

Among existing adaptation utilities, `adaptExprMethodToUDF` adapts a v2 function implementation into a UDF builder, while `adaptMathFunctionToUDF` adapts a static function from `java.lang.Math` to a
UDF builder. You can create your own adaptation utilities if you need to adapt other kinds of static methods to UDFs.

Example:

```java
SqlOperator SINH = adaptMathFunctionToUDF(
        "sinh", ReturnTypes.DOUBLE_FORCE_NULLABLE, NullPolicy.ANY, PPLOperandTypes.NUMERIC)
        .toUDF("SINH");
```

#### 3. Implement from scratch

For more complex functions or when you need complete control over the implementation:

1. Implement the `ImplementorUDF` interface, which is a simplified interface for creating
   a [SqlUserDefinedFunction](https://calcite.apache.org/javadocAggregate/org/apache/calcite/sql/validate/SqlUserDefinedFunction.html).
2. Instantiate and convert it to a `SqlOperator` in `PPLBuiltinOperators`
3. For optimal UDF performance, implement any data-independent logic during the compilation phase instead of at runtime.
   Specifically,
   use [linq4j expressions](https://calcite.apache.org/javadocAggregate/org/apache/calcite/linq4j/tree/Expression.html)
   for these operations rather than internal static method calls, as expressions are evaluated during compilation.

Example:

```java
public class MyCustomUDF extends ImplementorUDF {
    // Define operand types, return types, null policies, and constructors
    // ...

    public static class Crc32Implementor implements NotNullImplementor {
        @Override
        public Expression implement(
                RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
            // Implementation details goes here ...
        }
    }
}

// Converting to SqlUserDefinedFunction (an extension of SqlOperator)
SqlOperator myOperator = new MyCustomUDF().toUDF("FUNC_NAME");
```

### Type Checking for UDFs

Type checking ensures that functions receive the correct argument types:

- Each `SqlOperator` provides an operand type checker via its [
  `getOperandTypeChecker`](https://calcite.apache.org/javadocAggregate/org/apache/calcite/sql/SqlOperator.html#getOperandTypeChecker())
  method
- Calcite's built-in operators come with predefined type checkers of type [`SqlOperandTypeChecker`](https://calcite.apache.org/javadocAggregate/org/apache/calcite/sql/type/SqlOperandTypeChecker.html)
- For custom UDFs, the `UDFOperandMetadata` interface is used to feed function type information so that a
  `SqlOperandTypeChecker` can be retrieved in the same way as Calcite's built-in operators. Most of the operand types
  are defined in `PPLOperandTypes` as instances of `UDFOperandMetadata`. E.g. `PPLOperandTypes.NUMERIC_NUMERIC`
-  Since `SqlOperandTypeChecker` works on parsed SQL trees (which aren't directly accessible in our architecture), the
  `PPLTypeChecker` interface was created to perform actual type checking. Most instances of `PPLTypeChecker` are created
  by wrapping Calcite's built-in type checkers.

The following code snippet explains their relationships:

```java
// For built-in Calcite operators
SqlOperandTypeChecker cosSqlTypeChecker = SqlStdOperatorTable.COS.getOperandTypeChecker(); // FamilyOperandTypeChecker(NUMERIC)

// For user defined functions
// UDFOperandMetadata wraps a SqlOperandTypeChecker, so that the type information can be fed to a SqlUserDefinedFunction.
// Refer to the javadoc of UDFOperandMetadata class for more details on why this workaround is necessary
UDFOperandMetadata NUMERIC = UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.NUMERIC);
SqlOperator COSH =
        adaptMathFunctionToUDF(
                "cosh", ReturnTypes.DOUBLE_FORCE_NULLABLE, NullPolicy.ANY, NUMERIC)
                .toUDF("COSH");
SqlOperandTypeChecker coshTypeChecker = COSH.getOperandTypeChecker().getInnerTypeChecker(); // FamilyOperandTypeChecker(NUMERIC)

// SqlOperandTypeChecker works on parsed SQL trees, which don't exist in our architecture, so it cannot be directly
// applied to check operand types. We create another interface PPLTypeChecker to do the actual type checking.
// It works by retrieving operand type information from a SqlOperandTypeChecker, then checking against actual argument types.
PPLTypeChecker cosPplTypeChecker = PPLTypeChecker.wrapFamily(cosSqlTypeChecker);
// Equivalently, PPL type checkers can be created by directly specifying expected operand types
PPLTypeChecker numericTypeChecker = PPLTypeChecker.family(SqlTypeFamily.NUMERIC);
```

### Registering UDFs

#### Preferred registration API

UDFs should be registered in `PPLFuncImpTable`. The preferred API is

```java
AbstractBuilder::

registerOperator(BuiltinFunctionName functionName, SqlOperator... operators)`
```

- It automatically extracts type checkers from operators and converts them to `PPLTypeChecker` instances
- Multiple implementations can be registered to the same function name for overloading
- The system will try to resolve functions based on argument types, with automatic coercion when needed

For example, the following statement registers calcite's built-in `COS` operator as the cosine function in PPL. Under the
hood, it first retrieves a `SqlOperandTypeChecker` from `SqlStdOperatorTable.COS`, then converts it to a `PPLTypeChecker`,
finally registers it as `cos` function in PPL function registry.

```java
registerOperator(COS, SqlStdOperatorTable.COS);
```

The following example shows how to register overloadings to the same function name. `+` operator is registered for both
and number addition and string concatenation, controlled via type checkers. I.e. if both operands are number, they will
be resolved to `SqlStdOperatorTable.PLUS` since the operand types does not pass the type checking of
`SqlStdOperatorTable.CONCAT`,
which requires two strings.

```java
registerOperator(ADD, SqlStdOperatorTable.PLUS, SqlStdOperatorTable.CONCAT);
```

#### Lower-level registration API

```java
AbstractBuilder::

register(BuiltinFunctionName functionName, FunctionImp functionImp, PPLTypeChecker typeChecker)
```

Use this approach when:

- You need a custom type checker
- You want to customize an existing function by tweaking its arguments
- Setting `typeChecker` to `null` will bypass type checking (use with caution)

### External Functions

Some functions implementation depend on underlying data sources. They should be registered with
`PPLFuncImpTable::registerExternalOperator`
For example, the `GEOIP` function relies on
the [opensearch-geospatial](https://github.com/opensearch-project/geospatial) plugin. It is registered as an external
function in `OpenSearchExecutionEngine`.

### Testing UDFs

Comprehensive testing is essential for UDFs:

- Integration tests in `Calcite*IT` classes to verify function result correctness
- Unit tests in `CalcitePPLFunctionTypeTest` to validate type checker behavior
- Push-down tests in `CalciteExplainIT` if the function can be pushed down as a domain-specific language (DSL)

## User-Defined Aggregation Functions (UDAFs)

User-defined aggregation functions aggregate data across multiple rows.

### Creating UDAFs

There are two main approaches to create a UDAF:

#### 1. Use existing Calcite aggregation operators

Leverage existing aggregation operators from Calcite if they match your requirements.

#### 2. Implement from scratch

For custom aggregation logic:

1. Extend `SqlUserDefinedAggFunction` with custom aggregation logic
2. Instantiate the new aggregation function in `PPLBuiltinOperators`

### Registering UDAFs

- Use `AggBuilder::registerOperator(BuiltinFunctionName functionName, SqlAggFunction aggFunction)` for standard
  registration
- For more control, use
  `AggBuilder::register(BuiltinFunctionName functionName, AggHandler aggHandler, PPLTypeChecker typeChecker)`
- For functions dependent on data engines, use `PPLFuncImpTable::registerExternalAggOperator`

### Testing UDAFs

- Verify result correctness in `CalcitePPLAggregationIT`
- Test logical plans in `CalcitePPLAggregationTest`