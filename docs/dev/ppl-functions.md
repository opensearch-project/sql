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

- Each `SqlOperator` exposes an operand type checker via [`getOperandTypeChecker()`](https://calcite.apache.org/javadocAggregate/org/apache/calcite/sql/SqlOperator.html#getOperandTypeChecker())
- Calcite's built-in operators include predefined type checkers of type [`SqlOperandTypeChecker`](https://calcite.apache.org/javadocAggregate/org/apache/calcite/sql/type/SqlOperandTypeChecker.html)
- Custom UDFs define type checkers by implementing `getOperandMetadata()`, which returns a `UDFOperandMetadata`. This wrapper typically wraps a `SqlOperandTypeChecker`, allowing reuse of existing type checkers while remaining compatible with user-defined functions. Common operand type combinations are predefined in `PPLOperandTypes` (e.g., `PPLOperandTypes.NUMERIC_NUMERIC`)

The following example illustrates the relationship between `SqlOperandTypeChecker` and `UDFOperandMetadata`:

```java
// Built-in Calcite operators have type checkers directly
SqlOperandTypeChecker cosSqlTypeChecker = SqlStdOperatorTable.COS.getOperandTypeChecker();
// -> FamilyOperandTypeChecker(NUMERIC)

// For UDFs, wrap a SqlOperandTypeChecker in UDFOperandMetadata.
// This wrapper is necessary because SqlUserDefinedFunction requires a different interface.
// See UDFOperandMetadata javadoc for details.
UDFOperandMetadata NUMERIC = UDFOperandMetadata.wrap(OperandTypes.NUMERIC);
SqlOperator COSH = adaptMathFunctionToUDF(
        "cosh", ReturnTypes.DOUBLE_FORCE_NULLABLE, NullPolicy.ANY, NUMERIC)
        .toUDF("COSH");

// Extract the underlying type checker from a UDF
SqlOperandTypeChecker coshTypeChecker = COSH.getOperandTypeChecker().getInnerTypeChecker();
// -> FamilyOperandTypeChecker(NUMERIC)
```

### Registering UDFs

#### Preferred Registration API

Register UDFs in `PPLFuncImpTable` using the preferred API:

```java
registerOperator(BuiltinFunctionName functionName, SqlOperator operator);
```

For example, the following registers Calcite's built-in `COS` operator as the cosine function in PPL:

```java
registerOperator(BuiltinFunctionName.COS, SqlStdOperatorTable.COS);
```

#### Lower-Level Registration API

For more control, use the lower-level `register` method to register multiple overloads under the same function name:

```java
register(BuiltinFunctionName functionName, FunctionImp functionImp);
```

The following example registers the `+` operator for both numeric addition and string concatenation. The implementation examines the argument types at resolution time: if all operands are strings, it resolves to `SqlStdOperatorTable.CONCAT`; otherwise, it resolves to `SqlStdOperatorTable.PLUS` with arguments coerced to numbers.

```java
register(BuiltinFunctionName.ADD,
         (builder, args) -> {
            SqlOperator op =
                (Stream.of(args).map(RexNode::getType).allMatch(OpenSearchTypeUtil::isCharacter))
                    ? SqlStdOperatorTable.CONCAT
                    : SqlStdOperatorTable.PLUS;
            return builder.makeCall(op, args);
          });
```

Use this approach when:

- You need to overload a function with different behavior based on argument types
- You want to compose a new function from existing operators

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
  `AggBuilder::register(BuiltinFunctionName functionName, AggHandler aggHandler)`
- For functions dependent on data engines, use `PPLFuncImpTable::registerExternalAggOperator`

### Testing UDAFs

- Verify result correctness in `CalcitePPLAggregationIT`
- Test logical plans in `CalcitePPLAggregationTest`