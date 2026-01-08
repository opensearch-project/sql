/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function.calcite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.api.function.UnifiedFunction;

/** Unit tests for {@link UnifiedFunctionCalciteAdapter}. */
public class UnifiedFunctionCalciteAdapterTest extends UnifiedQueryTestBase {

  private RexBuilder rexBuilder;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    rexBuilder = context.getPlanContext().rexBuilder;
  }

  @Test
  public void testCreateUpperFunction() {
    // Create input reference for VARCHAR type
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);

    // Create UPPER function adapter
    UnifiedFunction upperFunc =
        UnifiedFunctionCalciteAdapter.create("UPPER", rexBuilder, List.of(input));

    // Verify function metadata
    assertNotNull(upperFunc);
    assertEquals("UPPER", upperFunc.getFunctionName());
    assertEquals(List.of("VARCHAR"), upperFunc.getInputTypes());
    assertEquals("VARCHAR", upperFunc.getReturnType());
    // Note: Nullability depends on Calcite's function definition
  }

  @Test
  public void testEvaluateUpperFunction() {
    // Create input reference for VARCHAR type
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);

    // Create UPPER function adapter
    UnifiedFunction upperFunc =
        UnifiedFunctionCalciteAdapter.create("UPPER", rexBuilder, List.of(input));

    // Evaluate function
    Object result = upperFunc.eval(List.of("hello"));

    // Verify result
    assertEquals("HELLO", result);
  }

  @Test
  public void testCreateAbsFunction() {
    // Create input reference for INTEGER type
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER), 0);

    // Create ABS function adapter
    UnifiedFunction absFunc =
        UnifiedFunctionCalciteAdapter.create("ABS", rexBuilder, List.of(input));

    // Verify function metadata
    assertNotNull(absFunc);
    assertEquals("ABS", absFunc.getFunctionName());
    assertEquals(List.of("INTEGER"), absFunc.getInputTypes());
    assertEquals("INTEGER", absFunc.getReturnType());
  }

  @Test
  public void testEvaluateAbsFunction() {
    // Create input reference for INTEGER type
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER), 0);

    // Create ABS function adapter
    UnifiedFunction absFunc =
        UnifiedFunctionCalciteAdapter.create("ABS", rexBuilder, List.of(input));

    // Evaluate function with negative number
    Object result = absFunc.eval(List.of(-42));

    // Verify result
    assertEquals(42, result);
  }

  @Test
  public void testCreateConcatFunction() {
    // Create input references for VARCHAR types
    RexNode input1 =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);
    RexNode input2 =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 1);

    // Create CONCAT function adapter
    UnifiedFunction concatFunc =
        UnifiedFunctionCalciteAdapter.create("CONCAT", rexBuilder, List.of(input1, input2));

    // Verify function metadata
    assertNotNull(concatFunc);
    assertEquals("CONCAT", concatFunc.getFunctionName());
    assertEquals(List.of("VARCHAR", "VARCHAR"), concatFunc.getInputTypes());
    assertEquals("VARCHAR", concatFunc.getReturnType());
  }

  @Test
  public void testEvaluateConcatFunction() {
    // Create input references for VARCHAR types
    RexNode input1 =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);
    RexNode input2 =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 1);

    // Create CONCAT function adapter
    UnifiedFunction concatFunc =
        UnifiedFunctionCalciteAdapter.create("CONCAT", rexBuilder, List.of(input1, input2));

    // Evaluate function
    Object result = concatFunc.eval(List.of("Hello", "World"));

    // Verify result
    assertEquals("HelloWorld", result);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateWithInvalidFunctionName() {
    // Create input reference
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);

    // Try to create adapter with invalid function name
    UnifiedFunctionCalciteAdapter.create("INVALID_FUNCTION", rexBuilder, List.of(input));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEvaluateWithWrongNumberOfArguments() {
    // Create input reference for VARCHAR type
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);

    // Create UPPER function adapter (expects 1 argument)
    UnifiedFunction upperFunc =
        UnifiedFunctionCalciteAdapter.create("UPPER", rexBuilder, List.of(input));

    // Try to evaluate with wrong number of arguments
    upperFunc.eval(List.of("hello", "world"));
  }

  @Test
  public void testEqualsAndHashCode() {
    // Create two identical function adapters
    RexNode input1 =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);
    RexNode input2 =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);

    UnifiedFunction func1 =
        UnifiedFunctionCalciteAdapter.create("UPPER", rexBuilder, List.of(input1));
    UnifiedFunction func2 =
        UnifiedFunctionCalciteAdapter.create("UPPER", rexBuilder, List.of(input2));

    // Verify equals and hashCode
    assertEquals(func1, func2);
    assertEquals(func1.hashCode(), func2.hashCode());
  }

  @Test
  public void testToString() {
    // Create function adapter
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);
    UnifiedFunction upperFunc =
        UnifiedFunctionCalciteAdapter.create("UPPER", rexBuilder, List.of(input));

    // Verify toString contains function information
    String toString = upperFunc.toString();
    assertTrue(toString.contains("UPPER"));
    assertTrue(toString.contains("VARCHAR"));
  }

  @Test
  public void testCreateLowerFunction() {
    // Create input reference for VARCHAR type
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);

    // Create LOWER function adapter
    UnifiedFunction lowerFunc =
        UnifiedFunctionCalciteAdapter.create("LOWER", rexBuilder, List.of(input));

    // Verify function metadata
    assertNotNull(lowerFunc);
    assertEquals("LOWER", lowerFunc.getFunctionName());
    assertEquals(List.of("VARCHAR"), lowerFunc.getInputTypes());
    assertEquals("VARCHAR", lowerFunc.getReturnType());
    // Note: Nullability depends on Calcite's function definition
  }

  @Test
  public void testEvaluateLowerFunction() {
    // Create input reference for VARCHAR type
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);

    // Create LOWER function adapter
    UnifiedFunction lowerFunc =
        UnifiedFunctionCalciteAdapter.create("LOWER", rexBuilder, List.of(input));

    // Evaluate function
    Object result = lowerFunc.eval(List.of("HELLO"));

    // Verify result
    assertEquals("hello", result);
  }

  @Test
  public void testEvaluateWithDoubleType() {
    // Create input reference for DOUBLE type
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), 0);

    // Create ABS function adapter for double
    UnifiedFunction absFunc =
        UnifiedFunctionCalciteAdapter.create("ABS", rexBuilder, List.of(input));

    // Evaluate function with negative double
    Object result = absFunc.eval(List.of(-3.14));

    // Verify result - ABS returns the absolute value
    // Note: The actual return type depends on Calcite's type inference
    assertTrue(result instanceof Number);
    double resultValue = ((Number) result).doubleValue();
    assertTrue("Expected positive value", resultValue >= 3.0);
  }

  @Test
  public void testNullInputHandlingForNullableFunction() {
    // Create input reference for VARCHAR type
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);

    // Create UPPER function adapter
    UnifiedFunction upperFunc =
        UnifiedFunctionCalciteAdapter.create("UPPER", rexBuilder, List.of(input));

    // Note: Calcite's DataContext implementation (using Guava's ImmutableMap)
    // does not support null values in the map, so we cannot test null input handling
    // in this unit test. This would need to be tested with a custom DataContext
    // implementation or at the integration test level.

    // Instead, test with empty string
    Object result = upperFunc.eval(List.of(""));
    assertEquals("", result);
  }

  @Test
  public void testNullInputHandlingForAbsFunction() {
    // Create input reference for INTEGER type
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER), 0);

    // Create ABS function adapter
    UnifiedFunction absFunc =
        UnifiedFunctionCalciteAdapter.create("ABS", rexBuilder, List.of(input));

    // Note: Calcite's DataContext implementation (using Guava's ImmutableMap)
    // does not support null values in the map, so we cannot test null input handling
    // in this unit test. This would need to be tested with a custom DataContext
    // implementation or at the integration test level.

    // Instead, test with zero
    Object result = absFunc.eval(List.of(0));
    assertEquals(0, result);
  }
}
