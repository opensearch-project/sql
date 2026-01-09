/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function.calcite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);

    UnifiedFunction upperFunc =
        UnifiedFunctionCalciteAdapter.create("UPPER", rexBuilder, List.of(input));

    assertNotNull(upperFunc);
    assertEquals("UPPER", upperFunc.getFunctionName());
    assertEquals(List.of("VARCHAR"), upperFunc.getInputTypes());
    assertEquals("VARCHAR", upperFunc.getReturnType());
  }

  @Test
  public void testEvaluateUpperFunction() {
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);

    UnifiedFunction upperFunc =
        UnifiedFunctionCalciteAdapter.create("UPPER", rexBuilder, List.of(input));

    Object result = upperFunc.eval(List.of("hello"));

    assertEquals("HELLO", result);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateWithInvalidFunctionName() {
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);

    UnifiedFunctionCalciteAdapter.create("INVALID_FUNCTION", rexBuilder, List.of(input));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEvaluateWithWrongNumberOfArguments() {
    RexNode input =
        rexBuilder.makeInputRef(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), 0);
    UnifiedFunction upperFunc =
        UnifiedFunctionCalciteAdapter.create("UPPER", rexBuilder, List.of(input));

    upperFunc.eval(List.of("hello", "world"));
  }
}
