/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function.calcite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import org.apache.calcite.rex.RexBuilder;
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
    UnifiedFunction upperFunc =
        UnifiedFunctionCalciteAdapter.create("UPPER", rexBuilder, List.of("VARCHAR"));

    assertNotNull(upperFunc);
    assertEquals("UPPER", upperFunc.getFunctionName());
    assertEquals(List.of("VARCHAR"), upperFunc.getInputTypes());
    assertEquals("VARCHAR", upperFunc.getReturnType());
  }

  @Test
  public void testEvaluateUpperFunction() {
    UnifiedFunction upperFunc =
        UnifiedFunctionCalciteAdapter.create("UPPER", rexBuilder, List.of("VARCHAR"));

    Object result = upperFunc.eval(List.of("hello"));

    assertEquals("HELLO", result);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateWithInvalidFunctionName() {
    UnifiedFunctionCalciteAdapter.create("INVALID_FUNCTION", rexBuilder, List.of("VARCHAR"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEvaluateWithWrongNumberOfArguments() {
    UnifiedFunction upperFunc =
        UnifiedFunctionCalciteAdapter.create("UPPER", rexBuilder, List.of("VARCHAR"));

    upperFunc.eval(List.of("hello", "world"));
  }
}
