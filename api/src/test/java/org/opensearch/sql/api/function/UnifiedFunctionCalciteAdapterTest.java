/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.UnifiedQueryTestBase;

public class UnifiedFunctionCalciteAdapterTest extends UnifiedQueryTestBase {

  private RexBuilder rexBuilder;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    rexBuilder = context.getPlanContext().rexBuilder;
  }

  @Test
  public void testCreateFunction() {
    UnifiedFunction upperFunc =
        UnifiedFunctionCalciteAdapter.create(rexBuilder, "UPPER", List.of("VARCHAR"));

    assertNotNull(upperFunc);
    assertEquals("UPPER", upperFunc.getFunctionName());
    assertEquals(List.of("VARCHAR"), upperFunc.getInputTypes());
    assertEquals("VARCHAR", upperFunc.getReturnType());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateWithInvalidFunctionName() {
    UnifiedFunctionCalciteAdapter.create(rexBuilder, "INVALID_FUNCTION", List.of("VARCHAR"));
  }

  @Test
  public void testEvaluateFunction() {
    UnifiedFunction upperFunc =
        UnifiedFunctionCalciteAdapter.create(rexBuilder, "UPPER", List.of("VARCHAR"));

    Object result = upperFunc.eval(List.of("hello"));
    assertEquals("HELLO", result);
  }

  @Test
  public void testSerializeAndDeserialize() throws Exception {
    UnifiedFunctionCalciteAdapter originalFunc =
        UnifiedFunctionCalciteAdapter.create(rexBuilder, "UPPER", List.of("VARCHAR"));

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(originalFunc);
    }

    // Deserialize
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    UnifiedFunctionCalciteAdapter deserializedFunc;
    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
      deserializedFunc = (UnifiedFunctionCalciteAdapter) ois.readObject();
    }

    // Verify metadata is preserved
    assertNotNull(deserializedFunc);
    assertEquals(originalFunc.getFunctionName(), deserializedFunc.getFunctionName());
    assertEquals(originalFunc.getInputTypes(), deserializedFunc.getInputTypes());
    assertEquals(originalFunc.getReturnType(), deserializedFunc.getReturnType());

    // Verify functionality is preserved after deserialization
    Object result = deserializedFunc.eval(List.of("hello"));
    assertEquals("HELLO", result);
  }
}
