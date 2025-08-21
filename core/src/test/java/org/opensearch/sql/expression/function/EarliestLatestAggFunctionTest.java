/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Set;
import org.junit.jupiter.api.Test;

public class EarliestLatestAggFunctionTest extends AggFunctionTestBase {

  @Test
  void testEarliestFunctionIsRegistered() {
    assertFunctionIsRegistered(BuiltinFunctionName.EARLIEST);
  }

  @Test
  void testLatestFunctionIsRegistered() {
    assertFunctionIsRegistered(BuiltinFunctionName.LATEST);
  }

  @Test
  void testBuiltinFunctionNameMapping() {
    assertFunctionNameMapping(BuiltinFunctionName.EARLIEST, "earliest");
    assertFunctionNameMapping(BuiltinFunctionName.LATEST, "latest");
  }

  @Test
  void testFunctionNameResolution() {
    assertFunctionNameResolution("earliest", BuiltinFunctionName.EARLIEST);
    assertFunctionNameResolution("latest", BuiltinFunctionName.LATEST);
  }

  @Test
  void testResolveAggWithValidFunctions() {
    try {
      java.lang.reflect.Method method =
          PPLFuncImpTable.class.getDeclaredMethod(
              "resolveAgg",
              BuiltinFunctionName.class,
              boolean.class,
              org.apache.calcite.rex.RexNode.class,
              java.util.List.class,
              org.opensearch.sql.calcite.CalcitePlanContext.class);

      assertNotNull(method, "resolveAgg method should exist");
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("resolveAgg method not found", e);
    }
  }

  @Test
  void testFunctionRegistryIntegrity() {
    assertFunctionsAreRegistered(
        BuiltinFunctionName.MAX_BY,
        BuiltinFunctionName.MIN_BY,
        BuiltinFunctionName.EARLIEST,
        BuiltinFunctionName.LATEST);
  }

  @Test
  void testFunctionHandlerTypes() {
    assertFunctionHandlerTypes(BuiltinFunctionName.EARLIEST, BuiltinFunctionName.LATEST);
  }

  @Test
  void testFunctionRegistrySize() {
    assertRegistryMinimumSize(10);

    Set<BuiltinFunctionName> knownFunctions =
        Set.of(
            BuiltinFunctionName.COUNT,
            BuiltinFunctionName.SUM,
            BuiltinFunctionName.AVG,
            BuiltinFunctionName.MAX,
            BuiltinFunctionName.MIN,
            BuiltinFunctionName.MAX_BY,
            BuiltinFunctionName.MIN_BY,
            BuiltinFunctionName.EARLIEST,
            BuiltinFunctionName.LATEST);

    assertKnownFunctionsPresent(knownFunctions);
  }

  @Test
  void testEarliestLatestFunctionNamesInEnum() {
    assertFunctionNamesInEnum(BuiltinFunctionName.EARLIEST, BuiltinFunctionName.LATEST);
  }
}
