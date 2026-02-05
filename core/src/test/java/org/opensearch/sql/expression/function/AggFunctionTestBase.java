/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import org.opensearch.sql.expression.function.PPLFuncImpTable.AggHandler;

public abstract class AggFunctionTestBase {

  @SuppressWarnings("unchecked")
  protected Map<BuiltinFunctionName, AggHandler> getAggFunctionRegistry() {
    try {
      PPLFuncImpTable funcTable = PPLFuncImpTable.INSTANCE;
      Field field = PPLFuncImpTable.class.getDeclaredField("aggFunctionRegistry");
      field.setAccessible(true);
      return (Map<BuiltinFunctionName, AggHandler>) field.get(funcTable);
    } catch (Exception e) {
      throw new RuntimeException("Failed to access aggFunctionRegistry", e);
    }
  }

  protected void assertFunctionIsRegistered(BuiltinFunctionName functionName) {
    Map<BuiltinFunctionName, AggHandler> registry = getAggFunctionRegistry();
    assertTrue(
        registry.containsKey(functionName),
        functionName.getName().getFunctionName()
            + " function should be registered in aggregate function registry");
    assertNotNull(
        registry.get(functionName),
        functionName.getName().getFunctionName() + " function handler should not be null");
  }

  protected void assertFunctionsAreRegistered(BuiltinFunctionName... functionNames) {
    for (BuiltinFunctionName functionName : functionNames) {
      assertFunctionIsRegistered(functionName);
    }
  }

  protected void assertFunctionHandlerTypes(BuiltinFunctionName... functionNames) {
    Map<BuiltinFunctionName, AggHandler> registry = getAggFunctionRegistry();
    for (BuiltinFunctionName functionName : functionNames) {
      AggHandler handler = registry.get(functionName);
      assertNotNull(
          handler, functionName.getName().getFunctionName() + " handler should not be null");
    }
  }

  protected void assertRegistryMinimumSize(int expectedMinimumSize) {
    Map<BuiltinFunctionName, AggHandler> registry = getAggFunctionRegistry();
    assertTrue(
        registry.size() >= expectedMinimumSize,
        "Registry should contain at least " + expectedMinimumSize + " aggregate functions");
  }

  protected void assertKnownFunctionsPresent(Set<BuiltinFunctionName> knownFunctions) {
    Map<BuiltinFunctionName, AggHandler> registry = getAggFunctionRegistry();
    long foundFunctions = registry.keySet().stream().filter(knownFunctions::contains).count();

    assertTrue(
        foundFunctions >= knownFunctions.size(),
        "Should have at least " + knownFunctions.size() + " known aggregate functions registered");
  }

  protected void assertFunctionNameResolution(
      String functionName, BuiltinFunctionName expectedEnum) {
    assertTrue(
        BuiltinFunctionName.of(functionName).isPresent(),
        "Should be able to resolve '" + functionName + "' function name");
    assertTrue(
        BuiltinFunctionName.of(functionName).get() == expectedEnum,
        "Resolved function should match expected enum value");
  }

  protected void assertFunctionNamesInEnum(BuiltinFunctionName... functionNames) {
    Set<BuiltinFunctionName> enumValues = Set.of(BuiltinFunctionName.values());

    for (BuiltinFunctionName functionName : functionNames) {
      assertTrue(
          enumValues.contains(functionName),
          functionName.getName().getFunctionName()
              + " should be defined in BuiltinFunctionName enum");
    }
  }

  protected void assertFunctionNameMapping(BuiltinFunctionName functionEnum, String expectedName) {
    assertTrue(
        functionEnum.getName().getFunctionName().equals(expectedName),
        "Function enum should map to expected name: " + expectedName);
  }
}
