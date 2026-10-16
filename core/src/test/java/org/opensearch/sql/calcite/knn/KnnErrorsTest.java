/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.knn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.SemanticCheckException;

class KnnErrorsTest {

  /** Every error factory, including ones added later, so none can forget the no-fallback mark. */
  private static List<Method> factories() {
    return Arrays.stream(KnnErrors.class.getDeclaredMethods())
        .filter(m -> Modifier.isPublic(m.getModifiers()) && Modifier.isStatic(m.getModifiers()))
        .filter(m -> m.getReturnType() == SemanticCheckException.class)
        .toList();
  }

  private static Object argumentFor(Class<?> type) {
    if (type == int.class) {
      return 1;
    }
    if (type == List.class) {
      return List.of("x");
    }
    return "x";
  }

  @Test
  void everyFactoryIsMarkedToNeverFallBackToV2() throws Exception {
    List<Method> factories = factories();
    assertFalse(factories.isEmpty());
    for (Method factory : factories) {
      Object[] args =
          Arrays.stream(factory.getParameterTypes()).map(KnnErrorsTest::argumentFor).toArray();
      SemanticCheckException error = (SemanticCheckException) factory.invoke(null, args);
      assertTrue(error.isWithoutCalciteFallback(), factory.getName());
    }
  }

  @Test
  void vectorMappingMismatchNamesTheIndicesIfKnown() {
    assertEquals(
        "indices [a, b] have different vector mappings for [f]",
        KnnErrors.vectorMappingMismatch(List.of("a", "b"), "f").getMessage());
    assertEquals(
        "the indices of the query have different vector mappings for [f]",
        KnnErrors.vectorMappingMismatch(List.of(), "f").getMessage());
  }

  @Test
  void scoreReplacedNamesTheCommand() {
    SemanticCheckException error = KnnErrors.scoreReplaced("bin");
    assertTrue(error.isWithoutCalciteFallback());
    assertEquals(
        "after knn, _score is the k-NN score; [bin] cannot replace it, give the result another"
            + " name",
        error.getMessage());
  }
}
