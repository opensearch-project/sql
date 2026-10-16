/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;

/**
 * {@link ExprCoreType#VECTOR} is Calcite-only: the V2 engine registers its functions over the same
 * types as before VECTOR was added, so its signatures and error texts are unchanged.
 */
class V2SignaturesWithoutVectorTest {

  /** The signature list of a V2 operator over every type, as the base commit prints it. */
  private static String allTypes(int arity) {
    return Arrays.stream(
            new String[] {
              "BYTE",
              "SHORT",
              "INTEGER",
              "LONG",
              "FLOAT",
              "DOUBLE",
              "STRING",
              "BOOLEAN",
              "DATE",
              "TIME",
              "TIMESTAMP",
              "INTERVAL",
              "IP",
              "GEO_POINT",
              "BINARY",
              "STRUCT",
              "ARRAY"
            })
        .map(type -> "[" + String.join(",", java.util.Collections.nCopies(arity, type)) + "]")
        .reduce((a, b) -> a + "," + b)
        .map(list -> "{" + list + "}")
        .orElseThrow();
  }

  @Test
  void coreTypesAreTheV2TypesOfBefore() {
    assertFalse(ExprCoreType.coreTypes().contains(ExprCoreType.VECTOR));
    assertFalse(ExprCoreType.v2Types().contains(ExprCoreType.VECTOR));
    List<ExprCoreType> all = Arrays.asList(ExprCoreType.values());
    assertTrue(all.contains(ExprCoreType.VECTOR));
    assertEquals(all.size() - 1, ExprCoreType.v2Types().size());
    assertTrue(ExprCoreType.v2Types().contains(ExprCoreType.UNKNOWN));
    assertTrue(ExprCoreType.v2Types().contains(ExprCoreType.UNDEFINED));
  }

  @Test
  void equalErrorTextIsUnchanged() {
    ExpressionEvaluationException e =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> DSL.equal(DSL.literal(1), DSL.literal(true)));
    assertEquals(
        "= function expected " + allTypes(2) + ", but got [INTEGER,BOOLEAN]", e.getMessage());
  }

  @Test
  void ifnullErrorTextIsUnchanged() {
    ExpressionEvaluationException e =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> DSL.ifnull(DSL.literal(1), DSL.literal(true)));
    assertEquals(
        "ifnull function expected " + allTypes(2) + ", but got [INTEGER,BOOLEAN]", e.getMessage());
  }

  @Test
  void ifErrorTextIsUnchanged() {
    ExpressionEvaluationException e =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> DSL.iffunction(DSL.literal(1), DSL.literal(1), DSL.literal(true)));
    assertTrue(
        e.getMessage().startsWith("if function expected {[BOOLEAN,BYTE,BYTE],"), e.getMessage());
    assertTrue(e.getMessage().contains("[BOOLEAN,ARRAY,ARRAY]}"), e.getMessage());
    assertFalse(e.getMessage().contains("VECTOR"), e.getMessage());
  }
}
