/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.flatten;

import java.util.Map;
import lombok.ToString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;

@ToString
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class FlattenOperatorTest extends PhysicalPlanTestBase {
  @Mock private PhysicalPlan inputPlan;

  // Define input values for testing.
  private final ExprValue integerExprValue = ExprValueUtils.integerValue(0);
  private final ExprValue doubleExprValue = ExprValueUtils.doubleValue(0.0);
  private final ExprValue stringExprValue = ExprValueUtils.stringValue("value");

  private final ExprValue structEmptyExprValue = ExprValueUtils.tupleValue(Map.of());
  private final ExprValue structNullExprValue = ExprValueUtils.nullValue();
  private final ExprValue structMissingExprValue = ExprValueUtils.missingValue();

  private final ExprValue structNestedExprValue =
      ExprValueUtils.tupleValue(Map.of("string", stringExprValue));

  private final ExprValue structExprValue =
      ExprValueUtils.tupleValue(
          Map.ofEntries(
              Map.entry("integer", integerExprValue),
              Map.entry("double", doubleExprValue),
              Map.entry("struct_nested", structNestedExprValue)));

  private final ExprValue rowValue =
      ExprValueUtils.tupleValue(
          Map.ofEntries(
              Map.entry("struct_empty", structEmptyExprValue),
              Map.entry("struct_null", structNullExprValue),
              Map.entry("struct_missing", structMissingExprValue),
              Map.entry("struct", structExprValue)));

  @BeforeEach
  void setup() {

    // Mock input values.
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(rowValue);
  }

  @Test
  void testStructEmpty() {
    ExprValue actual = execute(flatten(inputPlan, DSL.ref("struct_empty", STRUCT))).getFirst();
    assertEquals(rowValue, actual);
  }

  @Test
  void testStructNull() {
    ExprValue actual = execute(flatten(inputPlan, DSL.ref("struct_null", STRUCT))).getFirst();
    assertEquals(rowValue, actual);
  }

  @Test
  void testStructMissing() {
    ExprValue actual = execute(flatten(inputPlan, DSL.ref("struct_missing", STRUCT))).getFirst();
    assertEquals(rowValue, actual);
  }

  @Test
  void testStruct() {
    ExprValue actual = execute(flatten(inputPlan, DSL.ref("struct", STRUCT))).getFirst();

    ExprValue expected =
        ExprValueUtils.tupleValue(
            Map.ofEntries(
                Map.entry("struct_empty", structEmptyExprValue),
                Map.entry("struct_null", structNullExprValue),
                Map.entry("struct_missing", structMissingExprValue),
                Map.entry("struct", structExprValue),
                Map.entry("integer", integerExprValue),
                Map.entry("double", doubleExprValue),
                Map.entry("struct_nested", structNestedExprValue)));

    assertEquals(expected, actual);
  }

  @Test
  void testStructNested() {
    ExprValue actual =
        execute(flatten(inputPlan, DSL.ref("struct.struct_nested", STRUCT))).getFirst();

    ExprValue expected =
        ExprValueUtils.tupleValue(
            Map.ofEntries(
                Map.entry("struct_empty", structEmptyExprValue),
                Map.entry("struct_null", structNullExprValue),
                Map.entry("struct_missing", structMissingExprValue),
                Map.entry(
                    "struct",
                    ExprValueUtils.tupleValue(
                        Map.ofEntries(
                            Map.entry("integer", integerExprValue),
                            Map.entry("double", doubleExprValue),
                            Map.entry("struct_nested", structNestedExprValue),
                            Map.entry("string", stringExprValue))))));

    assertEquals(expected, actual);
  }

  @Test
  void testAncestorStructNull() {
    ExprValue actual = execute(flatten(inputPlan, DSL.ref("struct_null.path", STRUCT))).getFirst();
    assertEquals(rowValue, actual);
  }

  @Test
  void testAncestorStructMissing() {
    ExprValue actual =
        execute(flatten(inputPlan, DSL.ref("struct_missing.path", STRUCT))).getFirst();
    assertEquals(rowValue, actual);
  }

  @Test
  void testPathMissing() {
    ExprValue actual = execute(flatten(inputPlan, DSL.ref("struct.unknown", STRUCT))).getFirst();
    assertEquals(rowValue, actual);
  }

  @Test
  void testAncestorPathMissing() {
    ExprValue actual = execute(flatten(inputPlan, DSL.ref("unknown", STRUCT))).getFirst();
    assertEquals(rowValue, actual);
  }

  @Test
  void testInvalidType() {
    Exception ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> execute(flatten(inputPlan, DSL.ref("struct.integer", INTEGER))));
    assertEquals("invalid to get tupleValue from value of type INTEGER", ex.getMessage());
  }
}
