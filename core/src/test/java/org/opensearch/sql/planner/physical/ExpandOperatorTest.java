/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.expand;

import java.util.List;
import java.util.Map;
import lombok.ToString;
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
class ExpandOperatorTest extends PhysicalPlanTestBase {
  @Mock private PhysicalPlan inputPlan;

  // Test constants
  private static final Integer integerValue = 0;
  private static final Double doubleValue = 0.0;
  private static final String stringValue = "value";

  @Test
  void testArrayEmpty() {
    mockInput(
        ExprValueUtils.tupleValue(
            Map.of("array_empty", ExprValueUtils.collectionValue(List.of()))));
    List<ExprValue> actualRows = execute(expand(inputPlan, DSL.ref("array_empty", ARRAY)));

    assertTrue(actualRows.isEmpty());
  }

  @Test
  void testArrayNull() {
    mockInput(ExprValueUtils.tupleValue(Map.of("array_empty", ExprValueUtils.nullValue())));
    List<ExprValue> actualRows = execute(expand(inputPlan, DSL.ref("array_empty", ARRAY)));

    assertTrue(actualRows.isEmpty());
  }

  @Test
  void testArrayMissing() {
    mockInput(ExprValueUtils.tupleValue(Map.of("array_missing", ExprValueUtils.missingValue())));
    List<ExprValue> actualRows = execute(expand(inputPlan, DSL.ref("array_missing", ARRAY)));

    assertTrue(actualRows.isEmpty());
  }

  @Test
  void testArrayUnknown() {
    List<ExprValue> actualRows = execute(expand(inputPlan, DSL.ref("array_unknown", ARRAY)));
    assertTrue(actualRows.isEmpty());
  }

  @Test
  void testArray() {
    mockInput(
        ExprValueUtils.tupleValue(
            Map.of("array", ExprValueUtils.collectionValue(List.of(integerValue, doubleValue)))));
    List<ExprValue> actualRows = execute(expand(inputPlan, DSL.ref("array", ARRAY)));

    List<ExprValue> expectedRows =
        List.of(
            ExprValueUtils.tupleValue(Map.of("array", integerValue)),
            ExprValueUtils.tupleValue(Map.of("array", doubleValue)));

    assertEquals(expectedRows, actualRows);
  }

  @Test
  void testArrayNested() {
    mockInput(
        ExprValueUtils.tupleValue(
            Map.of(
                "struct",
                ExprValueUtils.tupleValue(
                    Map.of(
                        "array_nested", ExprValueUtils.collectionValue(List.of(stringValue)))))));
    List<ExprValue> actualRows = execute(expand(inputPlan, DSL.ref("struct.array_nested", ARRAY)));

    List<ExprValue> expectedRows =
        List.of(
            ExprValueUtils.tupleValue(
                Map.of("struct", ExprValueUtils.tupleValue(Map.of("array_nested", stringValue)))));

    assertEquals(expectedRows, actualRows);
  }

  @Test
  void testAncestorNull() {
    mockInput(ExprValueUtils.tupleValue(Map.of("struct", ExprValueUtils.nullValue())));

    List<ExprValue> actualRows = execute(expand(inputPlan, DSL.ref("struct.array_nested", ARRAY)));
    assertTrue(actualRows.isEmpty());
  }

  @Test
  void testAncestorMissing() {
    mockInput(ExprValueUtils.tupleValue(Map.of("struct", ExprValueUtils.missingValue())));

    List<ExprValue> actualRows = execute(expand(inputPlan, DSL.ref("struct.array_nested", ARRAY)));
    assertTrue(actualRows.isEmpty());
  }

  @Test
  void testAncestorUnknown() {
    List<ExprValue> actualRows = execute(expand(inputPlan, DSL.ref("struct.array_nested", ARRAY)));
    assertTrue(actualRows.isEmpty());
  }

  @Test
  void testInvalidType() {
    mockInput(ExprValueUtils.tupleValue(Map.of("integer", integerValue)));

    Exception ex =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> execute(expand(inputPlan, DSL.ref("integer", INTEGER))));
    assertEquals("invalid to get collectionValue from value of type INTEGER", ex.getMessage());
  }

  /** Mocks the input plan to return a single row with the given input value. */
  private void mockInput(ExprValue mockInputValue) {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(mockInputValue);
  }
}
