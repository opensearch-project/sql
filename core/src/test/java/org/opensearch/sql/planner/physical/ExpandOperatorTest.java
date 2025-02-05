/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
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
import org.opensearch.sql.expression.DSL;

@ToString
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class ExpandOperatorTest extends PhysicalPlanTestBase {

  // Test constants
  private static final Integer integerValue = 0;
  private static final Double doubleValue = 0.0;
  private static final String stringValue = "value";

  private static final ExprValue nullExprValue = ExprValueUtils.nullValue();
  private static final ExprValue missingExprValue = ExprValueUtils.missingValue();

  // Test variables
  @Mock private PhysicalPlan inputPlan;
  private ExprValue inputRow;
  private List<ExprValue> actualRows;
  private List<ExprValue> expectedRows;

  @Test
  void testArray() {
    inputRow =
        ExprValueUtils.tupleValue(
            Map.of("array", ExprValueUtils.collectionValue(List.of(integerValue, doubleValue))));
    mockInput(inputRow);

    actualRows = execute(expand(inputPlan, DSL.ref("array", ARRAY)));
    expectedRows =
        List.of(
            ExprValueUtils.tupleValue(Map.of("array", integerValue)),
            ExprValueUtils.tupleValue(Map.of("array", doubleValue)));

    assertEquals(expectedRows, actualRows);
  }

  @Test
  void testArrayEmpty() {
    ExprValue inputRow =
        ExprValueUtils.tupleValue(Map.of("array_empty", ExprValueUtils.collectionValue(List.of())));
    mockInput(inputRow);

    actualRows = execute(expand(inputPlan, DSL.ref("array_empty", ARRAY)));
    expectedRows = List.of(ExprValueUtils.tupleValue(Map.of("array_empty", nullExprValue)));

    assertEquals(expectedRows, actualRows);
  }

  @Test
  void testArrayNested() {
    ExprValue inputRow =
        ExprValueUtils.tupleValue(
            Map.of(
                "struct",
                ExprValueUtils.tupleValue(
                    Map.of("array", ExprValueUtils.collectionValue(List.of(stringValue))))));
    mockInput(inputRow);

    actualRows = execute(expand(inputPlan, DSL.ref("struct.array", ARRAY)));
    expectedRows =
        List.of(
            ExprValueUtils.tupleValue(
                Map.of("struct", ExprValueUtils.tupleValue(Map.of("array", stringValue)))));

    assertEquals(expectedRows, actualRows);
  }

  @Test
  void testScalar() {
    ExprValue inputValue = ExprValueUtils.tupleValue(Map.of("scalar", stringValue));
    mockInput(inputValue);

    actualRows = execute(expand(inputPlan, DSL.ref("scalar", ARRAY)));
    expectedRows = List.of(inputValue);

    assertEquals(expectedRows, actualRows);
  }

  @Test
  void testScalarNull() {
    ExprValue inputRow = ExprValueUtils.tupleValue(Map.of("scalar_null", nullExprValue));
    mockInput(inputRow);

    actualRows = execute(expand(inputPlan, DSL.ref("scalar_null", ARRAY)));
    expectedRows = List.of(inputRow);

    assertEquals(expectedRows, actualRows);
  }

  @Test
  void testScalarMissing() {

    /** With {@link org.opensearch.sql.data.model.ExprMissingValue} */
    inputRow = ExprValueUtils.tupleValue(Map.of());
    mockInput(inputRow);

    actualRows = execute(expand(inputPlan, DSL.ref("scalar_missing", ARRAY)));
    expectedRows = List.of(inputRow);

    assertEquals(expectedRows, actualRows);

    /** Without {@link org.opensearch.sql.data.model.ExprMissingValue} */
    inputRow = ExprValueUtils.tupleValue(Map.of("scalar_missing", missingExprValue));
    mockInput(inputRow);

    actualRows = execute(expand(inputPlan, DSL.ref("scalar_missing", ARRAY)));
    expectedRows = List.of(inputRow);

    assertEquals(expectedRows, actualRows);
  }

  @Test
  void testScalarNested() {
    ExprValue rowInput =
        ExprValueUtils.tupleValue(
            Map.of("struct", ExprValueUtils.tupleValue(Map.of("scalar", stringValue))));
    mockInput(rowInput);

    actualRows = execute(expand(inputPlan, DSL.ref("struct.scalar", ARRAY)));
    expectedRows =
        List.of(
            ExprValueUtils.tupleValue(
                Map.of("struct", ExprValueUtils.tupleValue(Map.of("scalar", stringValue)))));

    assertEquals(expectedRows, actualRows);
  }

  @Test
  void testPathUnknown() {
    actualRows = execute(expand(inputPlan, DSL.ref("unknown", ARRAY)));
    expectedRows = List.of();

    assertEquals(expectedRows, actualRows);
  }

  @Test
  void testAncestorNull() {
    ExprValue rowInput = ExprValueUtils.tupleValue(Map.of("struct_null", nullExprValue));
    mockInput(rowInput);

    actualRows = execute(expand(inputPlan, DSL.ref("struct_null.unreachable", ARRAY)));
    expectedRows = List.of(rowInput);

    assertEquals(expectedRows, actualRows);
  }

  @Test
  void testAncestorMissing() {

    /** With {@link org.opensearch.sql.data.model.ExprMissingValue} */
    inputRow = ExprValueUtils.tupleValue(Map.of());
    mockInput(inputRow);

    actualRows = execute(expand(inputPlan, DSL.ref("struct_missing.unreachable", ARRAY)));
    expectedRows = List.of(inputRow);

    assertEquals(expectedRows, actualRows);

    /** Without {@link org.opensearch.sql.data.model.ExprMissingValue} */
    inputRow = ExprValueUtils.tupleValue(Map.of("struct_missing", missingExprValue));
    mockInput(inputRow);

    actualRows = execute(expand(inputPlan, DSL.ref("struct_missing.unreachable", ARRAY)));
    expectedRows = List.of(inputRow);

    assertEquals(expectedRows, actualRows);
  }

  @Test
  void testAncestorUnknown() {
    actualRows = execute(expand(inputPlan, DSL.ref("unknown.unreachable", ARRAY)));
    assertTrue(actualRows.isEmpty());
  }

  /** Mocks the input plan to return a single row with the given input value. */
  private void mockInput(ExprValue mockInputValue) {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(mockInputValue);
  }
}
