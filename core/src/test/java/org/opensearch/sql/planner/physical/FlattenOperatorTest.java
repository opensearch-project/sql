/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
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
import org.opensearch.sql.expression.DSL;

@ToString
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class FlattenOperatorTest extends PhysicalPlanTestBase {
  @Mock private PhysicalPlan inputPlan;

  @BeforeEach
  void setup() {
    ExprValue rowValue =
        ExprValueUtils.tupleValue(
            Map.ofEntries(
                Map.entry("struct_empty", Map.of()),
                Map.entry(
                    "struct",
                    Map.ofEntries(
                        Map.entry("integer", 0),
                        Map.entry("double", 0),
                        Map.entry("struct_nested", Map.of("string", "value"))))));

    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(rowValue);
  }

  @Test
  void testStructEmpty() {
    ExprValue actual = execute(flatten(inputPlan, DSL.ref("struct_empty", STRUCT))).getFirst();

    ExprValue expected =
        ExprValueUtils.tupleValue(
            Map.ofEntries(
                Map.entry(
                    "struct",
                    Map.ofEntries(
                        Map.entry("integer", 0),
                        Map.entry("double", 0),
                        Map.entry("struct_nested", Map.of("string", "value"))))));

    assertEquals(expected, actual);
  }

  @Test
  void testStruct() {
    ExprValue actual = execute(flatten(inputPlan, DSL.ref("struct", STRUCT))).getFirst();

    ExprValue expected =
        ExprValueUtils.tupleValue(
            Map.ofEntries(
                Map.entry("struct_empty", Map.of()),
                Map.entry("integer", 0),
                Map.entry("double", 0),
                Map.entry("struct_nested", Map.of("string", "value"))));

    assertEquals(expected, actual);
  }

  @Test
  void testStructNested() {
    ExprValue actual =
        execute(flatten(inputPlan, DSL.ref("struct.struct_nested", STRUCT))).getFirst();

    ExprValue expected =
        ExprValueUtils.tupleValue(
            Map.ofEntries(
                Map.entry("struct_empty", Map.of()),
                Map.entry(
                    "struct",
                    Map.ofEntries(
                        Map.entry("integer", 0),
                        Map.entry("double", 0),
                        Map.entry("string", "value")))));

    assertEquals(expected, actual);
  }
}
