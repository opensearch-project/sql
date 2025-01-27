/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.flatten;

import com.google.common.collect.ImmutableMap;
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
class FlattenOperatorTest extends PhysicalPlanTestBase {
  @Mock private PhysicalPlan inputPlan;

  @Test
  void testFlattenStruct() {
    Map<String, Object> structMap =
        ImmutableMap.of(
            "string_field",
            "string_value",
            "integer_field",
            1,
            "long_field",
            1L,
            "boolean_field",
            true,
            "list_field",
            List.of("a", "b"));

    Map<String, Object> rowMap = ImmutableMap.of("struct_field", structMap);
    ExprValue rowValue = ExprValueUtils.tupleValue(rowMap);

    ExprValue expectedRowValue = ExprValueUtils.tupleValue(structMap);

    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(rowValue);

    PhysicalPlan plan = flatten(inputPlan, DSL.ref("struct_field", STRUCT));

    assertThat(execute(plan), allOf(iterableWithSize(1), hasItems(expectedRowValue)));
  }

  @Test
  void testFlattenStructEmpty() {
    Map<String, Object> structMap = ImmutableMap.of();
    Map<String, Object> rowMap = ImmutableMap.of("struct_field", structMap);
    ExprValue rowValue = ExprValueUtils.tupleValue(rowMap);

    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(rowValue);

    PhysicalPlan plan = flatten(inputPlan, DSL.ref("struct_field", STRUCT));

    assertThat(execute(plan), allOf(iterableWithSize(1), hasItems()));
  }

  @Test
  void testFlattenStructNested() {
    Map<String, Object> structMap =
        ImmutableMap.of(
            "nested_struct_field", ImmutableMap.of("nested_string_field", "string_value"));
    Map<String, Object> rowMap = ImmutableMap.of("struct_field", structMap);
    ExprValue rowValue = ExprValueUtils.tupleValue(rowMap);

    Map<String, Object> expectedRowMap = ImmutableMap.of("nested_string_field", "string_value");
    ExprValue expectedRowValue = ExprValueUtils.tupleValue(expectedRowMap);

    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(rowValue);

    PhysicalPlan plan = flatten(inputPlan, DSL.ref("struct_field", STRUCT));

    assertThat(execute(plan), allOf(iterableWithSize(1), hasItems(expectedRowValue)));
  }
}
