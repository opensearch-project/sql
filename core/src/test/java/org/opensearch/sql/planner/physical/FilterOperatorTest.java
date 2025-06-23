/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.planner.physical.PhysicalPlanDSL.filter;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class FilterOperatorTest extends PhysicalPlanTestBase {
  @Mock private PhysicalPlan inputPlan;

  @Mock private Expression condition;

  private FilterOperator filterOperator;

  @BeforeEach
  public void setup() {
    filterOperator = filter(inputPlan, condition);
  }

  @Test
  public void filter_test() {
    FilterOperator plan =
        new FilterOperator(
            new TestScan(),
            DSL.and(
                DSL.notequal(DSL.ref("response", INTEGER), DSL.literal(200)),
                DSL.notequal(DSL.ref("response", INTEGER), DSL.literal(500))));
    List<ExprValue> result = execute(plan);
    assertEquals(1, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(
                ImmutableMap.of(
                    "ip",
                    "209.160.24.63",
                    "action",
                    "GET",
                    "response",
                    404,
                    "referer",
                    "www.amazon.com"))));
  }

  @Test
  public void null_value_should_been_ignored() {
    LinkedHashMap<String, ExprValue> value = new LinkedHashMap<>();
    value.put("response", LITERAL_NULL);
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(new ExprTupleValue(value));

    FilterOperator plan =
        new FilterOperator(inputPlan, DSL.equal(DSL.ref("response", INTEGER), DSL.literal(404)));
    List<ExprValue> result = execute(plan);
    assertEquals(0, result.size());
  }

  @Test
  public void missing_value_should_been_ignored() {
    LinkedHashMap<String, ExprValue> value = new LinkedHashMap<>();
    value.put("response", LITERAL_MISSING);
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(new ExprTupleValue(value));

    FilterOperator plan =
        new FilterOperator(inputPlan, DSL.equal(DSL.ref("response", INTEGER), DSL.literal(404)));
    List<ExprValue> result = execute(plan);
    assertEquals(0, result.size());
  }

  @Test
  public void testHasNextWhenInputHasNoElements() {
    when(inputPlan.hasNext()).thenReturn(false);

    assertFalse(
        filterOperator.hasNext(), "hasNext() should return false when input has no elements");
  }

  @Test
  public void testHasNextWithMatchingCondition() {
    ExprValue inputValue = mock(ExprValue.class);
    when(inputPlan.hasNext()).thenReturn(true).thenReturn(false);
    when(inputPlan.next()).thenReturn(inputValue);
    when(condition.valueOf(any())).thenReturn(LITERAL_TRUE);

    assertTrue(filterOperator.hasNext(), "hasNext() should return true when condition matches");
    assertEquals(
        inputValue, filterOperator.next(), "next() should return the matching input value");
  }

  @Test
  public void testHasNextWithNonMatchingCondition() {
    ExprValue inputValue = mock(ExprValue.class);
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(inputValue);
    when(condition.valueOf(any())).thenReturn(LITERAL_FALSE);

    assertFalse(
        filterOperator.hasNext(), "hasNext() should return false if no values match the condition");
  }

  @Test
  public void testMultipleCallsToHasNextDoNotConsumeInput() {
    ExprValue inputValue = mock(ExprValue.class);
    when(inputPlan.hasNext()).thenReturn(true);
    when(inputPlan.next()).thenReturn(inputValue);
    when(condition.valueOf(any())).thenReturn(LITERAL_TRUE);

    assertTrue(
        filterOperator.hasNext(),
        "First hasNext() call should return true if there is a matching value");
    verify(inputPlan, times(1)).next();
    assertTrue(
        filterOperator.hasNext(),
        "Subsequent hasNext() calls should still return true without advancing the input");
    verify(inputPlan, times(1)).next();
    assertEquals(
        inputValue, filterOperator.next(), "next() should return the matching input value");
    verify(inputPlan, times(1)).next();
  }

  @Test
  public void testNextWithoutCallingHasNext() {
    ExprValue inputValue = mock(ExprValue.class);
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(inputValue);
    when(condition.valueOf(any())).thenReturn(LITERAL_TRUE);

    assertEquals(
        inputValue,
        filterOperator.next(),
        "next() should return the matching input value even if hasNext() was not called");
  }
}
