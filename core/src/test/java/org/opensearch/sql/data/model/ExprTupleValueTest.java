/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.utils.ComparisonUtil.compare;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;

class ExprTupleValueTest {
  @Test
  public void equal_to_itself() {
    ExprValue tupleValue = ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2));
    assertTrue(tupleValue.equals(tupleValue));
  }

  @Test
  public void tuple_compare_int() {
    ExprValue tupleValue = ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2));
    ExprValue intValue = ExprValueUtils.integerValue(10);
    assertFalse(tupleValue.equals(intValue));
  }

  @Test
  public void compare_tuple_with_different_key() {
    ExprValue tupleValue1 = ExprValueUtils.tupleValue(ImmutableMap.of("value", 2));
    ExprValue tupleValue2 =
        ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2, "float_value", 1f));
    assertNotEquals(tupleValue1, tupleValue2);
    assertNotEquals(tupleValue2, tupleValue1);
  }

  @Test
  public void compare_tuple_with_different_size() {
    ExprValue tupleValue1 = ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2));
    ExprValue tupleValue2 =
        ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2, "float_value", 1f));
    assertFalse(tupleValue1.equals(tupleValue2));
    assertFalse(tupleValue2.equals(tupleValue1));
  }

  @Test
  public void comparabilityTest() {
    ExprValue tupleValue = ExprValueUtils.tupleValue(ImmutableMap.of("integer_value", 2));
    ExpressionEvaluationException exception =
        assertThrows(ExpressionEvaluationException.class, () -> compare(tupleValue, tupleValue));
    assertEquals("ExprTupleValue instances are not comparable", exception.getMessage());
  }

  @Test
  public void testMergeTo() {
    ExprValue tupleValue1 =
        ExprValueUtils.tupleValue(
            ImmutableMap.of("v1", 1, "inner_tuple", ImmutableMap.of("inner_v1", 1)));
    ExprValue tupleValue2 =
        ExprValueUtils.tupleValue(
            ImmutableMap.of("v2", 2, "inner_tuple", ImmutableMap.of("inner_v2", 2)));
    ExprValue expectedMergedValue =
        ExprValueUtils.tupleValue(
            ImmutableMap.of(
                "v1", 1,
                "inner_tuple", ImmutableMap.of("inner_v1", 1, "inner_v2", 2),
                "v2", 2));
    assertEquals(expectedMergedValue, tupleValue1.mergeTo(tupleValue2));
  }
}
