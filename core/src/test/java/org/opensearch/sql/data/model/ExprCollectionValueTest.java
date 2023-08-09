/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.utils.ComparisonUtil.compare;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;

public class ExprCollectionValueTest {
  @Test
  public void equal_to_itself() {
    ExprValue value = ExprValueUtils.collectionValue(ImmutableList.of(1));
    assertTrue(value.equals(value));
  }

  @Test
  public void collection_compare_int() {
    ExprValue intValue = ExprValueUtils.integerValue(10);
    ExprValue value = ExprValueUtils.collectionValue(ImmutableList.of(1));
    assertFalse(value.equals(intValue));
  }

  @Test
  public void compare_collection_with_different_size() {
    ExprValue value1 = ExprValueUtils.collectionValue(ImmutableList.of(1));
    ExprValue value2 = ExprValueUtils.collectionValue(ImmutableList.of(1, 2));
    assertFalse(value1.equals(value2));
    assertFalse(value2.equals(value1));
  }

  @Test
  public void compare_collection_with_int_object() {
    ExprValue value = ExprValueUtils.collectionValue(ImmutableList.of(1));
    assertFalse(value.equals(1));
  }

  @Test
  public void comparabilityTest() {
    ExprValue collectionValue = ExprValueUtils.collectionValue(Arrays.asList(0, 1));
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class, () -> compare(collectionValue, collectionValue));
    assertEquals("ExprCollectionValue instances are not comparable", exception.getMessage());
  }
}
