/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.utils.ComparisonUtil.compare;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;

public class ExprNullValueTest {

  @Test
  public void test_is_null() {
    assertTrue(LITERAL_NULL.isNull());
  }

  @Test
  public void getValue() {
    assertNull(LITERAL_NULL.value());
  }

  @Test
  public void getType() {
    assertEquals(ExprCoreType.UNDEFINED, LITERAL_NULL.type());
  }

  @Test
  public void toStringTest() {
    assertEquals("NULL", LITERAL_NULL.toString());
  }

  @Test
  public void equal() {
    assertEquals(LITERAL_NULL, LITERAL_NULL);
    assertNotEquals(LITERAL_FALSE, LITERAL_NULL);
    assertNotEquals(LITERAL_NULL, LITERAL_FALSE);
  }

  @Test
  public void comparabilityTest() {
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class, () -> compare(LITERAL_NULL, LITERAL_NULL));
    assertEquals("invalid to call compare operation on null value", exception.getMessage());
  }
}
