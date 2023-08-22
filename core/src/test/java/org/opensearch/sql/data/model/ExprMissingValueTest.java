/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.utils.ComparisonUtil.compare;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;

class ExprMissingValueTest {

  @Test
  public void test_is_missing() {
    assertTrue(LITERAL_MISSING.isMissing());
  }

  @Test
  public void getValue() {
    assertNull(LITERAL_MISSING.value());
  }

  @Test
  public void getType() {
    assertEquals(ExprCoreType.UNDEFINED, LITERAL_MISSING.type());
  }

  @Test
  public void toStringTest() {
    assertEquals("MISSING", LITERAL_MISSING.toString());
  }

  @Test
  public void equal() {
    assertTrue(LITERAL_MISSING.equals(LITERAL_MISSING));
    assertFalse(LITERAL_FALSE.equals(LITERAL_MISSING));
    assertFalse(LITERAL_MISSING.equals(LITERAL_FALSE));
  }

  @Test
  public void comparabilityTest() {
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class, () -> compare(LITERAL_MISSING, LITERAL_MISSING));
    assertEquals("invalid to call compare operation on missing value", exception.getMessage());
  }
}
