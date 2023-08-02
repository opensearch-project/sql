/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;

public class ExprNumberValueTest {
  @Test
  public void getShortValueFromIncompatibleExprValue() {
    ExprBooleanValue booleanValue = ExprBooleanValue.of(true);
    ExpressionEvaluationException exception =
        Assertions.assertThrows(
            ExpressionEvaluationException.class, () -> booleanValue.shortValue());
    assertEquals("invalid to get shortValue from value of type BOOLEAN", exception.getMessage());
  }

  @Test
  public void key_value() {
    assertTrue(new ExprIntegerValue(1).keyValue("path").isMissing());
  }
}
