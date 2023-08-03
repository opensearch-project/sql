/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;

public class ExprBooleanValueTest {

  @Test
  public void equals_to_self() {
    ExprValue value = ExprValueUtils.booleanValue(false);
    assertEquals(value.booleanValue(), false);
    assertTrue(value.equals(value));
  }

  @Test
  public void equal() {
    ExprValue v1 = ExprBooleanValue.of(true);
    ExprValue v2 = ExprBooleanValue.of(true);
    assertTrue(v1.equals(v2));
    assertTrue(v2.equals(v1));
    assertEquals(0, ((ExprBooleanValue) v1).compare((ExprBooleanValue) v2));
    assertEquals(0, ((ExprBooleanValue) v2).compare((ExprBooleanValue) v1));
  }

  @Test
  public void compare() {
    var v1 = ExprBooleanValue.of(true);
    var v2 = ExprBooleanValue.of(false);
    assertEquals(1, v1.compare(v2));
    assertEquals(-1, v2.compare(v1));
  }

  @Test
  public void invalid_get_value() {
    ExprDateValue value = new ExprDateValue("2020-08-20");
    assertThrows(
        ExpressionEvaluationException.class,
        value::booleanValue,
        String.format("invalid to get booleanValue from value of type %s", value.type()));
  }

  @Test
  public void value() {
    ExprValue value = ExprBooleanValue.of(true);
    assertEquals(true, value.value());
  }

  @Test
  public void type() {
    ExprValue value = ExprBooleanValue.of(false);
    assertEquals(BOOLEAN, value.type());
  }
}
