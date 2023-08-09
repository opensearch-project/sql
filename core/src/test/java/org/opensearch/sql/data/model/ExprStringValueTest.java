/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;

public class ExprStringValueTest {
  @Test
  public void equals_to_self() {
    ExprValue string = ExprValueUtils.stringValue("str");
    assertEquals(string.stringValue(), "str");
    assertTrue(string.equals(string));
  }

  @Test
  public void equal() {
    ExprValue v1 = new ExprStringValue("str");
    ExprValue v2 = ExprValueUtils.stringValue("str");
    assertTrue(v1.equals(v2));
    assertTrue(v2.equals(v1));
    assertEquals(0, ((ExprStringValue) v1).compare((ExprStringValue) v2));
    assertEquals(0, ((ExprStringValue) v2).compare((ExprStringValue) v1));
  }

  @Test
  public void compare() {
    ExprStringValue v1 = new ExprStringValue("str1");
    ExprStringValue v2 = new ExprStringValue("str2");
    assertEquals(-1, v1.compare(v2));
    assertEquals(1, v2.compare(v1));
  }

  @Test
  public void invalid_get_value() {
    ExprDateValue value = new ExprDateValue("2020-08-20");
    assertThrows(
        ExpressionEvaluationException.class,
        value::stringValue,
        String.format("invalid to get intervalValue from value of type %s", value.type()));
  }

  @Test
  public void value() {
    ExprValue value = new ExprStringValue("string");
    assertEquals("string", value.value());
  }

  @Test
  public void type() {
    ExprValue value = new ExprStringValue("string");
    assertEquals(STRING, value.type());
  }
}
