/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;

public class ExprStringValueTest {
  @Test
  public void equals_to_self() {
    ExprValue string = ExprValueUtils.stringValue("str");
    assertEquals("str", string.stringValue());
    assertEquals(string, string);
  }

  @Test
  public void equal() {
    ExprStringValue v1 = new ExprStringValue("str");
    ExprValue v2 = ExprValueUtils.stringValue("str");
    assertEquals(v1, v2);
    assertEquals(v2, v1);
    assertEquals(0, v1.compare(v2));
    assertEquals(0, ((ExprStringValue) v2).compare(v1));
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
