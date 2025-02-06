/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import lombok.ToString;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;

@ToString
class PathUtilsTest {

  // Test values
  private final ExprValue value = ExprValueUtils.integerValue(0);
  private final ExprValue newValue = ExprValueUtils.stringValue("value");
  private final ExprValue nullValue = ExprValueUtils.nullValue();
  private final ExprValue missingValue = ExprValueUtils.missingValue();

  private final ExprValue struct1Value = ExprValueUtils.tupleValue(Map.of("field", value));
  private final ExprValue struct2Value =
      ExprValueUtils.tupleValue(
          Map.of("struct1", ExprValueUtils.tupleValue(Map.of("field", value))));

  private final ExprValue input =
      ExprValueUtils.tupleValue(
          Map.ofEntries(
              Map.entry("field", value),
              Map.entry("struct1", struct1Value),
              Map.entry("struct2", struct2Value),
              Map.entry("struct_null", nullValue),
              Map.entry("struct_missing", missingValue)));

  @Test
  void testContainsExprValueForPath() {
    assertTrue(PathUtils.containsExprValueAtPath(input, "field"));
    assertTrue(PathUtils.containsExprValueAtPath(input, "struct1.field"));
    assertTrue(PathUtils.containsExprValueAtPath(input, "struct2.struct1.field"));

    assertFalse(PathUtils.containsExprValueAtPath(input, "field_invalid"));
    assertFalse(PathUtils.containsExprValueAtPath(input, "struct_null.field_invalid"));
    assertFalse(PathUtils.containsExprValueAtPath(input, "struct_missing.field_invalid"));
    assertFalse(PathUtils.containsExprValueAtPath(input, "struct_invalid.field_invalid"));
  }

  @Test
  void testGetExprValueForPath() {
    assertEquals(value, PathUtils.getExprValueAtPath(input, "field"));
    assertEquals(value, PathUtils.getExprValueAtPath(input, "struct1.field"));
    assertEquals(value, PathUtils.getExprValueAtPath(input, "struct2.struct1.field"));

    assertNull(PathUtils.getExprValueAtPath(input, "field_invalid"));
    assertNull(PathUtils.getExprValueAtPath(input, "struct_null.field_invalid"));
    assertNull(PathUtils.getExprValueAtPath(input, "struct_missing.field_invalid"));
    assertNull(PathUtils.getExprValueAtPath(input, "struct_invalid.field_invalid"));
  }

  @Test
  void testSetExprValueForPath() {
    ExprValue expected;
    ExprValue actual;

    expected =
        ExprValueUtils.tupleValue(
            Map.ofEntries(
                Map.entry("field", newValue),
                Map.entry("struct1", struct1Value),
                Map.entry("struct2", struct2Value),
                Map.entry("struct_null", nullValue),
                Map.entry("struct_missing", missingValue)));
    actual = PathUtils.setExprValueAtPath(input, "field", newValue);
    assertEquals(expected, actual);

    expected =
        ExprValueUtils.tupleValue(
            Map.ofEntries(
                Map.entry("field", value),
                Map.entry("struct1", ExprValueUtils.tupleValue(Map.of("field", newValue))),
                Map.entry("struct2", struct2Value),
                Map.entry("struct_null", nullValue),
                Map.entry("struct_missing", missingValue)));
    actual = PathUtils.setExprValueAtPath(input, "struct1.field", newValue);
    assertEquals(expected, actual);

    expected =
        ExprValueUtils.tupleValue(
            Map.ofEntries(
                Map.entry("field", value),
                Map.entry("struct1", struct1Value),
                Map.entry(
                    "struct2",
                    ExprValueUtils.tupleValue(
                        Map.of("struct1", ExprValueUtils.tupleValue(Map.of("field", newValue))))),
                Map.entry("struct_null", nullValue),
                Map.entry("struct_missing", missingValue)));
    assertEquals(expected, PathUtils.setExprValueAtPath(input, "struct2.struct1.field", newValue));

    Exception ex;

    ex =
        assertThrows(
            SemanticCheckException.class,
            () -> PathUtils.setExprValueAtPath(input, "field_invalid", newValue));
    assertEquals("Field path 'field_invalid' does not exist.", ex.getMessage());

    ex =
        assertThrows(
            SemanticCheckException.class,
            () -> PathUtils.setExprValueAtPath(input, "struct_null.field_invalid", newValue));
    assertEquals("Field path 'struct_null.field_invalid' does not exist.", ex.getMessage());

    ex =
        assertThrows(
            SemanticCheckException.class,
            () -> PathUtils.setExprValueAtPath(input, "struct_missing.field_invalid", newValue));
    assertEquals("Field path 'struct_missing.field_invalid' does not exist.", ex.getMessage());

    ex =
        assertThrows(
            SemanticCheckException.class,
            () -> PathUtils.setExprValueAtPath(input, "struct_invalid.field_invalid", newValue));
    assertEquals("Field path 'struct_invalid.field_invalid' does not exist.", ex.getMessage());
  }
}
