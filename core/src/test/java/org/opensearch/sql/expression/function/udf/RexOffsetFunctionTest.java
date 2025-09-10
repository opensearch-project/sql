/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.PatternSyntaxException;
import org.junit.jupiter.api.Test;

public class RexOffsetFunctionTest {

  @Test
  public void testCalculateOffsetsWithSingleNamedGroup() {
    String text = "SMITH";
    String pattern = "(?<first>[A-Z])";
    String result = RexOffsetFunction.calculateOffsets(text, pattern);
    assertEquals("first=0-0", result);
  }

  @Test
  public void testCalculateOffsetsWithMultipleNamedGroups() {
    String text = "SMITH";
    String pattern = "(?<first>[A-Z])(?<rest>[A-Z]+)";
    String result = RexOffsetFunction.calculateOffsets(text, pattern);
    assertEquals("rest=1-4&first=0-0", result);
  }

  @Test
  public void testCalculateOffsetsWithNoMatch() {
    String text = "smith";
    String pattern = "(?<first>[0-9])";
    String result = RexOffsetFunction.calculateOffsets(text, pattern);
    assertNull(result);
  }

  @Test
  public void testCalculateOffsetsWithNullInput() {
    String result1 = RexOffsetFunction.calculateOffsets(null, "(?<test>.*)");
    assertNull(result1);

    String result2 = RexOffsetFunction.calculateOffsets("test", null);
    assertNull(result2);

    String result3 = RexOffsetFunction.calculateOffsets(null, null);
    assertNull(result3);
  }

  @Test
  public void testCalculateOffsetsWithInvalidPattern() {
    String text = "test";
    String pattern = "(?<invalid[";

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> RexOffsetFunction.calculateOffsets(text, pattern));

    assertTrue(exception.getMessage().contains("Invalid regex pattern in rex command"));
    assertTrue(exception.getCause() instanceof PatternSyntaxException);
  }

  @Test
  public void testCalculateOffsetsWithComplexPattern() {
    String text = "ABC123DEF";
    String pattern = "(?<letters>[A-Z]+)(?<numbers>[0-9]+)(?<moreletters>[A-Z]+)";
    String result = RexOffsetFunction.calculateOffsets(text, pattern);
    assertEquals("moreletters=6-8&numbers=3-5&letters=0-2", result);
  }

  @Test
  public void testCalculateOffsetsWithEmptyMatch() {
    String text = "test";
    String pattern = "(?<empty>)test";
    String result = RexOffsetFunction.calculateOffsets(text, pattern);
    // Empty groups should have start == end but end-1 might be negative, so handle appropriately
    assertEquals("empty=0--1", result);
  }
}
