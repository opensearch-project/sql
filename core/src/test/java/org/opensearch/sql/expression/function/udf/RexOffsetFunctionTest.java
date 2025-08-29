/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class RexOffsetFunctionTest {

  private final RexOffsetFunction function = new RexOffsetFunction();

  @Test
  public void testCalculateOffsets_SimpleExample() {
    String text = "user@domain.com";
    String pattern = "(?<username>\\w+)@(?<domain>\\w+)\\.(?<tld>\\w+)";

    String result = RexOffsetFunction.calculateOffsets(text, pattern);
    // user=0-3, domain=5-10, com=12-14 (but order is reversed)
    assertEquals("tld=12-14&domain=5-10&username=0-3", result);
  }

  @Test
  public void testCalculateOffsets_SingleNamedGroup() {
    String text = "hello world";
    String pattern = "(?<word>\\w+)";

    String result = RexOffsetFunction.calculateOffsets(text, pattern);
    assertEquals("word=0-4", result);
  }

  @Test
  public void testCalculateOffsets_TwoGroups() {
    String text = "abc123";
    String pattern = "(?<letters>[a-z]+)(?<numbers>\\d+)";

    String result = RexOffsetFunction.calculateOffsets(text, pattern);
    assertEquals("numbers=3-5&letters=0-2", result);
  }

  @Test
  public void testCalculateOffsets_NoMatches() {
    String text = "This text has no digits";
    String pattern = "(?<digit>\\d+)";

    String result = RexOffsetFunction.calculateOffsets(text, pattern);
    assertNull(result);
  }

  @Test
  public void testCalculateOffsets_NullInputs() {
    // Null text
    String result = RexOffsetFunction.calculateOffsets(null, "(?<test>\\w+)");
    assertNull(result);

    // Null pattern
    result = RexOffsetFunction.calculateOffsets("test text", null);
    assertNull(result);

    // Both null
    result = RexOffsetFunction.calculateOffsets(null, null);
    assertNull(result);
  }

  @Test
  public void testCalculateOffsets_InvalidPattern() {
    String text = "test string";
    String invalidPattern = "[unclosed";

    String result = RexOffsetFunction.calculateOffsets(text, invalidPattern);
    assertNull(result);
  }

  @Test
  public void testCalculateOffsets_EmptyString() {
    String text = "";
    String pattern = "(?<word>\\w+)";

    String result = RexOffsetFunction.calculateOffsets(text, pattern);
    assertNull(result);
  }

  @Test
  public void testCalculateOffsets_PatternWithoutNamedGroups() {
    String text = "test123";
    String pattern = "(\\w+)(\\d+)";

    String result = RexOffsetFunction.calculateOffsets(text, pattern);
    assertNull(result);
  }

  @Test
  public void testCalculateOffsets_SingleCharacterMatch() {
    String text = "a";
    String pattern = "(?<char>[a-z])";

    String result = RexOffsetFunction.calculateOffsets(text, pattern);
    assertEquals("char=0-0", result);
  }

  @Test
  public void testCalculateOffsets_DigitsPattern() {
    String text = "year 2023 month 12";
    String pattern = "(?<year>\\d{4}).*(?<month>\\d{2})";

    String result = RexOffsetFunction.calculateOffsets(text, pattern);
    assertEquals("month=16-17&year=5-8", result);
  }

  @Test
  public void testCalculateOffsets_EmailExample() {
    String text = "email: john@example.org";
    String pattern = "(?<name>\\w+)@(?<domain>\\w+)\\.(?<ext>\\w+)";

    String result = RexOffsetFunction.calculateOffsets(text, pattern);
    assertEquals("ext=20-22&domain=12-18&name=7-10", result);
  }

  @Test
  public void testReturnTypeInference() {
    assertNotNull(function.getReturnTypeInference(), "Return type inference should not be null");
  }

  @Test
  public void testOperandMetadata() {
    assertNotNull(function.getOperandMetadata(), "Operand metadata should not be null");
  }

  @Test
  public void testFunctionConstructor() {
    RexOffsetFunction testFunction = new RexOffsetFunction();
    assertNotNull(testFunction, "Function should be properly initialized");
  }
}
