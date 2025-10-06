/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class RexExtractFunctionTest {

  private final RexExtractFunction function = new RexExtractFunction();

  @Test
  public void testExtractGroup_BasicPattern() {
    String text = "user@domain.com";
    String pattern = "([^@]+)@([^.]+)\\.(.+)";

    // Extract username (group 1)
    String result = RexExtractFunction.extractGroup(text, pattern, 1);
    assertEquals("user", result);

    // Extract domain (group 2)
    result = RexExtractFunction.extractGroup(text, pattern, 2);
    assertEquals("domain", result);

    // Extract TLD (group 3)
    result = RexExtractFunction.extractGroup(text, pattern, 3);
    assertEquals("com", result);
  }

  @Test
  public void testExtractGroup_NamedGroups() {
    String text = "John Smith age:30";
    String pattern = "(?<name>\\w+ \\w+) age:(?<age>\\d+)";

    // Extract name (group 1)
    String name = RexExtractFunction.extractGroup(text, pattern, 1);
    assertEquals("John Smith", name);

    // Extract age (group 2)
    String age = RexExtractFunction.extractGroup(text, pattern, 2);
    assertEquals("30", age);
  }

  @Test
  public void testExtractGroup_NoMatch() {
    String text = "This text has no numbers";
    String pattern = "(\\d+)";

    String result = RexExtractFunction.extractGroup(text, pattern, 1);
    assertNull(result);
  }

  @Test
  public void testExtractGroup_InvalidGroupIndex() {
    String text = "abc123";
    String pattern = "(\\w+)(\\d+)";

    // Group index 3 doesn't exist (only groups 1 and 2)
    String result = RexExtractFunction.extractGroup(text, pattern, 3);
    assertNull(result);

    // Group index 0 is not valid
    result = RexExtractFunction.extractGroup(text, pattern, 0);
    assertNull(result);

    // Negative group index
    result = RexExtractFunction.extractGroup(text, pattern, -1);
    assertNull(result);
  }

  @Test
  public void testExtractGroup_InvalidPattern() {
    String text = "test string";
    String invalidPattern = "(?<invalid>["; // Unclosed bracket

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              RexExtractFunction.extractGroup(text, invalidPattern, 1);
            });

    String expectedMessage =
        "Error in 'rex' command: Encountered the following error while compiling the regex"
            + " '(?<invalid>[':";
    assertEquals(true, exception.getMessage().startsWith(expectedMessage));
  }

  @Test
  public void testExtractGroup_EmptyString() {
    String text = "";
    String pattern = "(\\w+)";

    String result = RexExtractFunction.extractGroup(text, pattern, 1);
    assertNull(result);
  }

  @Test
  public void testExtractGroup_SingleCharacter() {
    String text = "a";
    String pattern = "([a-z])";

    String result = RexExtractFunction.extractGroup(text, pattern, 1);
    assertEquals("a", result);
  }

  @Test
  public void testExtractGroup_ComplexPattern() {
    String text = "Error: File not found at line 42 in script.py";
    String pattern = "Error: (.+) at line (\\d+) in ([^\\s]+)";

    String errorMsg = RexExtractFunction.extractGroup(text, pattern, 1);
    assertEquals("File not found", errorMsg);

    String lineNum = RexExtractFunction.extractGroup(text, pattern, 2);
    assertEquals("42", lineNum);

    String filename = RexExtractFunction.extractGroup(text, pattern, 3);
    assertEquals("script.py", filename);
  }

  @Test
  public void testExtractGroup_EmailPattern() {
    String text = "Contact: john.doe@example.org for support";
    String pattern = "([\\w.]+)@([\\w.]+)\\.([a-z]+)";

    String username = RexExtractFunction.extractGroup(text, pattern, 1);
    assertEquals("john.doe", username);

    String domain = RexExtractFunction.extractGroup(text, pattern, 2);
    assertEquals("example", domain);

    String tld = RexExtractFunction.extractGroup(text, pattern, 3);
    assertEquals("org", tld);
  }

  @Test
  public void testExtractGroup_IPAddressPattern() {
    String text = "Server IP: 192.168.1.100:8080";
    String pattern = "(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+):(\\d+)";

    String firstOctet = RexExtractFunction.extractGroup(text, pattern, 1);
    assertEquals("192", firstOctet);

    String port = RexExtractFunction.extractGroup(text, pattern, 5);
    assertEquals("8080", port);
  }

  @Test
  public void testExtractGroup_MultiplePatternSyntaxErrors() {
    String text = "test";

    // Test various invalid patterns
    String[] invalidPatterns = {
      "[unclosed",
      "(?<invalid>",
      "*+invalid",
      "(?P<python_style>\\w+)", // Python-style named groups not supported in Java
      "\\k<invalid>"
    };

    for (String invalidPattern : invalidPatterns) {
      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> {
                RexExtractFunction.extractGroup(text, invalidPattern, 1);
              });

      String expectedPrefix =
          "Error in 'rex' command: Encountered the following error while compiling the regex";
      assertEquals(
          true,
          exception.getMessage().startsWith(expectedPrefix),
          "Error message should start with SPL-style prefix for pattern: " + invalidPattern);
    }
  }

  @Test
  public void testExtractGroup_CaseSensitivity() {
    String text = "Hello World";
    String pattern = "(hello)";

    // Should not match due to case sensitivity
    String result = RexExtractFunction.extractGroup(text, pattern, 1);
    assertNull(result);

    // Case-insensitive flag in pattern
    String caseInsensitivePattern = "(?i)(hello)";
    result = RexExtractFunction.extractGroup(text, caseInsensitivePattern, 1);
    assertEquals("Hello", result);
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
    RexExtractFunction testFunction = new RexExtractFunction();
    assertNotNull(testFunction, "Function should be properly initialized");
  }
}
