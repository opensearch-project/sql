/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class RexExtractMultiFunctionTest {

  private final RexExtractMultiFunction function = new RexExtractMultiFunction();

  @Test
  public void testExtractMultipleGroups_BasicPattern() {
    String text = "user1@domain1.com, user2@domain2.com, user3@domain3.com";
    String pattern = "(\\w+)@(\\w+)";

    // Extract first group (usernames) with max 2 matches
    List<String> result = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 1, 2);
    assertEquals(Arrays.asList("user1", "user2"), result);

    // Extract second group (domains) with max 3 matches
    result = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 2, 3);
    assertEquals(Arrays.asList("domain1", "domain2", "domain3"), result);
  }

  @Test
  public void testExtractMultipleGroups_NamedGroups() {
    String text = "John Smith age:30, Jane Doe age:25, Bob Johnson age:35";
    String pattern = "(?<name>\\w+ \\w+) age:(?<age>\\d+)";

    // Extract first group (names) with unlimited matches (maxMatch = 0)
    List<String> names = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 1, 0);
    assertEquals(Arrays.asList("John Smith", "Jane Doe", "Bob Johnson"), names);

    // Extract second group (ages) with max 2 matches
    List<String> ages = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 2, 2);
    assertEquals(Arrays.asList("30", "25"), ages);
  }

  @Test
  public void testExtractMultipleGroups_SingleMatch() {
    String text = "Error: File not found at line 42";
    String pattern = "Error: (.+) at line (\\d+)";

    List<String> errorMsg = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 1, 1);
    assertEquals(Arrays.asList("File not found"), errorMsg);

    List<String> lineNum = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 2, 1);
    assertEquals(Arrays.asList("42"), lineNum);
  }

  @Test
  public void testExtractMultipleGroups_NoMatches() {
    String text = "This text has no matches";
    String pattern = "(\\d+)";

    List<String> result = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 1, 5);
    assertNull(result);
  }

  @Test
  public void testExtractMultipleGroups_InvalidGroupIndex() {
    String text = "abc123def456";
    String pattern = "(\\w+)(\\d+)";

    // Group index 3 doesn't exist (only groups 1 and 2)
    List<String> result = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 3, 5);
    assertNull(result);

    // Group index 0 is not valid
    result = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 0, 5);
    assertNull(result);
  }

  @Test
  public void testExtractMultipleGroups_InvalidPattern() {
    String text = "test string";
    String invalidPattern = "[unclosed"; // Invalid regex pattern

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              RexExtractMultiFunction.extractMultipleGroups(text, invalidPattern, 1, 5);
            });

    String expectedMessage =
        "Error in 'rex' command: Encountered the following error while compiling the regex"
            + " '[unclosed':";
    assertTrue(exception.getMessage().startsWith(expectedMessage));
  }

  @Test
  public void testExtractMultipleGroups_EmptyString() {
    String text = "";
    String pattern = "(\\w+)";

    List<String> result = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 1, 5);
    assertNull(result);
  }

  @Test
  public void testExtractMultipleGroups_MaxMatchZero() {
    String text = "a1 b2 c3 d4 e5";
    String pattern = "([a-z])(\\d)";

    // maxMatch = 0 should extract all matches
    List<String> letters = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 1, 0);
    assertEquals(Arrays.asList("a", "b", "c", "d", "e"), letters);

    List<String> numbers = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 2, 0);
    assertEquals(Arrays.asList("1", "2", "3", "4", "5"), numbers);
  }

  @Test
  public void testExtractMultipleGroups_MaxMatchOne() {
    String text = "test1 test2 test3";
    String pattern = "(test\\d)";

    List<String> result = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 1, 1);
    assertEquals(Arrays.asList("test1"), result);
  }

  @Test
  public void testExtractMultipleGroups_ComplexPattern() {
    String text = "IP: 192.168.1.1:8080, IP: 10.0.0.1:9090, IP: 172.16.0.1:3000";
    String pattern = "IP: (\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)";

    // Extract IP addresses
    List<String> ips = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 1, 0);
    assertEquals(Arrays.asList("192.168.1.1", "10.0.0.1", "172.16.0.1"), ips);

    // Extract ports with max 2 matches
    List<String> ports = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 2, 2);
    assertEquals(Arrays.asList("8080", "9090"), ports);
  }

  @Test
  public void testExtractMultipleGroups_NullMatchGroups() {
    String text = "word1 word2 word3";
    String pattern = "(\\w+)";

    // Extract words with max 3 matches
    List<String> result = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 1, 3);
    assertEquals(Arrays.asList("word1", "word2", "word3"), result);
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
    RexExtractMultiFunction testFunction = new RexExtractMultiFunction();
    assertNotNull(testFunction, "Function should be properly initialized");
  }

  @Test
  public void testExtractMultipleGroups_EdgeCaseEmptyMatches() {
    String text = "abc def ghi";
    String pattern = "(\\d*)";

    List<String> result = RexExtractMultiFunction.extractMultipleGroups(text, pattern, 1, 3);

    assertTrue(
        result == null || result.isEmpty() || result.size() <= 3,
        "Should handle empty matches gracefully");
  }
}
