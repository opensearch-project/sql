/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

public class LimitedCharSequenceTest {

  @Test
  public void delegatesLengthAndCharAtAndToString() {
    LimitedCharSequence seq = new LimitedCharSequence("abcde", null, 10);
    assertEquals(5, seq.length());
    assertEquals('a', seq.charAt(0));
    assertEquals('e', seq.charAt(4));
    assertEquals("abcde", seq.toString());
    assertEquals("cd", seq.subSequence(2, 4).toString());
  }

  @Test
  public void rejectsNonPositiveLimitFactor() {
    assertThrows(IllegalArgumentException.class, () -> new LimitedCharSequence("abc", null, 0));
    assertThrows(IllegalArgumentException.class, () -> new LimitedCharSequence("abc", null, -1));
  }

  @Test
  public void throwsAfterExceedingCharAtBudget() {
    // limitFactor 2 * length 3 = budget of 6 charAt calls; the 7th must throw.
    LimitedCharSequence seq = new LimitedCharSequence("abc", null, 2);
    for (int i = 0; i < 6; i++) {
      seq.charAt(0); // 6 reads are allowed (count > limit is the trip condition)
    }
    assertThrows(IllegalArgumentException.class, () -> seq.charAt(0));
  }

  @Test
  public void errorMessageIncludesPatternAndCounts() {
    Pattern pattern = Pattern.compile("(a+)+");
    LimitedCharSequence seq = new LimitedCharSequence("ab", pattern, 1);
    // budget = 1 * 2 = 2; third read trips.
    seq.charAt(0);
    seq.charAt(0);
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> seq.charAt(0));
    assertTrue(e.getMessage().contains("too many characters"));
    assertTrue(e.getMessage().contains("(a+)+"), "should include the offending pattern");
    assertTrue(e.getMessage().contains("limit factor: [1]"));
  }

  @Test
  public void snippetTruncatesLongInput() {
    String longInput = "x".repeat(200);
    LimitedCharSequence seq = new LimitedCharSequence(longInput, null, 10);
    String snippet = seq.snippet(LimitedCharSequence.MAX_STR_LENGTH);
    assertTrue(snippet.length() <= LimitedCharSequence.MAX_STR_LENGTH);
    assertTrue(snippet.endsWith("..."));
  }

  @Test
  public void snippetReturnsFullShortInput() {
    LimitedCharSequence seq = new LimitedCharSequence("short", null, 10);
    assertEquals("short", seq.snippet(LimitedCharSequence.MAX_STR_LENGTH));
  }
}
