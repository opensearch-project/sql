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
    LimitedCharSequence seq = new LimitedCharSequence("abcde", null, 100);
    assertEquals(5, seq.length());
    assertEquals('a', seq.charAt(0));
    assertEquals('e', seq.charAt(4));
    assertEquals("abcde", seq.toString());
    assertEquals("cd", seq.subSequence(2, 4).toString());
  }

  @Test
  public void rejectsNonPositiveMatchLimit() {
    assertThrows(IllegalArgumentException.class, () -> new LimitedCharSequence("abc", null, 0));
    assertThrows(IllegalArgumentException.class, () -> new LimitedCharSequence("abc", null, -1));
  }

  @Test
  public void throwsAfterExceedingAbsoluteCap() {
    // Absolute cap of 5 charAt reads, independent of input length; the 6th must throw.
    LimitedCharSequence seq = new LimitedCharSequence("abc", null, 5);
    for (int i = 0; i < 5; i++) {
      seq.charAt(0); // 5 reads are allowed (count > limit is the trip condition)
    }
    assertThrows(IllegalArgumentException.class, () -> seq.charAt(0));
  }

  @Test
  public void capIsAbsoluteNotScaledByLength() {
    // A 1000-char input with a cap of 3 still trips on the 4th read — the cap does not scale with
    // input length (unlike the old factor model). This is what closes the short-input bypass.
    LimitedCharSequence seq = new LimitedCharSequence("x".repeat(1000), null, 3);
    seq.charAt(0);
    seq.charAt(0);
    seq.charAt(0);
    assertThrows(IllegalArgumentException.class, () -> seq.charAt(0));
  }

  @Test
  public void errorMessageIncludesPatternAndLimit() {
    Pattern pattern = Pattern.compile("(a+)+");
    LimitedCharSequence seq = new LimitedCharSequence("ab", pattern, 2);
    seq.charAt(0);
    seq.charAt(0);
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> seq.charAt(0));
    assertTrue(e.getMessage().contains("too many characters"));
    assertTrue(e.getMessage().contains("(a+)+"), "should include the offending pattern");
    assertTrue(e.getMessage().contains("match limit: [2]"));
    assertTrue(e.getMessage().contains("plugins.ppl.regex.match.limit"));
  }

  @Test
  public void snippetTruncatesLongInput() {
    String longInput = "x".repeat(200);
    LimitedCharSequence seq = new LimitedCharSequence(longInput, null, 1_000_000);
    String snippet = seq.snippet(LimitedCharSequence.MAX_STR_LENGTH);
    assertTrue(snippet.length() <= LimitedCharSequence.MAX_STR_LENGTH);
    assertTrue(snippet.endsWith("..."));
  }

  @Test
  public void snippetReturnsFullShortInput() {
    LimitedCharSequence seq = new LimitedCharSequence("short", null, 1_000_000);
    assertEquals("short", seq.snippet(LimitedCharSequence.MAX_STR_LENGTH));
  }
}
