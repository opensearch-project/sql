/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import java.util.regex.Pattern;

/**
 * A {@link CharSequence} that wraps another sequence and caps the total number of {@code charAt}
 * accesses a regex engine may perform against it. Once accesses exceed {@code matchLimit} the match
 * is aborted with an {@link IllegalArgumentException}, bounding worst-case backtracking work
 * without changing the regex engine or affecting normal matches.
 *
 * <p>The cap is an absolute total (not scaled by input length), analogous to PCRE2's {@code
 * match_limit} (and Splunk {@code rex}'s {@code match_limit}, default 100000), which bounds how
 * many times the matcher's internal loop runs. A catastrophic pattern re-reads characters far past
 * this cap and is aborted in well under a millisecond, regardless of how the attacker sizes the
 * input.
 *
 * <p>Adapted from {@code org.opensearch.painless.api.LimitedCharSequence}; kept plugin-local to
 * avoid a {@code lang-painless} dependency.
 */
public final class LimitedCharSequence implements CharSequence {
  private final CharSequence wrapped;
  private final long matchLimit;
  private long count;

  // Retained for the error message only.
  private final Pattern pattern;

  static final int MAX_STR_LENGTH = 64;
  private static final String SNIPPET = "...";

  /**
   * @param wrap the input text the regex engine will read
   * @param pattern the pattern being matched (for the error message only; may be null)
   * @param matchLimit absolute cap on total charAt reads before the match is aborted; must be
   *     positive
   */
  public LimitedCharSequence(CharSequence wrap, Pattern pattern, long matchLimit) {
    if (matchLimit <= 0) {
      throw new IllegalArgumentException("matchLimit must be positive");
    }
    this.wrapped = wrap;
    this.matchLimit = matchLimit;
    this.pattern = pattern;
  }

  String details() {
    return (pattern != null ? "pattern: [" + pattern.pattern() + "], " : "")
        + "match limit: ["
        + matchLimit
        + "], "
        + "count: ["
        + count
        + "], "
        + "wrapped: ["
        + snippet(MAX_STR_LENGTH)
        + "]";
  }

  /** Snip a long wrapped CharSequence for error messages. */
  String snippet(int maxStrLength) {
    if (maxStrLength < SNIPPET.length() * 6) {
      throw new IllegalArgumentException(
          "max str length must be large enough to include three snippets and three context chars,"
              + " at least ["
              + SNIPPET.length() * 6
              + "], not ["
              + maxStrLength
              + "]");
    }

    if (wrapped.length() <= maxStrLength) {
      return wrapped.toString();
    }

    return wrapped.subSequence(0, maxStrLength - SNIPPET.length()) + SNIPPET;
  }

  @Override
  public int length() {
    return wrapped.length();
  }

  @Override
  public char charAt(int index) {
    count++;
    if (count > matchLimit) {
      throw new IllegalArgumentException(
          "Regular expression in PPL parse/rex command considered too many characters, "
              + details()
              + ". The pattern and input combination is too expensive to evaluate (possible"
              + " catastrophic backtracking). This limit can be changed with the"
              + " plugins.ppl.regex.match.limit setting.");
    }
    return wrapped.charAt(index);
  }

  @Override
  public CharSequence subSequence(int start, int end) {
    return wrapped.subSequence(start, end);
  }

  @Override
  public String toString() {
    return wrapped.toString();
  }
}
