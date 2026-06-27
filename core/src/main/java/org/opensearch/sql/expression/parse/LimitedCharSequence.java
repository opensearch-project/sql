/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import java.util.regex.Pattern;

/**
 * A {@link CharSequence} that wraps another sequence and caps how many {@code charAt} accesses a
 * regex engine may perform against it. Once accesses exceed {@code limitFactor * length} the match
 * is aborted with an {@link IllegalArgumentException}, bounding worst-case backtracking work
 * without changing the regex engine or affecting normal matches.
 *
 * <p>Adapted from {@code org.opensearch.painless.api.LimitedCharSequence}; kept plugin-local to
 * avoid a {@code lang-painless} dependency.
 */
public final class LimitedCharSequence implements CharSequence {
  private final CharSequence wrapped;
  private final Counter counter;

  // Retained for the error message only.
  private final Pattern pattern;
  private final int limitFactor;

  static final int MAX_STR_LENGTH = 64;
  private static final String SNIPPET = "...";

  /**
   * @param wrap the input text the regex engine will read
   * @param pattern the pattern being matched (for the error message only; may be null)
   * @param limitFactor charAt budget as a multiple of input length; must be positive
   */
  public LimitedCharSequence(CharSequence wrap, Pattern pattern, int limitFactor) {
    if (limitFactor <= 0) {
      throw new IllegalArgumentException("limitFactor must be positive");
    }
    this.wrapped = wrap;
    this.counter = new Counter((long) limitFactor * wrapped.length());
    this.pattern = pattern;
    this.limitFactor = limitFactor;
  }

  String details() {
    return (pattern != null ? "pattern: [" + pattern.pattern() + "], " : "")
        + "limit factor: ["
        + limitFactor
        + "], "
        + "char limit: ["
        + counter.charAtLimit
        + "], "
        + "count: ["
        + counter.count
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
    counter.count++;
    if (counter.hitLimit()) {
      throw new IllegalArgumentException(
          "Regular expression in PPL parse/rex command considered too many characters, "
              + details()
              + ". The pattern and input combination is too expensive to evaluate (possible"
              + " catastrophic backtracking).");
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

  /** Tracks charAt accesses across the wrapped sequence. */
  private static final class Counter {
    final long charAtLimit;
    long count;

    Counter(long charAtLimit) {
      this.charAtLimit = charAtLimit;
      this.count = 0;
    }

    boolean hitLimit() {
      return count > charAtLimit;
    }
  }
}
