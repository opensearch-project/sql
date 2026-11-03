/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Bounds the backtracking work a user-supplied parse regular expression may perform against per-row
 * field values.
 *
 * <p>Matches against user-supplied text should go through {@link #boundedMatcher}, which wraps the
 * input in a {@link LimitedCharSequence} so a pathological pattern/input combination is aborted
 * with a client-side error instead of occupying a worker thread indefinitely.
 */
public class RegexCommonUtils {

  /**
   * Default absolute cap on the number of characters the regex engine may read in a single match
   * before it is aborted. Mirrors the default of PCRE2's {@code match_limit} as adopted by Splunk
   * {@code rex} (100000): high enough never to trip legitimate matches, low enough that a
   * catastrophic match aborts in well under a millisecond. Overridable via the {@code
   * plugins.ppl.regex.match.limit} setting through {@link #setMatchLimit(long)}.
   */
  public static final long DEFAULT_MATCH_LIMIT = 100_000L;

  // Node-scoped value, pushed in by the OpenSearch settings layer on register and on dynamic
  // change. volatile so updates are visible to matching threads.
  private static volatile long matchLimit = DEFAULT_MATCH_LIMIT;

  private RegexCommonUtils() {}

  /**
   * Set the absolute per-match character-read cap. Called by the OpenSearch settings layer when
   * {@code plugins.ppl.regex.match.limit} is registered or changed. Non-positive values are ignored
   * (the previous value is kept) so a misconfiguration cannot disable the guard.
   */
  public static void setMatchLimit(long limit) {
    if (limit > 0) {
      matchLimit = limit;
    }
  }

  /** Current absolute per-match character-read cap. */
  public static long getMatchLimit() {
    return matchLimit;
  }

  /**
   * Create a {@link Matcher} whose backtracking work is bounded by wrapping the input in a {@link
   * LimitedCharSequence}. All user-controlled parse matching should use this.
   *
   * @param pattern the compiled pattern
   * @param text the input text to match against
   * @return a Matcher over a length-bounded view of the text
   */
  public static Matcher boundedMatcher(Pattern pattern, String text) {
    return pattern.matcher(new LimitedCharSequence(text, pattern, matchLimit));
  }
}
