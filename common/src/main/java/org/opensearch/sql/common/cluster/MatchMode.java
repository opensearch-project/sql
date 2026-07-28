/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.cluster;

import java.util.Locale;

/** Vectorization strategy used by {@link TextSimilarityClustering}. */
public enum MatchMode {
  /** Positional term frequency; token order matters. */
  TERMLIST,
  /** Bag-of-words term frequency; token order ignored. */
  TERMSET,
  /** Character trigram frequency. */
  NGRAMSET;

  /** The mode applied when the user does not specify one. */
  public static final MatchMode DEFAULT = TERMLIST;

  /**
   * Parse a user-supplied match mode. Case-insensitive. A null value resolves to {@link #DEFAULT}.
   *
   * @throws IllegalArgumentException when the value is not a known mode
   */
  public static MatchMode fromString(String value) {
    if (value == null) {
      return DEFAULT;
    }
    switch (value.toLowerCase(Locale.ROOT)) {
      case "termlist":
        return TERMLIST;
      case "termset":
        return TERMSET;
      case "ngramset":
        return NGRAMSET;
      default:
        throw new IllegalArgumentException(
            "Invalid match mode: " + value + ". Must be one of: termlist, termset, ngramset");
    }
  }
}
