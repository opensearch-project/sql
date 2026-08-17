/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser.bucket;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/** Lookup table mapping bucket-function names to their {@link BucketFunctionExpander}. */
public final class BucketFunctionRegistry {

  private static final Map<String, BucketFunctionExpander> EXPANDERS =
      Map.of(
          HistogramExpander.FUNCTION_NAME, new HistogramExpander(),
          DateHistogramExpander.FUNCTION_NAME, new DateHistogramExpander());

  private BucketFunctionRegistry() {}

  /**
   * Returns the expander for {@code functionName} (case-insensitive), or empty if not a bucket
   * function.
   */
  public static Optional<BucketFunctionExpander> lookup(String functionName) {
    if (functionName == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(EXPANDERS.get(functionName.toUpperCase(Locale.ROOT)));
  }
}
