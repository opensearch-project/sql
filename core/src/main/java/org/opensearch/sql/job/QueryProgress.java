/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

/**
 * Best-effort estimate of how much of a query's work is done.
 *
 * @param fractionDone finite value in {@code [0.0, 1.0]}
 */
public record QueryProgress(double fractionDone) {

  public static final QueryProgress NONE = new QueryProgress(0D);

  public static final QueryProgress DONE = new QueryProgress(1D);

  public QueryProgress {
    if (!Double.isFinite(fractionDone) || fractionDone < 0D || fractionDone > 1D) {
      throw new IllegalArgumentException("fractionDone must be within [0.0, 1.0]");
    }
  }
}
