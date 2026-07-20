/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

/**
 * Raised when the engine cannot open the Point-In-Time (PIT) context it needs to run a query
 * because the node has hit {@code search.max_open_pit_context}. The engine opens a PIT (one reader
 * context per shard) to page over a query it cannot push down -- e.g. a {@code stats} that groups
 * by a text field with no {@code keyword} sub-field -- so a single query over a many-shard index
 * can exhaust the per-node budget on its own. Its message is the customer-facing {@code reason}.
 */
public class PointInTimeLimitExceededException extends RuntimeException {

  public PointInTimeLimitExceededException(String message, Throwable cause) {
    super(message, cause);
  }
}
