/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.error;

/**
 * Raised when a query cannot proceed because it would exceed a node or cluster resource budget --
 * e.g. the per-node Point-In-Time (PIT) context limit ({@code search.max_open_pit_context}). Pairs
 * with {@link ErrorCode#RESOURCE_LIMIT_EXCEEDED}: the code is the machine-readable classifier while
 * this type gives clients a stable, semantic name to match on. The message is the customer-facing
 * {@code reason}; put the explanation and remedy in the {@link ErrorReport} details.
 */
public class ResourceLimitExceededException extends RuntimeException {

  public ResourceLimitExceededException(String message) {
    super(message);
  }

  public ResourceLimitExceededException(String message, Throwable cause) {
    super(message, cause);
  }
}
