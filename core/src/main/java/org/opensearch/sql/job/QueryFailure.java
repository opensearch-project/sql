/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.Objects;

/**
 * Neutral, client-safe description of a query failure.
 *
 * <p>The lifecycle layer stores this snapshot rather than the original {@link Throwable} so that
 * later observers cannot mutate the failure and no engine-specific exception types leak through the
 * public API.
 *
 * @param type simple exception type
 * @param reason client-facing message
 */
public record QueryFailure(String type, String reason) {

  /**
   * Validates the client-safe descriptor. Both fields are required.
   *
   * @throws NullPointerException if {@code type} or {@code reason} is {@code null}
   */
  public QueryFailure {
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(reason, "reason must not be null");
  }

  /** Builds a snapshot from an exception thrown by a runner. */
  public static QueryFailure of(Throwable throwable) {
    Objects.requireNonNull(throwable, "throwable must not be null");
    String type =
        throwable.getClass().getSimpleName().isBlank()
            ? throwable.getClass().getName()
            : throwable.getClass().getSimpleName();
    String message = throwable.getMessage();
    String reason = (message == null || message.isBlank()) ? "query execution failed" : message;
    return new QueryFailure(type, reason);
  }
}
