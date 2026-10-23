/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job.exceptions;

/**
 * Thrown when a caller attempts to access a job owned by a different principal.
 *
 * <p>The message is intentionally generic: leaking the owner identity would be an information
 * disclosure. Callers observe only "forbidden", never "wrong user".
 */
public final class QueryJobForbiddenException extends RuntimeException {

  /**
   * Constructs the exception with a fixed, owner-agnostic message. The message deliberately omits
   * the owner's identity to avoid information disclosure across principals.
   */
  public QueryJobForbiddenException() {
    super("Not authorized to access this query job");
  }
}
