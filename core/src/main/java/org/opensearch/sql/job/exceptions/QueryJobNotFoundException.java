/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job.exceptions;

import org.opensearch.sql.job.QueryJobId;

/** Thrown when a job ID does not resolve on the owner node. */
public final class QueryJobNotFoundException extends RuntimeException {

  /**
   * @param id opaque id the caller supplied; its encoded form is included in the message so log
   *     lines can be correlated to the original request without leaking internal state
   */
  public QueryJobNotFoundException(QueryJobId id) {
    super("Query job [" + id.encode() + "] not found");
  }
}
