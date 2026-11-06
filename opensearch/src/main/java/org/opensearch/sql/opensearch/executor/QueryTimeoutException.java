/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;

/**
 * A query ran past {@code plugins.ppl.query.timeout} and was stopped.
 *
 * <p>Distinct from the {@link OpenSearchTimeoutException} an {@code actionGet(timeout)} raises,
 * which is a backend call failing to answer in time rather than a query exceeding its own deadline.
 *
 * <p>The status is {@link RestStatus#REQUEST_TIMEOUT} rather than {@code GATEWAY_TIMEOUT} because
 * clients retry a gateway timeout: {@code RestClient} marks the node dead and retries on 502, 503
 * and 504, and opensearch-py retries the same three by default. Resending a query that has just
 * exhausted its deadline is the opposite of what the deadline is for.
 */
public class QueryTimeoutException extends OpenSearchTimeoutException {

  /**
   * @param timeout the deadline the query exceeded
   */
  public QueryTimeoutException(TimeValue timeout) {
    super("Query execution timed out after " + timeout);
  }

  @Override
  public RestStatus status() {
    return RestStatus.REQUEST_TIMEOUT;
  }
}
