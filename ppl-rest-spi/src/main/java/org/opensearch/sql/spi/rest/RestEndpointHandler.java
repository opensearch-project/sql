/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

import java.util.List;
import java.util.Map;

/**
 * Produces the raw rows of a {@code rest} endpoint. Invoked at execution time (scan open), never at
 * planning time, so the scan stays lazy and EXPLAIN is side-effect free. A transport backed
 * provider may block on {@code ctx.client().execute(action, request).actionGet()} inside {@link
 * #fetch}; because {@code fetch} runs at execution rather than planning, blocking here is expected
 * and safe.
 *
 * <p>Each row is a map of output-column name to a plain {@code java.lang} value (String, Number,
 * Boolean, or a nested Map of the same). The sql side coerces each value to the declared {@link
 * Column} type and rejects an uncoercible value with a clear client error, so a handler does not
 * need to pre-shape its values to the schema.
 */
@FunctionalInterface
public interface RestEndpointHandler {

  /**
   * Fetch the rows for one invocation.
   *
   * @param ctx the validated query args and (optional) transport client for this invocation
   * @return one map of column name to value per row (never null; empty when there are no rows)
   */
  List<Map<String, Object>> fetch(RestEndpointContext ctx);
}
