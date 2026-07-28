/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

import java.util.List;
import java.util.Map;

/**
 * Produces the raw rows of a {@code rest} endpoint. Invoked at execution time (scan open), never at
 * planning, so the scan stays lazy and EXPLAIN is side-effect free; a transport-backed provider may
 * block on {@code ctx.client().execute(...).actionGet()} here. Each row is a map of output-column
 * name to a plain {@code java.lang} value; the sql side coerces each to the declared {@link Column}
 * type.
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
