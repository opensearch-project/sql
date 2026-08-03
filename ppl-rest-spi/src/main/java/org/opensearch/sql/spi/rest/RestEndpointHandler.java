/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

import java.util.List;

/**
 * Produces the rows of a {@code rest} endpoint. Invoked at execution time (scan open), never at
 * planning, so the scan stays lazy and EXPLAIN is side-effect free; a transport-backed provider may
 * block on {@code ctx.client().execute(...).actionGet()} here. Each row is one string, surfaced as
 * the single {@code response} column, typically a serialized JSON document; a query extracts and
 * casts the fields it needs with {@code spath} or {@code json_extract}. A provider that must mask
 * sensitive values does so before serializing the response it returns here.
 */
@FunctionalInterface
public interface RestEndpointHandler {

  /**
   * Fetch the rows for one invocation.
   *
   * @param ctx the validated query args and (optional) transport client for this invocation
   * @return one response string per row (never null; empty when there are no rows)
   */
  List<String> fetch(RestEndpointContext ctx);
}
