/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

import java.util.Map;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Per-invocation context handed to {@link RestEndpointHandler#fetch} at execution time (scan open),
 * carrying the validated query args and the node transport client a provider may use for its own
 * read-only transport action. A provider that already holds a client may ignore {@link #client()}.
 */
public interface RestEndpointContext {

  /** Validated query args for this invocation (never null; empty when none supplied). */
  Map<String, String> args();

  /** Node transport client for a provider that issues its own transport action; may be null. */
  NodeClient client();

  static RestEndpointContext of(Map<String, String> args, NodeClient client) {
    Map<String, String> safeArgs = args == null ? Map.of() : args;
    return new RestEndpointContext() {
      @Override
      public Map<String, String> args() {
        return safeArgs;
      }

      @Override
      public NodeClient client() {
        return client;
      }
    };
  }
}
