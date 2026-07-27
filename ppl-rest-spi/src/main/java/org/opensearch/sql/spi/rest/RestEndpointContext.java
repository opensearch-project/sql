/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

import java.util.Map;
import org.opensearch.transport.client.node.NodeClient;

/**
 * The per-invocation context handed to a {@link RestEndpointHandler#fetch} at execution time (scan
 * open), never at planning time. It carries the validated query args and the node transport client
 * a provider may use to issue its own read-only transport action.
 *
 * <p>A provider that already holds a client (e.g. the built-in core endpoints capture the sql
 * client via closure) may ignore {@link #client()} and read only {@link #args()}. A transport
 * backed provider may block on {@code client().execute(action, request).actionGet()} inside {@code
 * fetch}; because {@code fetch} runs at execution rather than planning, EXPLAIN stays side-effect
 * free.
 */
public interface RestEndpointContext {

  /** The validated query args for this invocation (never null; empty when none were supplied). */
  Map<String, String> args();

  /**
   * The node transport client, for a provider that issues its own read-only transport action. May
   * be null in unit tests or for providers that do not need it.
   */
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
