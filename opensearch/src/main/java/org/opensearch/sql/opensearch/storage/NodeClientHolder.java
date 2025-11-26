/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import org.opensearch.transport.client.node.NodeClient;

public final class NodeClientHolder {
  private static volatile NodeClient INSTANCE;

  private NodeClientHolder() {}

  public static void init(NodeClient client) {
    INSTANCE = client;
  }

  public static NodeClient get() {
    if (INSTANCE == null) {
      throw new IllegalStateException("NodeClient has not been initialized");
    }
    return INSTANCE;
  }
}
