/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import java.util.function.Supplier;
import org.opensearch.transport.RemoteClusterService;

/**
 * The node's {@link RemoteClusterService}, for {@link OpenSearchNodeClient}. It is only reachable
 * through {@code TransportService}, which plugin components are created before, so the plugin
 * creates this provider with its components and a transport action fills it at startup.
 */
public class RemoteClusterServiceProvider implements Supplier<RemoteClusterService> {

  private volatile RemoteClusterService service;

  public void set(RemoteClusterService remoteClusterService) {
    this.service = remoteClusterService;
  }

  @Override
  public RemoteClusterService get() {
    RemoteClusterService current = service;
    if (current == null) {
      throw new IllegalStateException("Remote cluster service is not available yet");
    }
    return current;
  }
}
