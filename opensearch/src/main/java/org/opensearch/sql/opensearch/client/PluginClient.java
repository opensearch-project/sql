/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import org.opensearch.transport.client.Client;

/** The SQL plugin's client that runs transport actions as its system subject. */
public final class PluginClient extends org.opensearch.commons.client.PluginClient {

  /**
   * Wraps a client so its actions run as the plugin subject, which is assigned later.
   *
   * @param delegate the client whose actions this wraps
   */
  public PluginClient(Client delegate) {
    super(delegate);
  }
}
