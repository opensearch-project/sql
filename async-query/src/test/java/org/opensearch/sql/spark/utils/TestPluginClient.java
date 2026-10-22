/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.utils;

import org.opensearch.identity.noop.NoopIdentityPlugin;
import org.opensearch.sql.opensearch.client.PluginClient;
import org.opensearch.transport.client.Client;

/** Builds the plugin client the state store expects, for tests that run without security. */
public final class TestPluginClient {

  private TestPluginClient() {}

  /**
   * Wraps a client in a plugin client with the subject core assigns when security is absent.
   *
   * @param delegate the client the actions run on
   * @return a plugin client with its subject assigned
   */
  public static PluginClient of(Client delegate) {
    PluginClient pluginClient = new PluginClient(delegate);
    pluginClient.setSubject(
        new NoopIdentityPlugin(delegate.threadPool()).getPluginSubject(null));
    return pluginClient;
  }
}
