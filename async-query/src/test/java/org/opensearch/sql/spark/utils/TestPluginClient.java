/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.utils;

import org.opensearch.identity.NamedPrincipal;
import org.opensearch.sql.opensearch.client.PluginClient;
import org.opensearch.transport.client.Client;

/**
 * Builds the plugin client the state store expects. These tests run without the security plugin, so
 * the subject only has to exist: {@code Subject.runAs} runs the action on the calling thread.
 */
public class TestPluginClient {

  private TestPluginClient() {}

  public static PluginClient of(Client delegate) {
    PluginClient pluginClient = new PluginClient(delegate);
    pluginClient.setSubject(() -> new NamedPrincipal("sql"));
    return pluginClient;
  }
}
