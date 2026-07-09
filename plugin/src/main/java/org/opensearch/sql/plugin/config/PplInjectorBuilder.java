/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.config;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Guice;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Builds the PPL execution Guice injector shared by the PPL transport actions. Both the foreground
 * query action and the background collect materialize action construct their stack through this one
 * path so the module wiring lives in a single place.
 */
public final class PplInjectorBuilder {

  private PplInjectorBuilder() {}

  public static Injector build(
      NodeClient client,
      ClusterService clusterService,
      DataSourceService dataSourceService,
      EngineExtensionsHolder extensionsHolder) {
    ModulesBuilder modules = new ModulesBuilder();
    modules.add(new OpenSearchPluginModule(extensionsHolder.engines()));
    Settings pluginSettings = new OpenSearchSettings(clusterService.getClusterSettings());
    modules.add(
        b -> {
          b.bind(NodeClient.class).toInstance(client);
          b.bind(Settings.class).toInstance(pluginSettings);
          b.bind(DataSourceService.class).toInstance(dataSourceService);
        });
    return Guice.createInjector(modules);
  }
}
