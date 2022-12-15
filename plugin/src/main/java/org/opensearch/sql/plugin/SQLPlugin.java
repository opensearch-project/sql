/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin;

import static org.opensearch.sql.datasource.model.DataSourceMetadata.defaultOpenSearchDataSourceMetadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ReloadablePlugin;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptService;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.DataSourceServiceImpl;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.executor.AsyncRestExecutor;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.plugin.RestSqlAction;
import org.opensearch.sql.legacy.plugin.RestSqlStatsAction;
import org.opensearch.sql.opensearch.client.OpenSearchNodeClient;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.opensearch.setting.LegacyOpenDistroSettings;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.opensearch.storage.OpenSearchDataSourceFactory;
import org.opensearch.sql.opensearch.storage.script.ExpressionScriptEngine;
import org.opensearch.sql.opensearch.storage.serialization.DefaultExpressionSerializer;
import org.opensearch.sql.plugin.config.OpenSearchPluginModule;
import org.opensearch.sql.plugin.datasource.DataSourceSettings;
import org.opensearch.sql.plugin.rest.RestPPLQueryAction;
import org.opensearch.sql.plugin.rest.RestPPLStatsAction;
import org.opensearch.sql.plugin.rest.RestQuerySettingsAction;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.prometheus.storage.PrometheusStorageFactory;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

public class SQLPlugin extends Plugin implements ActionPlugin, ScriptPlugin, ReloadablePlugin {

  private static final Logger LOG = LogManager.getLogger();

  private ClusterService clusterService;

  /**
   * Settings should be inited when bootstrap the plugin.
   */
  private org.opensearch.sql.common.setting.Settings pluginSettings;

  private NodeClient client;

  private SQLService sqlService;

  private DataSourceService dataSourceService;

  public String name() {
    return "sql";
  }

  public String description() {
    return "Use sql to query OpenSearch.";
  }

  @Override
  public List<RestHandler> getRestHandlers(
      Settings settings,
      RestController restController,
      ClusterSettings clusterSettings,
      IndexScopedSettings indexScopedSettings,
      SettingsFilter settingsFilter,
      IndexNameExpressionResolver indexNameExpressionResolver,
      Supplier<DiscoveryNodes> nodesInCluster) {
    Objects.requireNonNull(clusterService, "Cluster service is required");
    Objects.requireNonNull(pluginSettings, "Cluster settings is required");

    LocalClusterState.state().setResolver(indexNameExpressionResolver);
    Metrics.getInstance().registerDefaultMetrics();

    return Arrays.asList(
        new RestPPLQueryAction(pluginSettings, settings),
        new RestSqlAction(settings, sqlService),
        new RestSqlStatsAction(settings, restController),
        new RestPPLStatsAction(settings, restController),
        new RestQuerySettingsAction(settings, restController));
  }

  /**
   * Register action and handler so that transportClient can find proxy for action.
   */
  @Override
  public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
    return Arrays.asList(
        new ActionHandler<>(
            new ActionType<>(PPLQueryAction.NAME, TransportPPLQueryResponse::new),
            TransportPPLQueryAction.class));
  }

  @Override
  public Collection<Object> createComponents(
      Client client,
      ClusterService clusterService,
      ThreadPool threadPool,
      ResourceWatcherService resourceWatcherService,
      ScriptService scriptService,
      NamedXContentRegistry contentRegistry,
      Environment environment,
      NodeEnvironment nodeEnvironment,
      NamedWriteableRegistry namedWriteableRegistry,
      IndexNameExpressionResolver indexNameResolver,
      Supplier<RepositoriesService> repositoriesServiceSupplier) {
    this.clusterService = clusterService;
    this.pluginSettings = new OpenSearchSettings(clusterService.getClusterSettings());
    this.client = (NodeClient) client;
    this.dataSourceService =
        new DataSourceServiceImpl(
            new ImmutableSet.Builder<DataSourceFactory>()
                .add(new OpenSearchDataSourceFactory(
                        new OpenSearchNodeClient(this.client), pluginSettings))
                .add(new PrometheusStorageFactory())
                .build());
    dataSourceService.addDataSource(defaultOpenSearchDataSourceMetadata());
    loadDataSources(dataSourceService, clusterService.getSettings());
    LocalClusterState.state().setClusterService(clusterService);
    LocalClusterState.state().setPluginSettings((OpenSearchSettings) pluginSettings);

    ModulesBuilder modules = new ModulesBuilder();
    modules.add(
        new OpenSearchPluginModule((NodeClient) client, pluginSettings, dataSourceService));
    Injector injector = modules.createInjector();
    PPLService pplService =
        SecurityAccess.doPrivileged(() -> injector.getInstance(PPLService.class));
    this.sqlService = SecurityAccess.doPrivileged(() -> injector.getInstance(SQLService.class));

    // return objects used by Guice to inject dependencies for e.g.,
    // transport action handler constructors
    return ImmutableList.of(pplService);
  }

  @Override
  public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
    return Collections.singletonList(
        new FixedExecutorBuilder(
            settings,
            AsyncRestExecutor.SQL_WORKER_THREAD_POOL_NAME,
            OpenSearchExecutors.allocatedProcessors(settings),
            1000,
            null));
  }

  @Override
  public List<Setting<?>> getSettings() {
    return new ImmutableList.Builder<Setting<?>>()
        .addAll(LegacyOpenDistroSettings.legacySettings())
        .addAll(OpenSearchSettings.pluginSettings())
        .add(DataSourceSettings.DATASOURCE_CONFIG)
        .build();
  }

  @Override
  public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
    return new ExpressionScriptEngine(new DefaultExpressionSerializer());
  }

  @Override
  public void reload(Settings settings) {
    dataSourceService.clear();
    dataSourceService.addDataSource(defaultOpenSearchDataSourceMetadata());
    loadDataSources(dataSourceService, settings);
  }

  /**
   * load {@link DataSource} from settings.
   */
  @VisibleForTesting
  public static void loadDataSources(DataSourceService dataSourceService, Settings settings) {
    SecurityAccess.doPrivileged(
        () -> {
          InputStream inputStream = DataSourceSettings.DATASOURCE_CONFIG.get(settings);
          if (inputStream != null) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            try {
              List<DataSourceMetadata> metadataList =
                  objectMapper.readValue(inputStream, new TypeReference<>() {});
              dataSourceService.addDataSource(metadataList.toArray(new DataSourceMetadata[0]));
            } catch (IOException e) {
              LOG.error(
                  "DataSource Configuration File uploaded is malformed. Verify and re-upload.", e);
            } catch (Throwable e) {
              LOG.error("DataSource construction failed.", e);
            }
          }
          return null;
        });
  }
}
