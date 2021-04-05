/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.plugin;

import com.amazon.opendistroforelasticsearch.sql.elasticsearch.setting.OpenSearchSettings;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.script.ExpressionScriptEngine;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage.serialization.DefaultExpressionSerializer;
import com.amazon.opendistroforelasticsearch.sql.legacy.esdomain.LocalClusterState;
import com.amazon.opendistroforelasticsearch.sql.legacy.executor.AsyncRestExecutor;
import com.amazon.opendistroforelasticsearch.sql.legacy.metrics.Metrics;
import com.amazon.opendistroforelasticsearch.sql.legacy.plugin.RestSqlAction;
import com.amazon.opendistroforelasticsearch.sql.legacy.plugin.RestSqlSettingsAction;
import com.amazon.opendistroforelasticsearch.sql.legacy.plugin.RestSqlStatsAction;
import com.amazon.opendistroforelasticsearch.sql.legacy.plugin.SqlSettings;
import com.amazon.opendistroforelasticsearch.sql.plugin.rest.RestPPLQueryAction;
import com.amazon.opendistroforelasticsearch.sql.plugin.rest.RestPPLStatsAction;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
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
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

public class SQLPlugin extends Plugin implements ActionPlugin, ScriptPlugin {

  /**
   * Sql plugin specific settings in ES cluster settings.
   */
  private final SqlSettings sqlSettings = new SqlSettings();

  private ClusterService clusterService;

  /**
   * Settings should be inited when bootstrap the plugin.
   */
  private com.amazon.opendistroforelasticsearch.sql.common.setting.Settings pluginSettings;

  public String name() {
    return "sql";
  }

  public String description() {
    return "Use sql to query elasticsearch.";
  }

  @Override
  public List<RestHandler> getRestHandlers(Settings settings, RestController restController,
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
        new RestPPLQueryAction(restController, clusterService, pluginSettings, settings),
        new RestSqlAction(settings, clusterService, pluginSettings),
        new RestSqlStatsAction(settings, restController),
        new RestSqlSettingsAction(settings, restController),
        new RestPPLStatsAction(settings, restController)
    );
  }

  @Override
  public Collection<Object> createComponents(Client client, ClusterService clusterService,
                                             ThreadPool threadPool,
                                             ResourceWatcherService resourceWatcherService,
                                             ScriptService scriptService,
                                             NamedXContentRegistry contentRegistry,
                                             Environment environment,
                                             NodeEnvironment nodeEnvironment,
                                             NamedWriteableRegistry namedWriteableRegistry,
                                             IndexNameExpressionResolver indexNameResolver,
                                             Supplier<RepositoriesService>
                                                       repositoriesServiceSupplier) {
    this.clusterService = clusterService;
    this.pluginSettings = new OpenSearchSettings(clusterService.getClusterSettings());

    LocalClusterState.state().setClusterService(clusterService);
    LocalClusterState.state().setSqlSettings(sqlSettings);

    return super
        .createComponents(client, clusterService, threadPool, resourceWatcherService, scriptService,
            contentRegistry, environment, nodeEnvironment, namedWriteableRegistry,
            indexNameResolver, repositoriesServiceSupplier);
  }

  @Override
  public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
    return Collections.singletonList(
        new FixedExecutorBuilder(
            settings,
            AsyncRestExecutor.SQL_WORKER_THREAD_POOL_NAME,
            OpenSearchExecutors.allocatedProcessors(settings),
            1000,
            null
        )
    );
  }

  @Override
  public List<Setting<?>> getSettings() {
    ImmutableList<Setting<?>> settings =
        new ImmutableList.Builder<Setting<?>>().addAll(sqlSettings.getSettings())
            .addAll(OpenSearchSettings.pluginSettings()).build();
    return settings;
  }

  @Override
  public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
    return new ExpressionScriptEngine(new DefaultExpressionSerializer());
  }

}
