/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.config;

import lombok.RequiredArgsConstructor;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Provides;
import org.opensearch.common.inject.Singleton;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.client.OpenSearchNodeClient;
import org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngine;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.executor.protector.ExecutionProtector;
import org.opensearch.sql.opensearch.executor.protector.OpenSearchExecutionProtector;
import org.opensearch.sql.opensearch.monitor.OpenSearchMemoryHealthy;
import org.opensearch.sql.opensearch.monitor.OpenSearchResourceMonitor;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.storage.StorageEngine;

@RequiredArgsConstructor
public class OpenSearchPluginModule extends AbstractModule {

  private final BuiltinFunctionRepository functionRepository =
      BuiltinFunctionRepository.getInstance();

  @Override
  protected void configure() {
  }

  @Provides
  public OpenSearchClient openSearchClient(NodeClient nodeClient) {
    return new OpenSearchNodeClient(nodeClient);
  }

  @Provides
  public StorageEngine storageEngine(OpenSearchClient client, Settings settings) {
    return new OpenSearchStorageEngine(client, settings);
  }

  @Provides
  public ExecutionEngine executionEngine(OpenSearchClient client, ExecutionProtector protector,
                                         PlanSerializer planSerializer) {
    return new OpenSearchExecutionEngine(client, protector, planSerializer);
  }

  @Provides
  public ResourceMonitor resourceMonitor(Settings settings) {
    return new OpenSearchResourceMonitor(settings, new OpenSearchMemoryHealthy());
  }

  @Provides
  public ExecutionProtector protector(ResourceMonitor resourceMonitor) {
    return new OpenSearchExecutionProtector(resourceMonitor);
  }

  @Provides
  public PlanSerializer planSerializer(StorageEngine storageEngine) {
    return new PlanSerializer(storageEngine);
  }

  @Provides
  @Singleton
  public QueryManager queryManager(NodeClient nodeClient) {
    return new OpenSearchQueryManager(nodeClient);
  }

  @Provides
  public PPLService pplService(QueryManager queryManager, QueryPlanFactory queryPlanFactory) {
    return new PPLService(new PPLSyntaxParser(), queryManager, queryPlanFactory);
  }

  @Provides
  public SQLService sqlService(QueryManager queryManager, QueryPlanFactory queryPlanFactory) {
    return new SQLService(new SQLSyntaxParser(), queryManager, queryPlanFactory);
  }

  /**
   * {@link QueryPlanFactory}.
   */
  @Provides
  public QueryPlanFactory queryPlanFactory(DataSourceService dataSourceService,
      ExecutionEngine executionEngine) {
    Analyzer analyzer =
        new Analyzer(
            new ExpressionAnalyzer(functionRepository), dataSourceService, functionRepository);
    Planner planner = new Planner(LogicalPlanOptimizer.create());
    QueryService queryService = new QueryService(
        analyzer, executionEngine, planner);
    return new QueryPlanFactory(queryService);
  }
}
