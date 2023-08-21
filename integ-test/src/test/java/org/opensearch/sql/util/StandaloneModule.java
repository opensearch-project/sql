/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import lombok.RequiredArgsConstructor;
import org.opensearch.client.RestHighLevelClient;
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
import org.opensearch.sql.monitor.AlwaysHealthyMonitor;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.client.OpenSearchRestClient;
import org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngine;
import org.opensearch.sql.opensearch.executor.protector.ExecutionProtector;
import org.opensearch.sql.opensearch.executor.protector.OpenSearchExecutionProtector;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.storage.StorageEngine;

/**
 * A utility class which registers SQL engine singletons as `OpenSearchPluginModule` does. It is
 * needed to get access to those instances in test and validate their behavior.
 */
@RequiredArgsConstructor
public class StandaloneModule extends AbstractModule {

  private final RestHighLevelClient client;

  private final Settings settings;

  private final DataSourceService dataSourceService;

  private final BuiltinFunctionRepository functionRepository =
      BuiltinFunctionRepository.getInstance();

  @Override
  protected void configure() {}

  @Provides
  public OpenSearchClient openSearchClient() {
    return new OpenSearchRestClient(client);
  }

  @Provides
  public StorageEngine storageEngine(OpenSearchClient client) {
    return new OpenSearchStorageEngine(client, settings);
  }

  @Provides
  public ExecutionEngine executionEngine(
      OpenSearchClient client, ExecutionProtector protector, PlanSerializer planSerializer) {
    return new OpenSearchExecutionEngine(client, protector, planSerializer);
  }

  @Provides
  public ResourceMonitor resourceMonitor() {
    return new AlwaysHealthyMonitor();
  }

  @Provides
  public ExecutionProtector protector(ResourceMonitor resourceMonitor) {
    return new OpenSearchExecutionProtector(resourceMonitor);
  }

  @Provides
  @Singleton
  public QueryManager queryManager() {
    return new ExecuteOnCallerThreadQueryManager();
  }

  @Provides
  public PPLService pplService(QueryManager queryManager, QueryPlanFactory queryPlanFactory) {
    return new PPLService(new PPLSyntaxParser(), queryManager, queryPlanFactory);
  }

  @Provides
  public SQLService sqlService(QueryManager queryManager, QueryPlanFactory queryPlanFactory) {
    return new SQLService(new SQLSyntaxParser(), queryManager, queryPlanFactory);
  }

  @Provides
  public PlanSerializer planSerializer(StorageEngine storageEngine) {
    return new PlanSerializer(storageEngine);
  }

  @Provides
  public QueryPlanFactory queryPlanFactory(QueryService qs) {

    return new QueryPlanFactory(qs);
  }

  @Provides
  public QueryService queryService(ExecutionEngine executionEngine) {
    Analyzer analyzer =
        new Analyzer(
            new ExpressionAnalyzer(functionRepository), dataSourceService, functionRepository);
    Planner planner = new Planner(LogicalPlanOptimizer.create());
    return new QueryService(analyzer, executionEngine, planner);
  }
}
