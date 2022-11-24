/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


package org.opensearch.sql.plugin.config;

import org.opensearch.client.node.NodeClient;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
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
import org.opensearch.sql.storage.StorageEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * OpenSearch plugin config that injects cluster service and node client from plugin
 * and initialize OpenSearch storage and execution engine.
 */
@Configuration
public class OpenSearchPluginConfig {

  @Autowired
  private NodeClient nodeClient;

  @Autowired
  private Settings settings;

  @Autowired
  private DataSourceService dataSourceService;

  @Bean
  @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
  public OpenSearchClient client() {
    return new OpenSearchNodeClient(nodeClient);
  }

  @Bean
  @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
  public StorageEngine storageEngine() {
    return new OpenSearchStorageEngine(client(), settings);
  }

  @Bean
  @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
  public ExecutionEngine executionEngine() {
    return new OpenSearchExecutionEngine(client(), protector());
  }

  @Bean
  @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
  public ResourceMonitor resourceMonitor() {
    return new OpenSearchResourceMonitor(settings, new OpenSearchMemoryHealthy());
  }

  @Bean
  @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
  public ExecutionProtector protector() {
    return new OpenSearchExecutionProtector(resourceMonitor());
  }

  /**
   * Per node singleton object.
   */
  @Bean
  public QueryManager queryManager() {
    return new OpenSearchQueryManager(nodeClient);
  }

  /**
   * QueryPlanFactory.
   */
  @Bean
  @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
  public QueryPlanFactory queryExecutionFactory() {
    BuiltinFunctionRepository functionRepository = BuiltinFunctionRepository.getInstance();
    Analyzer analyzer = new Analyzer(new ExpressionAnalyzer(functionRepository),
        dataSourceService, functionRepository);
    Planner planner =
        new Planner(LogicalPlanOptimizer.create());
    return new QueryPlanFactory(new QueryService(analyzer, executionEngine(), planner));
  }
}
