/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.plugin;

import org.opensearch.client.node.NodeClient;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.OpenSearchFunctions;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.client.OpenSearchNodeClient;
import org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngine;
import org.opensearch.sql.opensearch.executor.protector.ExecutionProtector;
import org.opensearch.sql.opensearch.executor.protector.OpenSearchExecutionProtector;
import org.opensearch.sql.opensearch.monitor.OpenSearchMemoryHealthy;
import org.opensearch.sql.opensearch.monitor.OpenSearchResourceMonitor;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;
import org.opensearch.sql.storage.StorageEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * OpenSearch Plugin Config for SQL.
 */
@Configuration
@Import({ExpressionConfig.class})
public class OpenSearchSQLPluginConfig {

  @Autowired
  private NodeClient nodeClient;

  @Autowired
  private Settings settings;

  @Autowired
  private BuiltinFunctionRepository functionRepository;

  @Bean
  public OpenSearchClient client() {
    return new OpenSearchNodeClient(nodeClient);
  }

  @Bean
  public StorageEngine storageEngine() {
    return new OpenSearchStorageEngine(client(), settings);
  }

  @Bean
  public ExecutionEngine executionEngine() {
    OpenSearchFunctions.register(functionRepository);
    return new OpenSearchExecutionEngine(client(), protector());
  }

  @Bean
  public ResourceMonitor resourceMonitor() {
    return new OpenSearchResourceMonitor(settings, new OpenSearchMemoryHealthy());
  }

  @Bean
  public ExecutionProtector protector() {
    return new OpenSearchExecutionProtector(resourceMonitor());
  }
}
