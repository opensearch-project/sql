/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.plugin.rest;

import org.opensearch.client.node.NodeClient;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.ExecutionEngine;
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
