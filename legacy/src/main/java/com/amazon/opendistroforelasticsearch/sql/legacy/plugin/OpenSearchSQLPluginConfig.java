/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.sql.legacy.plugin;

import com.amazon.opendistroforelasticsearch.sql.common.setting.Settings;
import com.amazon.opendistroforelasticsearch.sql.executor.ExecutionEngine;
import com.amazon.opendistroforelasticsearch.sql.monitor.ResourceMonitor;
import com.amazon.opendistroforelasticsearch.sql.opensearch.client.OpenSearchClient;
import com.amazon.opendistroforelasticsearch.sql.opensearch.client.OpenSearchNodeClient;
import com.amazon.opendistroforelasticsearch.sql.opensearch.executor.OpenSearchExecutionEngine;
import com.amazon.opendistroforelasticsearch.sql.opensearch.executor.protector.ExecutionProtector;
import com.amazon.opendistroforelasticsearch.sql.opensearch.executor.protector.OpenSearchExecutionProtector;
import com.amazon.opendistroforelasticsearch.sql.opensearch.monitor.OpenSearchMemoryHealthy;
import com.amazon.opendistroforelasticsearch.sql.opensearch.monitor.OpenSearchResourceMonitor;
import com.amazon.opendistroforelasticsearch.sql.opensearch.storage.OpenSearchStorageEngine;
import com.amazon.opendistroforelasticsearch.sql.storage.StorageEngine;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

/**
 * OpenSearch Plugin Config for SQL.
 */
public class OpenSearchSQLPluginConfig {
  @Autowired
  private ClusterService clusterService;

  @Autowired
  private NodeClient nodeClient;

  @Autowired
  private Settings settings;

  @Bean
  public OpenSearchClient client() {
    return new OpenSearchNodeClient(clusterService, nodeClient);
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
