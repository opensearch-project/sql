/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.config;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;

@ExtendWith(MockitoExtension.class)
public class AsyncExecutorServiceModuleTest {

  @Mock private NodeClient nodeClient;

  @Mock private ClusterService clusterService;

  @Mock private Settings settings;

  @Mock private DataSourceService dataSourceService;

  @Test
  public void testAsyncQueryExecutorService() {
    ModulesBuilder modulesBuilder = new ModulesBuilder();
    modulesBuilder.add(new AsyncExecutorServiceModule());
    modulesBuilder.add(
        b -> {
          b.bind(NodeClient.class).toInstance(nodeClient);
          b.bind(org.opensearch.sql.common.setting.Settings.class).toInstance(settings);
          b.bind(DataSourceService.class).toInstance(dataSourceService);
          b.bind(ClusterService.class).toInstance(clusterService);
        });
    Injector injector = modulesBuilder.createInjector();
    assertNotNull(injector.getInstance(AsyncQueryExecutorService.class));
    assertNotNull(Metrics.getInstance().getMetric("active_async_query_sessions_count"));
    assertNotNull(Metrics.getInstance().getMetric("active_async_query_statements_count"));
  }
}
