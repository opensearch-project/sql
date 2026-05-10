/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.calcite.rel.RelNode;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Tests for analytics index routing in RestUnifiedQueryAction. Routing is driven by the {@code
 * index.pluggable.dataformat.enabled} index setting, read from cluster state.
 */
public class RestUnifiedQueryActionTest {

  private ClusterService clusterService;
  private Metadata metadata;
  private RestUnifiedQueryAction action;

  @Before
  public void setUp() {
    clusterService = mock(ClusterService.class);
    ClusterState clusterState = mock(ClusterState.class);
    metadata = mock(Metadata.class);
    when(clusterService.state()).thenReturn(clusterState);
    when(clusterState.metadata()).thenReturn(metadata);

    @SuppressWarnings("unchecked")
    QueryPlanExecutor<RelNode, Iterable<Object[]>> executor = mock(QueryPlanExecutor.class);
    action =
        new RestUnifiedQueryAction(
            mock(NodeClient.class),
            clusterService,
            executor,
            mock(org.opensearch.sql.common.setting.Settings.class));
  }

  @Test
  public void pluggableDataformatIndexRoutesToAnalytics() {
    registerIndex(
        "parquet_logs",
        Settings.builder()
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .build());

    assertTrue(action.isAnalyticsIndex("source = parquet_logs | fields ts", QueryType.PPL));
    assertTrue(
        action.isAnalyticsIndex("source = opensearch.parquet_logs | fields ts", QueryType.PPL));
  }

  @Test
  public void indexWithoutSettingRoutesToLucene() {
    registerIndex("plain_logs", Settings.EMPTY);

    assertFalse(action.isAnalyticsIndex("source = plain_logs | fields ts", QueryType.PPL));
  }

  @Test
  public void missingIndexRoutesToLucene() {
    assertFalse(action.isAnalyticsIndex("source = does_not_exist | fields ts", QueryType.PPL));
  }

  @Test
  public void nullAndEmptyQueriesRouteToLucene() {
    assertFalse(action.isAnalyticsIndex(null, QueryType.PPL));
    assertFalse(action.isAnalyticsIndex("", QueryType.PPL));
  }

  private void registerIndex(String name, Settings settings) {
    IndexMetadata indexMetadata = mock(IndexMetadata.class);
    when(indexMetadata.getSettings()).thenReturn(settings);
    when(metadata.index(name)).thenReturn(indexMetadata);
  }
}
