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
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.IndicesService;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Tests for analytics index routing in RestUnifiedQueryAction. Routing requires both {@code
 * index.pluggable.dataformat.enabled=true} and {@code index.pluggable.dataformat=composite}.
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
    // isAnalyticsIndex short-circuits on the cluster.pluggable.dataformat setting; the per-index
    // path is only exercised when this returns something other than "composite".
    when(clusterService.getSettings()).thenReturn(Settings.EMPTY);

    @SuppressWarnings("unchecked")
    QueryPlanExecutor<RelNode, Iterable<Object[]>> executor = mock(QueryPlanExecutor.class);
    action =
        new RestUnifiedQueryAction(
            mock(NodeClient.class),
            clusterService,
            executor,
            mock(EngineContextProvider.class),
            mock(org.opensearch.sql.common.setting.Settings.class));
  }

  @Test
  public void pluggableDataformatIndexRoutesToAnalytics() {
    registerIndex(
        "parquet_logs",
        Settings.builder()
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "composite")
            .build());

    assertTrue(action.isAnalyticsIndex("source = parquet_logs | fields ts", QueryType.PPL));
    assertTrue(
        action.isAnalyticsIndex("source = opensearch.parquet_logs | fields ts", QueryType.PPL));
  }

  @Test
  public void pluggableEnabledButLuceneFormatRoutesToLucene() {
    registerIndex(
        "lucene_logs",
        Settings.builder()
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "lucene")
            .build());

    assertFalse(action.isAnalyticsIndex("source = lucene_logs | fields ts", QueryType.PPL));
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
  public void sqlQueryRoutesToAnalyticsForPluggableIndex() {
    registerIndex(
        "parquet_logs",
        Settings.builder()
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "composite")
            .build());

    assertTrue(action.isAnalyticsIndex("SELECT * FROM parquet_logs", QueryType.SQL));
    assertTrue(
        action.isAnalyticsIndex(
            "SELECT ts, level FROM parquet_logs WHERE level = 'ERROR'", QueryType.SQL));
  }

  @Test
  public void sqlQueryWithSchemaRoutesToAnalytics() {
    registerIndex(
        "parquet_logs",
        Settings.builder()
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "composite")
            .build());

    assertTrue(action.isAnalyticsIndex("SELECT * FROM opensearch.parquet_logs", QueryType.SQL));
  }

  @Test
  public void sqlQueryRoutesToLuceneForNonPluggableIndex() {
    registerIndex("plain_logs", Settings.EMPTY);

    assertFalse(action.isAnalyticsIndex("SELECT * FROM plain_logs", QueryType.SQL));
  }

  @Test
  public void sqlQueryRoutesToLuceneForMissingIndex() {
    assertFalse(action.isAnalyticsIndex("SELECT * FROM does_not_exist", QueryType.SQL));
  }

  @Test
  public void nullAndEmptyQueriesRouteToLucene() {
    assertFalse(action.isAnalyticsIndex(null, QueryType.PPL));
    assertFalse(action.isAnalyticsIndex("", QueryType.PPL));
    assertFalse(action.isAnalyticsIndex(null, QueryType.SQL));
    assertFalse(action.isAnalyticsIndex("", QueryType.SQL));
  }

  @Test
  public void showStatementNotRoutedToAnalyticsEngine() {
    registerIndex(
        "parquet_logs",
        Settings.builder()
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "composite")
            .build());

    assertFalse(action.isAnalyticsIndex("SHOW TABLES LIKE 'parquet_logs'", QueryType.SQL));
  }

  @Test
  public void describeStatementNotRoutedToAnalyticsEngine() {
    registerIndex(
        "parquet_logs",
        Settings.builder()
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "composite")
            .build());

    assertFalse(action.isAnalyticsIndex("DESCRIBE TABLES LIKE 'parquet_logs'", QueryType.SQL));
  }

  @Test
  public void showStatementNotRoutedToAnalyticsEngineUnderClusterComposite() {
    enableClusterComposite();
    assertFalse(action.isAnalyticsIndex("SHOW TABLES LIKE 'parquet_logs'", QueryType.SQL));
  }

  @Test
  public void describeStatementNotRoutedToAnalyticsEngineUnderClusterComposite() {
    enableClusterComposite();
    assertFalse(action.isAnalyticsIndex("DESCRIBE TABLES LIKE 'parquet_logs'", QueryType.SQL));
  }

  @Test
  public void dataQueryStillRoutesToAnalyticsUnderClusterComposite() {
    enableClusterComposite();
    assertTrue(action.isAnalyticsIndex("SELECT * FROM parquet_logs", QueryType.SQL));
  }

  @Test
  public void unparseableQueryRoutesToAnalyticsUnderClusterComposite() {
    enableClusterComposite();
    // malformed -> AE re-parses & reports
    assertTrue(action.isAnalyticsIndex("SELECT FROM WHERE", QueryType.SQL));
  }

  @Test
  public void legacyShowNotRoutedToAnalyticsEngineUnderClusterComposite() {
    enableClusterComposite();
    // unquoted LIKE is rejected by the V2 parser, but still belongs on the default pipeline
    assertFalse(action.isAnalyticsIndex("SHOW TABLES LIKE %", QueryType.SQL));
  }

  @Test
  public void legacyDescribeNotRoutedToAnalyticsEngineUnderClusterComposite() {
    enableClusterComposite();
    // legacy DESCRIBE syntax is rejected by the V2 parser, but belongs on the default pipeline
    assertFalse(action.isAnalyticsIndex("DESCRIBE my_index", QueryType.SQL));
  }

  @Test
  public void pplDescribeNotRoutedToAnalyticsEngineUnderClusterComposite() {
    enableClusterComposite();
    assertFalse(action.isAnalyticsIndex("describe parquet_logs", QueryType.PPL));
  }

  @Test
  public void pplShowDatasourcesNotRoutedToAnalyticsEngineUnderClusterComposite() {
    enableClusterComposite();
    assertFalse(action.isAnalyticsIndex("show datasources", QueryType.PPL));
  }

  @Test
  public void pplDataQueryStillRoutesToAnalyticsUnderClusterComposite() {
    enableClusterComposite();
    assertTrue(action.isAnalyticsIndex("source = parquet_logs | fields ts", QueryType.PPL));
  }

  @Test
  public void pplUnparseableQueryRoutesToAnalyticsUnderClusterComposite() {
    enableClusterComposite();
    // malformed -> AE re-parses & reports
    assertTrue(action.isAnalyticsIndex("source = parquet_logs | | fields ts", QueryType.PPL));
  }

  private void enableClusterComposite() {
    when(clusterService.getSettings())
        .thenReturn(
            Settings.builder()
                .put(
                    IndicesService.CLUSTER_PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "composite")
                .build());
  }

  private void registerIndex(String name, Settings settings) {
    IndexMetadata indexMetadata = mock(IndexMetadata.class);
    when(indexMetadata.getSettings()).thenReturn(settings);
    when(metadata.index(name)).thenReturn(indexMetadata);
  }
}
