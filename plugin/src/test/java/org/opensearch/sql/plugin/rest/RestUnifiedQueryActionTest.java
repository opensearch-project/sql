/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.calcite.rel.RelNode;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.schema.SchemaProvider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Tests for analytics index routing in RestUnifiedQueryAction. Uses context parser for AST-based
 * index name extraction.
 */
public class RestUnifiedQueryActionTest {

  private RestUnifiedQueryAction action;

  @Before
  public void setUp() {
    @SuppressWarnings("unchecked")
    QueryPlanExecutor<RelNode, Iterable<Object[]>> executor = mock(QueryPlanExecutor.class);
    action =
        new RestUnifiedQueryAction(
            mock(NodeClient.class),
            mock(ClusterService.class),
            executor,
            mock(SchemaProvider.class));
  }

  @Test
  public void parquetIndexRoutesToAnalytics() {
    assertTrue(action.isAnalyticsIndex("source = parquet_logs | fields ts", QueryType.PPL));
    assertTrue(
        action.isAnalyticsIndex("source = opensearch.parquet_logs | fields ts", QueryType.PPL));
  }

  @Test
  public void nonParquetIndexRoutesToLucene() {
    assertFalse(action.isAnalyticsIndex("source = my_logs | fields ts", QueryType.PPL));
    assertFalse(action.isAnalyticsIndex(null, QueryType.PPL));
    assertFalse(action.isAnalyticsIndex("", QueryType.PPL));
  }
}
