/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.analytics.QueryPlanExecutor;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Tests for analytics index routing in RestUnifiedQueryAction. Uses PPLQueryParser for AST-based
 * index name extraction.
 */
public class RestUnifiedQueryActionTest {

  private RestUnifiedQueryAction action;

  @Before
  public void setUp() {
    action =
        new RestUnifiedQueryAction(
            mock(NodeClient.class), mock(QueryPlanExecutor.class), mock(Settings.class));
  }

  @Test
  public void parquetIndexRoutesToAnalytics() {
    assertTrue(action.isAnalyticsIndex("source = parquet_logs | fields ts"));
    assertTrue(action.isAnalyticsIndex("source = opensearch.parquet_logs | fields ts"));
  }

  @Test
  public void nonParquetIndexRoutesToLucene() {
    assertFalse(action.isAnalyticsIndex("source = my_logs | fields ts"));
    assertFalse(action.isAnalyticsIndex(null));
    assertFalse(action.isAnalyticsIndex(""));
  }
}
