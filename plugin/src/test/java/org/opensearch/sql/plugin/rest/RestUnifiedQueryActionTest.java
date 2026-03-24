/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests for analytics index routing in RestUnifiedQueryAction. Index name extraction will be
 * replaced by UnifiedQueryParser -- these tests focus on routing behavior only.
 */
public class RestUnifiedQueryActionTest {

  @Test
  public void parquetIndexRoutesToAnalytics() {
    assertTrue(RestUnifiedQueryAction.isAnalyticsIndex("source = parquet_logs | fields ts"));
    assertTrue(
        RestUnifiedQueryAction.isAnalyticsIndex("source = opensearch.parquet_logs | fields ts"));
  }

  @Test
  public void nonParquetIndexRoutesToLucene() {
    assertFalse(RestUnifiedQueryAction.isAnalyticsIndex("source = my_logs | fields ts"));
    assertFalse(RestUnifiedQueryAction.isAnalyticsIndex(null));
    assertFalse(RestUnifiedQueryAction.isAnalyticsIndex(""));
  }
}
