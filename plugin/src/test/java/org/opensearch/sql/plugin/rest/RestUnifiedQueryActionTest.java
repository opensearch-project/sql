/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class RestUnifiedQueryActionTest {

  // --- isAnalyticsIndex ---

  @Test
  public void isAnalyticsIndex_parquetPrefix() {
    assertTrue(RestUnifiedQueryAction.isAnalyticsIndex("source = parquet_logs | fields ts"));
  }

  @Test
  public void isAnalyticsIndex_parquetPrefixNoSpaces() {
    assertTrue(RestUnifiedQueryAction.isAnalyticsIndex("source=parquet_logs | fields ts"));
  }

  @Test
  public void isAnalyticsIndex_parquetPrefixWithCatalog() {
    assertTrue(
        RestUnifiedQueryAction.isAnalyticsIndex("source = opensearch.parquet_logs | fields ts"));
  }

  @Test
  public void isAnalyticsIndex_nonParquetIndex() {
    assertFalse(RestUnifiedQueryAction.isAnalyticsIndex("source = my_logs | fields ts"));
  }

  @Test
  public void isAnalyticsIndex_nullQuery() {
    assertFalse(RestUnifiedQueryAction.isAnalyticsIndex(null));
  }

  @Test
  public void isAnalyticsIndex_emptyQuery() {
    assertFalse(RestUnifiedQueryAction.isAnalyticsIndex(""));
  }

  // --- extractIndexName ---

  @Test
  public void extractIndexName_simpleSource() {
    assertEquals("my_logs", RestUnifiedQueryAction.extractIndexName("source = my_logs"));
  }

  @Test
  public void extractIndexName_noSpaces() {
    assertEquals("my_logs", RestUnifiedQueryAction.extractIndexName("source=my_logs"));
  }

  @Test
  public void extractIndexName_withPipe() {
    assertEquals(
        "my_logs",
        RestUnifiedQueryAction.extractIndexName("source = my_logs | where status = 200"));
  }

  @Test
  public void extractIndexName_backticks() {
    assertEquals(
        "my_logs", RestUnifiedQueryAction.extractIndexName("source = `my_logs` | fields ts"));
  }

  @Test
  public void extractIndexName_qualifiedName() {
    assertEquals(
        "opensearch.my_logs",
        RestUnifiedQueryAction.extractIndexName("source = opensearch.my_logs | fields ts"));
  }

  @Test
  public void extractIndexName_caseInsensitive() {
    assertEquals("my_logs", RestUnifiedQueryAction.extractIndexName("SOURCE = my_logs"));
  }

  @Test
  public void extractIndexName_noSourceClause() {
    assertNull(RestUnifiedQueryAction.extractIndexName("describe my_logs"));
  }

  @Test
  public void extractIndexName_wildcardIndex() {
    assertEquals("parquet_*", RestUnifiedQueryAction.extractIndexName("source = parquet_*"));
  }
}
