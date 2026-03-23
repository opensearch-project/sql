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

  // --- isUnifiedQueryPath ---

  @Test
  public void isUnifiedQueryPath_parquetPrefix() {
    assertTrue(RestUnifiedQueryAction.isUnifiedQueryPath("source = parquet_logs | fields ts"));
  }

  @Test
  public void isUnifiedQueryPath_parquetPrefixNoSpaces() {
    assertTrue(RestUnifiedQueryAction.isUnifiedQueryPath("source=parquet_logs | fields ts"));
  }

  @Test
  public void isUnifiedQueryPath_parquetPrefixWithCatalog() {
    assertTrue(
        RestUnifiedQueryAction.isUnifiedQueryPath("source = opensearch.parquet_logs | fields ts"));
  }

  @Test
  public void isUnifiedQueryPath_nonParquetIndex() {
    assertFalse(RestUnifiedQueryAction.isUnifiedQueryPath("source = my_logs | fields ts"));
  }

  @Test
  public void isUnifiedQueryPath_nullQuery() {
    assertFalse(RestUnifiedQueryAction.isUnifiedQueryPath(null));
  }

  @Test
  public void isUnifiedQueryPath_emptyQuery() {
    assertFalse(RestUnifiedQueryAction.isUnifiedQueryPath(""));
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
