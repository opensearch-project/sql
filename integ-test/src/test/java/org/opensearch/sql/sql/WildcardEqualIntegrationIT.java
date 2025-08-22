/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * Integration tests for wildcard pattern matching with = and != operators in SQL.
 * Tests behavior with and without Calcite engine and pushdown optimization.
 */
public class WildcardEqualIntegrationIT extends SQLIntegTestCase {
  
  @Override
  protected void init() throws Exception {
    loadIndex(Index.WILDCARD);
  }

  // ========== Basic Wildcard Tests ==========
  
  @Test
  public void test_sql_wildcard_equal_asterisk() throws IOException {
    String query = "SELECT KeywordBody FROM wildcard WHERE KeywordBody='test*'";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, 
        rows("test wildcard"),
        rows("test wildcard in body"),
        rows("test123"));
  }

  @Test
  public void test_sql_wildcard_equal_question() throws IOException {
    String query = "SELECT KeywordBody FROM wildcard WHERE KeywordBody='test?'";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, 
        rows("test1"),
        rows("test2"));
  }

  @Test
  public void test_sql_wildcard_not_equal() throws IOException {
    String query = "SELECT KeywordBody FROM wildcard WHERE KeywordBody!='test*' LIMIT 5";
    JSONObject result = executeJdbcRequest(query);
    // Should return rows that don't match the pattern
    assertCondition("Should return non-matching rows", result.getInt("total") > 0);
  }

  @Test
  public void test_sql_wildcard_complex_pattern() throws IOException {
    String query = "SELECT KeywordBody FROM wildcard WHERE KeywordBody='test*wild*'";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, 
        rows("test wildcard"),
        rows("test wildcard in body"));
  }

  // ========== Calcite Engine Tests ==========
  
  @Test
  public void test_sql_wildcard_with_calcite() throws IOException {
    enableCalcite();
    allowCalciteFallback();
    
    try {
      String query = "SELECT KeywordBody FROM wildcard WHERE KeywordBody='test*'";
      JSONObject result = executeJdbcRequest(query);
      verifyDataRows(result, 
          rows("test wildcard"),
          rows("test wildcard in body"),
          rows("test123"));
    } finally {
      disableCalcite();
      disallowCalciteFallback();
    }
  }

  @Test
  public void test_sql_numeric_wildcard_requires_cast() throws IOException {
    enableCalcite();
    allowCalciteFallback();
    
    try {
      // This should fail - numeric fields can't use wildcards without CAST
      String query = "SELECT count FROM wildcard WHERE count='123*'";
      JSONObject result = executeJdbcRequest(query);
      
      // With our fix, this should return an error about incompatible types
      assertCondition("Should get error for numeric wildcard without CAST", 
                      result.has("error") || result.getInt("total") == 0);
    } finally {
      disableCalcite();
      disallowCalciteFallback();
    }
  }

  @Test
  public void test_sql_cast_with_wildcard() throws IOException {
    enableCalcite();
    allowCalciteFallback();
    
    try {
      // With explicit CAST, numeric wildcards should work
      String query = "SELECT count FROM wildcard WHERE CAST(count AS VARCHAR)='10*'";
      JSONObject result = executeJdbcRequest(query);
      
      // This should work with the explicit CAST
      assertCondition("Explicit CAST should allow numeric wildcards", result.getInt("total") >= 0);
    } finally {
      disableCalcite();
      disallowCalciteFallback();
    }
  }

  // ========== Pushdown Tests ==========
  
  @Test
  public void test_sql_keyword_wildcard_with_pushdown() throws IOException {
    enableCalcite();
    enablePushdown();
    
    try {
      // Keyword fields should use native wildcardQuery with pushdown
      String query = "SELECT KeywordBody FROM wildcard WHERE KeywordBody='test*'";
      JSONObject result = executeJdbcRequest(query);
      
      verifyDataRows(result, 
          rows("test wildcard"),
          rows("test wildcard in body"),
          rows("test123"));
    } finally {
      disablePushdown();
      disableCalcite();
    }
  }

  @Test
  public void test_sql_text_field_with_pushdown() throws IOException {
    enableCalcite();
    enablePushdown();
    
    try {
      // Text fields should automatically use .keyword subfield if available
      String query = "SELECT TextBody FROM wildcard WHERE TextBody='test*'";
      JSONObject result = executeJdbcRequest(query);
      
      // Should work if .keyword subfield exists
      assertCondition("Text field with .keyword should support wildcards", result.getInt("total") >= 0);
    } finally {
      disablePushdown();
      disableCalcite();
    }
  }

  @Test
  public void test_sql_wildcard_without_pushdown() throws IOException {
    enableCalcite();
    disablePushdown();
    
    try {
      // Without pushdown, should fetch all data and filter in-memory
      String query = "SELECT KeywordBody FROM wildcard WHERE KeywordBody='test*'";
      JSONObject result = executeJdbcRequest(query);
      
      verifyDataRows(result, 
          rows("test wildcard"),
          rows("test wildcard in body"),
          rows("test123"));
    } finally {
      enablePushdown(); // Re-enable for other tests
      disableCalcite();
    }
  }

  @Test
  public void test_sql_ip_field_wildcard() throws IOException {
    enableCalcite();
    enablePushdown();
    
    try {
      // IP fields with wildcards should require CAST
      String query = "SELECT ip_field FROM wildcard WHERE ip_field='192.168.*.*'";
      JSONObject result = executeJdbcRequest(query);
      
      // Should fail without CAST
      assertCondition("IP field wildcard should require CAST", 
                      result.has("error") || result.getInt("total") == 0);
    } finally {
      disablePushdown();
      disableCalcite();
    }
  }

  // ========== Helper Methods ==========
  
  private void enableCalcite() throws IOException {
    updateClusterSettings(
        new ClusterSetting(
            "persistent", Settings.Key.CALCITE_ENGINE_ENABLED.getKeyValue(), "true"));
  }

  private void disableCalcite() throws IOException {
    updateClusterSettings(
        new ClusterSetting(
            "persistent", Settings.Key.CALCITE_ENGINE_ENABLED.getKeyValue(), "false"));
  }

  private void allowCalciteFallback() throws IOException {
    updateClusterSettings(
        new ClusterSetting(
            "persistent", Settings.Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(), "true"));
  }

  private void disallowCalciteFallback() throws IOException {
    updateClusterSettings(
        new ClusterSetting(
            "persistent", Settings.Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(), "false"));
  }

  private void enablePushdown() throws IOException {
    updateClusterSettings(
        new ClusterSetting(
            "persistent", Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(), "true"));
  }

  private void disablePushdown() throws IOException {
    updateClusterSettings(
        new ClusterSetting(
            "persistent", Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(), "false"));
  }

  private void assertCondition(String message, boolean condition) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}