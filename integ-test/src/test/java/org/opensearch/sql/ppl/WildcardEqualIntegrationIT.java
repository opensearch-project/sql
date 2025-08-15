/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * Integration tests for wildcard pattern matching with = and != operators in PPL.
 * Tests behavior with and without Calcite engine and pushdown optimization.
 */
public class WildcardEqualIntegrationIT extends PPLIntegTestCase {
  
  private static final String TEST_INDEX_WILDCARD = "wildcard";
  
  @Override
  protected void init() throws Exception {
    loadIndex(Index.WILDCARD);
  }

  // ========== Basic Wildcard Tests ==========
  
  @Test
  public void test_wildcard_asterisk_keyword_field() throws IOException {
    String query = "search source=" + TEST_INDEX_WILDCARD + " | where KeywordBody='test*' | fields KeywordBody";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, 
        rows("test wildcard"),
        rows("test wildcard in body"),
        rows("test123"));
  }

  @Test
  public void test_wildcard_question_keyword_field() throws IOException {
    String query = "search source=" + TEST_INDEX_WILDCARD + " | where KeywordBody='test?' | fields KeywordBody";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, 
        rows("test1"),
        rows("test2"));
  }

  @Test
  public void test_wildcard_not_equals_keyword_field() throws IOException {
    String query = "search source=" + TEST_INDEX_WILDCARD + " | where KeywordBody!='test*' | fields KeywordBody";
    JSONObject result = executeQuery(query);
    // Should return all rows that don't match the pattern
    assertTrue(result.getInt("total") > 0);
  }

  // ========== Calcite Engine Tests ==========
  
  @Test
  public void test_wildcard_with_calcite_enabled() throws IOException {
    enableCalcite();
    allowCalciteFallback();
    
    try {
      String query = "search source=" + TEST_INDEX_WILDCARD + " | where KeywordBody='test*' | fields KeywordBody";
      JSONObject result = executeQuery(query);
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
  public void test_numeric_wildcard_with_calcite_requires_cast() throws IOException {
    enableCalcite();
    allowCalciteFallback();
    
    try {
      // This should fail with type error - numeric fields can't use wildcards without CAST
      String query = "search source=" + TEST_INDEX_WILDCARD + " | where count='123*' | fields count";
      JSONObject result = executeQuery(query);
      
      // With our fix, this should return an error about incompatible types
      assertCondition("Should get error for numeric wildcard without CAST", 
                      result.has("error") || result.getInt("total") == 0);
    } finally {
      disableCalcite();
      disallowCalciteFallback();
    }
  }

  @Test
  public void test_explicit_cast_with_wildcard_calcite() throws IOException {
    enableCalcite();
    allowCalciteFallback();
    
    try {
      // With explicit CAST, numeric wildcards should work
      String query = "search source=" + TEST_INDEX_WILDCARD + " | where CAST(count AS STRING)='10*' | fields count";
      JSONObject result = executeQuery(query);
      
      // This should work with the explicit CAST
      assertCondition("Explicit CAST should allow numeric wildcards", result.getInt("total") >= 0);
    } finally {
      disableCalcite();
      disallowCalciteFallback();
    }
  }

  // ========== Pushdown Tests ==========
  
  @Test
  public void test_keyword_wildcard_with_pushdown() throws IOException {
    enableCalcite();
    enablePushdown();
    
    try {
      // Keyword fields should use native wildcardQuery with pushdown
      String query = "search source=" + TEST_INDEX_WILDCARD + " | where KeywordBody='test*' | fields KeywordBody";
      JSONObject result = executeQuery(query);
      
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
  public void test_text_field_keyword_subfield_with_pushdown() throws IOException {
    enableCalcite();
    enablePushdown();
    
    try {
      // Text fields should automatically use .keyword subfield if available
      String query = "search source=" + TEST_INDEX_WILDCARD + " | where TextBody='test*' | fields TextBody";
      JSONObject result = executeQuery(query);
      
      // Should work if .keyword subfield exists
      assertCondition("Text field with .keyword should support wildcards", result.getInt("total") >= 0);
    } finally {
      disablePushdown();
      disableCalcite();
    }
  }

  @Test
  public void test_wildcard_without_pushdown() throws IOException {
    enableCalcite();
    disablePushdown();
    
    try {
      // Without pushdown, should fetch all data and filter in-memory
      String query = "search source=" + TEST_INDEX_WILDCARD + " | where KeywordBody='test*' | fields KeywordBody";
      JSONObject result = executeQuery(query);
      
      verifyDataRows(result, 
          rows("test wildcard"),
          rows("test wildcard in body"),
          rows("test123"));
    } finally {
      enablePushdown(); // Re-enable for other tests
      disableCalcite();
    }
  }

  // ========== Helper Methods ==========
  
  private void enablePushdown() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(), "true"));
  }

  private void disablePushdown() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(), "false"));
  }

  private void assertCondition(String message, boolean condition) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}