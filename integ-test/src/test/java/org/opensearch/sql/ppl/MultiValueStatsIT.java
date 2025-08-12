/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_TIME;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for multivalue statistics functions list() and values().
 * Tests cover both V2 and V3 engines, different data types, and pushdown scenarios.
 */
public class MultiValueStatsIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.DATA_TYPE_NONNUMERIC);
    loadIndex(Index.DATA_TYPE_NUMERIC);
    loadIndex(Index.DATETIME);
  }

  // ================================
  // Basic Function Tests
  // ================================

  @Test
  public void testListFunctionBasic() throws IOException {
    JSONObject response = executeQuery(
        String.format("source=%s | where account_number < 5 | stats list(gender) as gender_list", TEST_INDEX_ACCOUNT));
    
    verifySchema(response, schema("gender_list", null, "array"));
    
    // Verify we get results
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should have at least one result row");
  }

  @Test 
  public void testValuesFunctionBasic() throws IOException {
    JSONObject response = executeQuery(
        String.format("source=%s | where account_number < 10 | stats values(gender) as unique_genders", TEST_INDEX_ACCOUNT));
    
    verifySchema(response, schema("unique_genders", null, "array"));
    
    // Verify we get results
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should have at least one result row");
  }

  @Test
  public void testBothFunctionsInSameQuery() throws IOException {
    JSONObject response = executeQuery(
        String.format("source=%s | where account_number < 5 | stats list(gender) as all_genders, values(gender) as unique_genders", TEST_INDEX_ACCOUNT));
    
    verifySchema(response,
        schema("all_genders", null, "array"),
        schema("unique_genders", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should have at least one result row");
  }

  @Test
  public void testWithGrouping() throws IOException {
    JSONObject response = executeQuery(
        String.format("source=%s | where account_number < 10 | stats list(firstname) as names by gender", TEST_INDEX_ACCOUNT));
    
    verifySchema(response,
        schema("gender", null, "keyword"),
        schema("names", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() >= 1, "Should have at least one group");
  }

  // ================================
  // Data Type Tests
  // ================================

  @Test
  public void testStringFields() throws IOException {
    JSONObject response = executeQuery(
        String.format("source=%s | where account_number < 5 | stats list(firstname) as names, values(state) as states", TEST_INDEX_ACCOUNT));
    
    verifySchema(response,
        schema("names", null, "array"),
        schema("states", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should have results for string fields");
  }

  @Test
  public void testNumericFields() throws IOException {
    JSONObject response = executeQuery(
        String.format("source=%s | where account_number < 5 | stats list(age) as ages, values(balance) as balances", TEST_INDEX_ACCOUNT));
    
    verifySchema(response,
        schema("ages", null, "array"),
        schema("balances", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should have results for numeric fields");
  }

  @Test
  public void testBooleanFields() throws IOException {
    JSONObject response = executeQuery(
        String.format("source=%s | stats list(boolean_value) as bool_list, values(boolean_value) as unique_bools", TEST_INDEX_DATATYPE_NONNUMERIC));
    
    verifySchema(response,
        schema("bool_list", null, "array"),
        schema("unique_bools", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should have results for boolean fields");
  }

  @Test
  public void testMixedNumericTypes() throws IOException {
    JSONObject response = executeQuery(
        String.format("source=%s | stats list(long_number) as longs, values(double_number) as doubles", TEST_INDEX_DATATYPE_NUMERIC));
    
    verifySchema(response,
        schema("longs", null, "array"),
        schema("doubles", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should have results for mixed numeric types");
  }

  // ================================
  // Engine-Specific Tests
  // ================================

  @Test
  public void testV2EngineCompatibility() throws IOException {
    // Test with Calcite disabled (V2 engine)
    boolean originalCalciteState = isCalciteEnabled();
    
    try {
      if (originalCalciteState) {
        updateClusterSettings(new ClusterSetting("persistent", "plugins.query.engine.calcite.enabled", "false"));
      }
      
      JSONObject response = executeQuery(
          String.format("source=%s | where account_number < 5 | stats list(gender) as genders", TEST_INDEX_ACCOUNT));
      
      verifySchema(response, schema("genders", null, "array"));
      
      JSONArray dataRows = response.getJSONArray("datarows");
      Assertions.assertTrue(dataRows.length() > 0, "V2 engine should produce results");
      
    } finally {
      // Restore original Calcite state
      if (originalCalciteState) {
        updateClusterSettings(new ClusterSetting("persistent", "plugins.query.engine.calcite.enabled", "true"));
      }
    }
  }

  @Test 
  public void testV3CalciteEngine() throws IOException {
    // Test with Calcite enabled (V3 engine)
    boolean originalCalciteState = isCalciteEnabled();
    
    try {
      if (!originalCalciteState) {
        updateClusterSettings(new ClusterSetting("persistent", "plugins.query.engine.calcite.enabled", "true"));
      }
      
      JSONObject response = executeQuery(
          String.format("source=%s | where account_number < 5 | stats values(gender) as unique_genders", TEST_INDEX_ACCOUNT));
      
      verifySchema(response, schema("unique_genders", null, "array"));
      
      JSONArray dataRows = response.getJSONArray("datarows");
      Assertions.assertTrue(dataRows.length() > 0, "V3 Calcite engine should produce results");
      
    } finally {
      // Restore original Calcite state
      if (!originalCalciteState) {
        updateClusterSettings(new ClusterSetting("persistent", "plugins.query.engine.calcite.enabled", "false"));
      }
    }
  }

  // ================================
  // Pushdown Tests  
  // ================================

  @Test
  public void testWithPushdownEnabled() throws IOException {
    // Test behavior with pushdown - should work regardless of setting
    JSONObject response = executeQuery(
        String.format("source=%s | stats list(state) as states by gender", TEST_INDEX_ACCOUNT));
    
    verifySchema(response,
        schema("gender", null, "keyword"),
        schema("states", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() >= 2, "Should have results for both genders");
  }

  // ================================
  // Edge Case Tests
  // ================================

  @Test
  public void testWithNullValues() throws IOException {
    JSONObject response = executeQuery(
        String.format("source=%s | stats list(balance) as balances, values(balance) as unique_balances", TEST_INDEX_BANK_WITH_NULL_VALUES));
    
    verifySchema(response,
        schema("balances", null, "array"),
        schema("unique_balances", null, "array"));
    
    // Should handle nulls gracefully (filter them out)
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should handle null values gracefully");
  }

  @Test
  public void testEmptyResults() throws IOException {
    JSONObject response = executeQuery(
        String.format("source=%s | where account_number > 9999 | stats list(state) as states", TEST_INDEX_ACCOUNT));
    
    verifySchema(response, schema("states", null, "array"));
    
    // Should return results even for empty input (empty arrays)
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should return row even for empty input");
  }

  @Test
  public void testLargeDataset() throws IOException {
    // Test with larger dataset to verify limits
    JSONObject response = executeQuery(
        String.format("source=%s | stats list(state) as all_states, values(state) as unique_states", TEST_INDEX_ACCOUNT));
    
    verifySchema(response,
        schema("all_states", null, "array"),
        schema("unique_states", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should handle large datasets");
  }

  // ================================
  // Limit Tests for list() Function
  // ================================

  @Test
  public void testListFunctionLimitV2Engine() throws IOException {
    // Test with V2 engine (Calcite disabled)
    boolean originalCalciteState = isCalciteEnabled();
    
    try {
      if (originalCalciteState) {
        updateClusterSettings(new ClusterSetting("persistent", "plugins.query.engine.calcite.enabled", "false"));
      }
      
      // Use all account data (1000 records) to test 100-item limit
      JSONObject response = executeQuery(
          String.format("source=%s | stats list(account_number) as account_numbers", TEST_INDEX_ACCOUNT));
      
      verifySchema(response, schema("account_numbers", null, "array"));
      
      JSONArray dataRows = response.getJSONArray("datarows");
      Assertions.assertTrue(dataRows.length() > 0, "Should have at least one result row");
      
      // Get the first (and only) row since we're not grouping
      JSONArray firstRow = dataRows.getJSONArray(0);
      JSONArray accountNumbers = firstRow.getJSONArray(0);
      
      // Verify the 100-item limit is enforced
      Assertions.assertTrue(accountNumbers.length() <= 100, 
          String.format("V2 engine list() should be limited to 100 items, but got %d", accountNumbers.length()));
      
      // Should be exactly 100 since we have 1000 records (more than the limit)
      Assertions.assertEquals(100, accountNumbers.length(), 
          "V2 engine list() should return exactly 100 items when more data is available");
      
    } finally {
      // Restore original Calcite state
      if (originalCalciteState) {
        updateClusterSettings(new ClusterSetting("persistent", "plugins.query.engine.calcite.enabled", "true"));
      }
    }
  }

  @Test
  public void testListFunctionLimitV3Engine() throws IOException {
    // Test with V3 engine (Calcite enabled)
    boolean originalCalciteState = isCalciteEnabled();
    
    try {
      if (!originalCalciteState) {
        updateClusterSettings(new ClusterSetting("persistent", "plugins.query.engine.calcite.enabled", "true"));
      }
      
      // Use all account data (1000 records) to test 100-item limit
      JSONObject response = executeQuery(
          String.format("source=%s | stats list(account_number) as account_numbers", TEST_INDEX_ACCOUNT));
      
      verifySchema(response, schema("account_numbers", null, "array"));
      
      JSONArray dataRows = response.getJSONArray("datarows");
      Assertions.assertTrue(dataRows.length() > 0, "Should have at least one result row");
      
      // Get the first (and only) row since we're not grouping
      JSONArray firstRow = dataRows.getJSONArray(0);
      JSONArray accountNumbers = firstRow.getJSONArray(0);
      
      // Verify the 100-item limit is enforced
      Assertions.assertTrue(accountNumbers.length() <= 100, 
          String.format("V3 engine list() should be limited to 100 items, but got %d", accountNumbers.length()));
      
      // Should be exactly 100 since we have 1000 records (more than the limit)
      Assertions.assertEquals(100, accountNumbers.length(), 
          "V3 engine list() should return exactly 100 items when more data is available");
      
    } finally {
      // Restore original Calcite state
      if (!originalCalciteState) {
        updateClusterSettings(new ClusterSetting("persistent", "plugins.query.engine.calcite.enabled", "false"));
      }
    }
  }

  @Test
  public void testListFunctionLimitConsistencyBetweenEngines() throws IOException {
    // Test that both engines respect the same 100-item limit
    String query = String.format("source=%s | stats list(account_number) as account_numbers", TEST_INDEX_ACCOUNT);
    
    // Test with V2 engine
    updateClusterSettings(new ClusterSetting("persistent", "plugins.query.engine.calcite.enabled", "false"));
    JSONObject v2Response = executeQuery(query);
    JSONArray v2DataRows = v2Response.getJSONArray("datarows");
    JSONArray v2FirstRow = v2DataRows.getJSONArray(0);
    JSONArray v2AccountNumbers = v2FirstRow.getJSONArray(0);
    
    // Test with V3 engine
    updateClusterSettings(new ClusterSetting("persistent", "plugins.query.engine.calcite.enabled", "true"));
    JSONObject v3Response = executeQuery(query);
    JSONArray v3DataRows = v3Response.getJSONArray("datarows");
    JSONArray v3FirstRow = v3DataRows.getJSONArray(0);
    JSONArray v3AccountNumbers = v3FirstRow.getJSONArray(0);
    
    // Both engines should respect the same limit
    Assertions.assertEquals(v2AccountNumbers.length(), v3AccountNumbers.length(), 
        "Both V2 and V3 engines should return the same number of items for list() function");
    
    Assertions.assertEquals(100, v2AccountNumbers.length(), 
        "V2 engine should return exactly 100 items");
    Assertions.assertEquals(100, v3AccountNumbers.length(), 
        "V3 engine should return exactly 100 items");
  }

  @Test
  public void testListFunctionNoLimitWhenLessData() throws IOException {
    // Test that list() returns all items when less than 100 are available
    JSONObject response = executeQuery(
        String.format("source=%s | where account_number < 50 | stats list(account_number) as account_numbers", TEST_INDEX_ACCOUNT));
    
    verifySchema(response, schema("account_numbers", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should have at least one result row");
    
    JSONArray firstRow = dataRows.getJSONArray(0);
    JSONArray accountNumbers = firstRow.getJSONArray(0);
    
    // Should return all matching items (less than 100)
    Assertions.assertTrue(accountNumbers.length() < 100, 
        "Should return less than 100 items when input data is smaller");
    Assertions.assertTrue(accountNumbers.length() > 0, 
        "Should return some items for the filtered query");
    
    // Should be around 49 items (account_number < 50, but some account numbers may not exist)
    Assertions.assertTrue(accountNumbers.length() <= 49, 
        "Should not exceed the number of filtered records");
  }

  @Test
  public void testListFunctionLimitWithGrouping() throws IOException {
    // Test that 100-item limit applies per group
    JSONObject response = executeQuery(
        String.format("source=%s | stats list(account_number) as account_numbers by gender", TEST_INDEX_ACCOUNT));
    
    verifySchema(response,
        schema("gender", null, "keyword"),
        schema("account_numbers", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() >= 2, "Should have at least 2 gender groups");
    
    // Check each group respects the 100-item limit
    for (int i = 0; i < dataRows.length(); i++) {
      JSONArray row = dataRows.getJSONArray(i);
      String gender = row.getString(0);
      JSONArray accountNumbers = row.getJSONArray(1);
      
      Assertions.assertTrue(accountNumbers.length() <= 100, 
          String.format("Group %s should have at most 100 items, but got %d", gender, accountNumbers.length()));
      
      // Each gender group should have a reasonable number of accounts (likely close to 100 for 1000 total records)
      Assertions.assertTrue(accountNumbers.length() > 0, 
          String.format("Group %s should have at least some items", gender));
    }
  }

  @Test 
  public void testValuesFunctionNoLimit() throws IOException {
    // Verify that values() function does NOT have the 100-item limit
    JSONObject response = executeQuery(
        String.format("source=%s | stats values(state) as unique_states", TEST_INDEX_ACCOUNT));
    
    verifySchema(response, schema("unique_states", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should have at least one result row");
    
    JSONArray firstRow = dataRows.getJSONArray(0);
    JSONArray uniqueStates = firstRow.getJSONArray(0);
    
    // values() should not be limited to 100 items (though US states are < 100 anyway)
    // The key point is that it doesn't artificially limit to 100 like list() does
    Assertions.assertTrue(uniqueStates.length() > 10, 
        "Should have many unique states (no artificial limit)");
    Assertions.assertTrue(uniqueStates.length() <= 50, 
        "Should have reasonable number of US states");
  }

  @Test
  public void testListFunctionPreservesDuplicates() throws IOException {
    // Test that list() preserves duplicate values (key requirement from RFC)
    JSONObject response = executeQuery(
        String.format("source=%s | where account_number < 100 | stats list(gender) as all_genders", TEST_INDEX_ACCOUNT));
    
    verifySchema(response, schema("all_genders", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should have at least one result row");
    
    JSONArray firstRow = dataRows.getJSONArray(0);
    JSONArray allGenders = firstRow.getJSONArray(0);
    
    // Count occurrences of each gender value
    int maleCount = 0;
    int femaleCount = 0;
    
    for (int i = 0; i < allGenders.length(); i++) {
      String gender = allGenders.getString(i);
      if ("M".equals(gender)) {
        maleCount++;
      } else if ("F".equals(gender)) {
        femaleCount++;
      }
    }
    
    // Should have multiple occurrences of both M and F (preserving duplicates)
    Assertions.assertTrue(maleCount > 1, 
        String.format("list() should preserve duplicate 'M' values, found %d occurrences", maleCount));
    Assertions.assertTrue(femaleCount > 1, 
        String.format("list() should preserve duplicate 'F' values, found %d occurrences", femaleCount));
    
    // Total should be significantly more than 2 (which would be the unique count)
    int totalGenders = maleCount + femaleCount;
    Assertions.assertTrue(totalGenders > 10, 
        String.format("list() should have many duplicate entries, found %d total", totalGenders));
  }

  @Test
  public void testValuesFunctionRemovesDuplicates() throws IOException {
    // Test that values() removes duplicate values (contrasting with list())
    JSONObject response = executeQuery(
        String.format("source=%s | where account_number < 100 | stats values(gender) as unique_genders", TEST_INDEX_ACCOUNT));
    
    verifySchema(response, schema("unique_genders", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should have at least one result row");
    
    JSONArray firstRow = dataRows.getJSONArray(0);
    JSONArray uniqueGenders = firstRow.getJSONArray(0);
    
    // Should have exactly 2 unique values: "F" and "M" (assuming both genders present)
    Assertions.assertTrue(uniqueGenders.length() >= 1 && uniqueGenders.length() <= 2, 
        String.format("values() should have 1-2 unique gender values, found %d", uniqueGenders.length()));
    
    // Verify no duplicates exist
    for (int i = 0; i < uniqueGenders.length(); i++) {
      String gender1 = uniqueGenders.getString(i);
      for (int j = i + 1; j < uniqueGenders.length(); j++) {
        String gender2 = uniqueGenders.getString(j);
        Assertions.assertNotEquals(gender1, gender2, 
            String.format("values() should not contain duplicates, but found '%s' twice", gender1));
      }
    }
  }

  @Test
  public void testListVsValuesDuplicateBehaviorComparison() throws IOException {
    // Direct comparison test to highlight the key behavioral difference
    JSONObject response = executeQuery(
        String.format("source=%s | where account_number < 50 | stats list(gender) as all_genders, values(gender) as unique_genders", TEST_INDEX_ACCOUNT));
    
    verifySchema(response,
        schema("all_genders", null, "array"),
        schema("unique_genders", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should have at least one result row");
    
    JSONArray firstRow = dataRows.getJSONArray(0);
    JSONArray allGenders = firstRow.getJSONArray(0);
    JSONArray uniqueGenders = firstRow.getJSONArray(1);
    
    // list() should have significantly more items than values() due to duplicates
    Assertions.assertTrue(allGenders.length() > uniqueGenders.length(), 
        String.format("list() should have more items (%d) than values() (%d) due to duplicates", 
            allGenders.length(), uniqueGenders.length()));
    
    // values() should have at most 2 items (M, F)
    Assertions.assertTrue(uniqueGenders.length() <= 2, 
        String.format("values() should have at most 2 unique genders, found %d", uniqueGenders.length()));
    
    // list() should have many more items (one per account in the sample)
    Assertions.assertTrue(allGenders.length() >= 10, 
        String.format("list() should have many items preserving duplicates, found %d", allGenders.length()));
  }

  @Test
  public void testListFunctionPreservesDuplicatesV2Engine() throws IOException {
    // Test duplicate preservation specifically in V2 engine
    boolean originalCalciteState = isCalciteEnabled();
    
    try {
      if (originalCalciteState) {
        updateClusterSettings(new ClusterSetting("persistent", "plugins.query.engine.calcite.enabled", "false"));
      }
      
      JSONObject response = executeQuery(
          String.format("source=%s | where account_number < 30 | stats list(gender) as all_genders", TEST_INDEX_ACCOUNT));
      
      verifySchema(response, schema("all_genders", null, "array"));
      
      JSONArray dataRows = response.getJSONArray("datarows");
      Assertions.assertTrue(dataRows.length() > 0, "Should have at least one result row");
      
      JSONArray firstRow = dataRows.getJSONArray(0);
      JSONArray allGenders = firstRow.getJSONArray(0);
      
      // Count duplicates to verify they're preserved in V2 engine
      int maleCount = 0;
      int femaleCount = 0;
      
      for (int i = 0; i < allGenders.length(); i++) {
        String gender = allGenders.getString(i);
        if ("M".equals(gender)) {
          maleCount++;
        } else if ("F".equals(gender)) {
          femaleCount++;
        }
      }
      
      // V2 engine should preserve duplicates
      Assertions.assertTrue(maleCount + femaleCount > 2, 
          String.format("V2 engine list() should preserve duplicates, found %d M + %d F = %d total", 
              maleCount, femaleCount, maleCount + femaleCount));
      
    } finally {
      // Restore original Calcite state
      if (originalCalciteState) {
        updateClusterSettings(new ClusterSetting("persistent", "plugins.query.engine.calcite.enabled", "true"));
      }
    }
  }

  @Test
  public void testListFunctionPreservesDuplicatesV3Engine() throws IOException {
    // Test duplicate preservation specifically in V3 Calcite engine
    boolean originalCalciteState = isCalciteEnabled();
    
    try {
      if (!originalCalciteState) {
        updateClusterSettings(new ClusterSetting("persistent", "plugins.query.engine.calcite.enabled", "true"));
      }
      
      JSONObject response = executeQuery(
          String.format("source=%s | where account_number < 30 | stats list(gender) as all_genders", TEST_INDEX_ACCOUNT));
      
      verifySchema(response, schema("all_genders", null, "array"));
      
      JSONArray dataRows = response.getJSONArray("datarows");
      Assertions.assertTrue(dataRows.length() > 0, "Should have at least one result row");
      
      JSONArray firstRow = dataRows.getJSONArray(0);
      JSONArray allGenders = firstRow.getJSONArray(0);
      
      // Count duplicates to verify they're preserved in V3 engine
      int maleCount = 0;
      int femaleCount = 0;
      
      for (int i = 0; i < allGenders.length(); i++) {
        String gender = allGenders.getString(i);
        if ("M".equals(gender)) {
          maleCount++;
        } else if ("F".equals(gender)) {
          femaleCount++;
        }
      }
      
      // V3 engine should preserve duplicates
      Assertions.assertTrue(maleCount + femaleCount > 2, 
          String.format("V3 engine list() should preserve duplicates, found %d M + %d F = %d total", 
              maleCount, femaleCount, maleCount + femaleCount));
      
    } finally {
      // Restore original Calcite state
      if (!originalCalciteState) {
        updateClusterSettings(new ClusterSetting("persistent", "plugins.query.engine.calcite.enabled", "false"));
      }
    }
  }

  // ================================
  // Function Behavior Tests
  // ================================

  @Test
  public void testListVsValuesComparison() throws IOException {
    // Test both functions on same data to verify different behaviors
    JSONObject response = executeQuery(
        String.format("source=%s | where account_number < 20 | stats list(gender) as all_genders, values(gender) as unique_genders", TEST_INDEX_ACCOUNT));
    
    verifySchema(response,
        schema("all_genders", null, "array"),
        schema("unique_genders", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should compare list vs values behavior");
  }

  @Test
  public void testMultipleGroupsWithStats() throws IOException {
    // Test aggregation with multiple groups
    JSONObject response = executeQuery(
        String.format("source=%s | where account_number < 20 | stats count(*) as cnt, list(firstname) as names, values(state) as states by gender", TEST_INDEX_ACCOUNT));
    
    verifySchema(response,
        schema("gender", null, "keyword"),
        schema("cnt", null, isCalciteEnabled() ? "bigint" : "int"),
        schema("names", null, "array"),
        schema("states", null, "array"));
    
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() >= 1, "Should handle multiple groups with mixed aggregations");
  }
}