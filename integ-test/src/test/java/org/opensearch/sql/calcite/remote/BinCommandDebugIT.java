/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class BinCommandDebugIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
    
    // Load test data using existing accounts.json data
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void step1_verifyAccountsDataLoaded() throws IOException {
    System.out.println("=== STEP 1: Verify Accounts Data ===");
    
    // Test basic query to ensure data is loaded
    JSONObject result = executeQuery(String.format("source=%s | fields account_number, balance, age | head 5", TEST_INDEX_ACCOUNT));
    System.out.println("Basic query result: " + result.toString());
    
    // Check total count
    JSONObject count = executeQuery(String.format("source=%s | stats count()", TEST_INDEX_ACCOUNT));
    System.out.println("Total count: " + count.toString());
  }

  @Test
  public void step2_testBasicBinCommand() throws IOException {
    System.out.println("=== STEP 2: Test Basic Bin Command ===");
    
    // Test bin with balance and span=5000
    try {
      JSONObject result = executeQuery(String.format("source=%s | bin balance span=5000 | fields account_number, balance, balance_bin | head 5", TEST_INDEX_ACCOUNT));
      System.out.println("Bin with span=5000: " + result.toString());
    } catch (Exception e) {
      System.err.println("Error with bin span=5000: " + e.getMessage());
      e.printStackTrace();
    }
    
    // Test bin with age and span=5
    try {
      JSONObject result = executeQuery(String.format("source=%s | bin age span=5 | fields account_number, age, age_bin | head 5", TEST_INDEX_ACCOUNT));
      System.out.println("Bin age with span=5: " + result.toString());
    } catch (Exception e) {
      System.err.println("Error with bin age span=5: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Test
  public void step3_testBinWithAlias() throws IOException {
    System.out.println("=== STEP 3: Test Bin with Alias ===");
    
    try {
      JSONObject result = executeQuery(String.format("source=%s | bin balance span=10000 AS balance_bucket | fields account_number, balance, balance_bucket | head 3", TEST_INDEX_ACCOUNT));
      System.out.println("Bin with alias: " + result.toString());
    } catch (Exception e) {
      System.err.println("Error with bin alias: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Test
  public void step4_testBinWithStats() throws IOException {
    System.out.println("=== STEP 4: Test Bin with Stats ===");
    
    try {
      JSONObject result = executeQuery(String.format("source=%s | bin balance span=10000 AS balance_bucket | stats count() by balance_bucket | sort balance_bucket", TEST_INDEX_ACCOUNT));
      System.out.println("Bin with stats: " + result.toString());
    } catch (Exception e) {
      System.err.println("Error with bin stats: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Test
  public void step5_testBinWithBinsParameter() throws IOException {
    System.out.println("=== STEP 5: Test Bins Parameter ===");
    
    try {
      JSONObject result = executeQuery(String.format("source=%s | bin age bins=5 | fields account_number, age, age_bin | head 3", TEST_INDEX_ACCOUNT));
      System.out.println("Bin with bins=5: " + result.toString());
    } catch (Exception e) {
      System.err.println("Error with bins parameter: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Test
  public void step6_analyzeDataRanges() throws IOException {
    System.out.println("=== STEP 6: Analyze Data Ranges ===");
    
    // Get min/max balance
    try {
      JSONObject minBalance = executeQuery(String.format("source=%s | stats min(balance) as min_balance", TEST_INDEX_ACCOUNT));
      System.out.println("Min balance: " + minBalance.toString());
      
      JSONObject maxBalance = executeQuery(String.format("source=%s | stats max(balance) as max_balance", TEST_INDEX_ACCOUNT));
      System.out.println("Max balance: " + maxBalance.toString());
    } catch (Exception e) {
      System.err.println("Error getting balance range: " + e.getMessage());
    }
    
    // Get min/max age
    try {
      JSONObject ageRange = executeQuery(String.format("source=%s | stats min(age) as min_age, max(age) as max_age", TEST_INDEX_ACCOUNT));
      System.out.println("Age range: " + ageRange.toString());
    } catch (Exception e) {
      System.err.println("Error getting age range: " + e.getMessage());
    }
  }

  @Test
  public void step7_testDefaultBinBehavior() throws IOException {
    System.out.println("=== STEP 7: Test Default Bin Behavior ===");
    
    try {
      JSONObject result = executeQuery(String.format("source=%s | bin age | fields account_number, age, age_bin | head 3", TEST_INDEX_ACCOUNT));
      System.out.println("Bin with no parameters: " + result.toString());
    } catch (Exception e) {
      System.err.println("Error with default bin: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Test
  public void step8_testDecimalSpan() throws IOException {
    System.out.println("=== STEP 8: Test Decimal Span ===");
    
    try {
      JSONObject result = executeQuery(String.format("source=%s | bin balance span=2500.5 AS balance_group | fields account_number, balance, balance_group | head 3", TEST_INDEX_ACCOUNT));
      System.out.println("Bin with decimal span: " + result.toString());
    } catch (Exception e) {
      System.err.println("Error with decimal span: " + e.getMessage());
      e.printStackTrace();
    }
  }
}