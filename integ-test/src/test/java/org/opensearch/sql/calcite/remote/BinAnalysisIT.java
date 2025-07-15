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

public class BinAnalysisIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void analyzeAccountsData() throws IOException {
    System.out.println("=== ACCOUNTS DATA ANALYSIS ===");

    // Basic data verification
    JSONObject count = executeQuery(String.format("source=%s | stats count()", TEST_INDEX_ACCOUNT));
    System.out.println("Total records: " + count.toString());

    // Sample data
    JSONObject sample =
        executeQuery(
            String.format(
                "source=%s | fields account_number, balance, age | head 3", TEST_INDEX_ACCOUNT));
    System.out.println("Sample data: " + sample.toString());

    // Balance range
    JSONObject balanceRange =
        executeQuery(
            String.format(
                "source=%s | stats min(balance) as min_bal, max(balance) as max_bal",
                TEST_INDEX_ACCOUNT));
    System.out.println("Balance range: " + balanceRange.toString());

    // Age range
    JSONObject ageRange =
        executeQuery(
            String.format(
                "source=%s | stats min(age) as min_age, max(age) as max_age", TEST_INDEX_ACCOUNT));
    System.out.println("Age range: " + ageRange.toString());
  }

  @Test
  public void testBinBasic() throws IOException {
    System.out.println("=== BIN COMMAND BASIC TEST ===");

    // Test simple bin with balance
    JSONObject binBalance =
        executeQuery(
            String.format(
                "source=%s | bin balance span=5000 | fields account_number, balance, balance_bin |"
                    + " head 3",
                TEST_INDEX_ACCOUNT));
    System.out.println("Bin balance span=5000: " + binBalance.toString());

    // Test simple bin with age
    JSONObject binAge =
        executeQuery(
            String.format(
                "source=%s | bin age span=5 | fields account_number, age, age_bin | head 3",
                TEST_INDEX_ACCOUNT));
    System.out.println("Bin age span=5: " + binAge.toString());
  }

  @Test
  public void testBinWithStats() throws IOException {
    System.out.println("=== BIN WITH STATS TEST ===");

    // Bin balance and count per bucket
    JSONObject statsResult =
        executeQuery(
            String.format(
                "source=%s | bin balance span=10000 AS bucket | stats count() by bucket | sort"
                    + " bucket",
                TEST_INDEX_ACCOUNT));
    System.out.println("Balance histogram (span=10000): " + statsResult.toString());

    // Age histogram
    JSONObject ageHisto =
        executeQuery(
            String.format(
                "source=%s | bin age span=5 AS age_group | stats count() by age_group | sort"
                    + " age_group",
                TEST_INDEX_ACCOUNT));
    System.out.println("Age histogram (span=5): " + ageHisto.toString());
  }

  @Test
  public void testBinsBug() throws IOException {
    System.out.println("=== BINS PARAMETER BUG INVESTIGATION ===");

    // Test bins parameter to see the bug
    JSONObject binsResult =
        executeQuery(
            String.format(
                "source=%s | bin age bins=5 | fields account_number, age, age_bin | head 3",
                TEST_INDEX_ACCOUNT));
    System.out.println("Bins parameter result: " + binsResult.toString());

    // Get age data for manual calculation
    JSONObject ageStats =
        executeQuery(
            String.format(
                "source=%s | stats min(age) as min_age, max(age) as max_age, count() as cnt",
                TEST_INDEX_ACCOUNT));
    System.out.println("Age stats for bins calculation: " + ageStats.toString());
  }
}
