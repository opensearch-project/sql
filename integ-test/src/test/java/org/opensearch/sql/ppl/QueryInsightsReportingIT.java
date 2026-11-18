/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.TestUtils;

/**
 * Verifies that reporting completed PPL queries to Query Insights does not interfere with query
 * execution.
 *
 * <p>Query Insights is a separate plugin and is not installed in the SQL integ-test cluster. The
 * reporting gate ({@code PPLQueryInsightsCoordinator#isEnabledFor}) therefore returns false and the
 * whole reporting path is skipped, so these tests assert the important contract: with Query
 * Insights absent, plain and multi-scan (join) PPL queries still run and return correct results.
 * {@link QueryInsightsRecordingIT} asserts the records and sub-queries themselves, on a cluster
 * with Query Insights installed.
 */
public class QueryInsightsReportingIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite(); // join runs on the Calcite path
    loadIndex(Index.BANK);
    loadIndex(Index.DOG);
  }

  @Test
  public void singleSourceQueryReturnsResultsWithReportingPathActive() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | where age > 30 | fields firstname", TEST_INDEX_BANK));
    verifyDataRows(
        result,
        rows("Amber JOHnny"),
        rows("Hattie"),
        rows("Dale"),
        rows("Elinor"),
        rows("Virginia"),
        rows("Dillard"));
  }

  @Test
  public void joinQuerySpawningChildScansReturnsResults() throws IOException {
    // A join fans out into per-side child scans on the background pool — the path that stamps the
    // parent marker. It must still return results when Query Insights is not installed.
    JSONObject result;
    try {
      result =
          executeQuery(
              String.format(
                  "source=%s | join on firstname=holdersName %s | fields firstname, dog_name",
                  TEST_INDEX_BANK, TEST_INDEX_DOG));
    } catch (ResponseException e) {
      // Surface the server error body to make failures debuggable rather than opaque.
      throw new AssertionError(
          "join query failed: " + TestUtils.getResponseBody(e.getResponse()), e);
    }
    // Hattie is the only account holder who owns a dog.
    verifyDataRows(result, rows("Hattie", "snoopy"));
  }
}
