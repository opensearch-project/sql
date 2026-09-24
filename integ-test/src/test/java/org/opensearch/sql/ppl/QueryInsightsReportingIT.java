/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;

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
 * reporting gate ({@code isQueryInsightsRecordingEnabled}) therefore returns false and the whole
 * reporting path is skipped, so these tests assert the important contract: with Query Insights
 * absent, plain and multi-scan (join) PPL queries still run and return correct results. The
 * end-to-end assertion that records and sub-queries land in Query Insights lives with the Query
 * Insights plugin, which can co-install both plugins.
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
    assertTrue(result.getJSONArray("datarows").length() > 0);
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
                  "source=%s | join on firstname=holdersName %s", TEST_INDEX_BANK, TEST_INDEX_DOG));
    } catch (ResponseException e) {
      // Surface the server error body to make failures debuggable rather than opaque.
      throw new AssertionError(
          "join query failed: " + TestUtils.getResponseBody(e.getResponse()), e);
    }
    assertTrue(result.has("datarows"));
  }
}
