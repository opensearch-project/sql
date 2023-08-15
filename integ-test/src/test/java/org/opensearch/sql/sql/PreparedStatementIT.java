/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

public class PreparedStatementIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testPreparedStatement() {
    JSONObject response =
        new JSONObject(
            executeQuery(
                String.format(
                    "{\n"
                        + "  \"query\": \"SELECT state FROM %s WHERE state = ? GROUP BY state\",\n"
                        + "  \"parameters\": [\n"
                        + "    {\n"
                        + "      \"type\": \"string\",\n"
                        + "      \"value\": \"WA\"\n"
                        + "    }\n"
                        + " ]\n"
                        + "}",
                    TestsConstants.TEST_INDEX_ACCOUNT),
                "jdbc"));

    assertFalse(response.getJSONArray("datarows").isEmpty());
  }

  @Override
  protected String makeRequest(String query) {
    // Avoid wrap with "query" again
    return query;
  }
}
