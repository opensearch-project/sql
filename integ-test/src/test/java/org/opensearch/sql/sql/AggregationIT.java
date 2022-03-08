/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class AggregationIT extends SQLIntegTestCase {
  @Override
  protected void init() throws Exception {
    loadIndex(Index.BANK);
  }

  @Test
  void filteredAggregatePushedDown() throws IOException {
    JSONObject response = executeQuery(
        "SELECT COUNT(*) FILTER(WHERE age > 35) FROM " + TEST_INDEX_BANK);
    verifySchema(response, schema("COUNT(*)", null, "integer"));
    verifyDataRows(response, rows(3));
  }

  @Test
  void filteredAggregateNotPushedDown() throws IOException {
    JSONObject response = executeQuery(
        "SELECT COUNT(*) FILTER(WHERE age > 35) FROM (SELECT * FROM " + TEST_INDEX_BANK
            + ") AS a");
    verifySchema(response, schema("COUNT(*)", null, "integer"));
    verifyDataRows(response, rows(3));
  }
}
