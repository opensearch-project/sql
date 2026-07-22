/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_LOGS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Tests partial filter pushdown when some predicates can be pushed natively and others require
 * script evaluation.
 *
 * <p>Regression test for issue where LIKE on text fields (without .keyword subfield) caused entire
 * AND filter to fall back to script, preventing timestamp range pushdown.
 */
public class CalcitePartialFilterPushdownIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.LOGS);
  }

  @Test
  public void testTimestampRangePushesWithUnpushableLike() throws IOException {
    // message is text field without .keyword — LIKE on it requires script evaluation
    // @timestamp is date field — range should push natively despite LIKE failing
    String query =
        String.format(
            "source=%s | where `@timestamp` >= '2023-01-01' and `@timestamp` < '2023-01-04' "
                + "and LIKE(message, '%%failed%%') | stats count()",
            TEST_INDEX_LOGS);

    JSONObject result = executeQuery(query);

    // Just verify query executes and returns reasonable results
    // The key regression is that this doesn't do a full table scan
    verifySchema(result, schema("count()", "bigint"));
    // Should find "Database connection failed" in the date range
    verifyDataRows(result, rows(1L));
  }

  @Test
  public void testMultipleUnpushablePredicatesInAnd() throws IOException {
    // Both LIKE conditions are on text field, but timestamp should still push
    String query =
        String.format(
            "source=%s | where `@timestamp` >= '2023-01-01' and LIKE(message, '%%space%%') "
                + "and LIKE(message, '%%low%%') | stats count()",
            TEST_INDEX_LOGS);

    JSONObject result = executeQuery(query);
    verifySchema(result, schema("count()", "bigint"));
    // Should find "Disk space low"
    verifyDataRows(result, rows(1L));
  }
}
