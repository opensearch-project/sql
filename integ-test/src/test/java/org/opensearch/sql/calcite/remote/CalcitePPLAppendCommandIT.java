/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLAppendCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testAppend() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats sum(age) as sum_age_by_gender by gender | append [ stats"
                    + " sum(age) as sum_age_by_state by state | sort sum_age_by_state ] | head 5",
                TEST_INDEX_ACCOUNT));
    verifySchemaInOrder(
        actual,
        schema("sum_age_by_gender", "bigint"),
        schema("gender", "string"),
        schema("sum_age_by_state", "bigint"),
        schema("state", "string"));
    verifyDataRows(
        actual,
        rows(14947, "F", null, null),
        rows(15224, "M", null, null),
        rows(null, null, 369, "NV"),
        rows(null, null, 412, "NM"),
        rows(null, null, 414, "AZ"));
  }

  @Test
  public void testAppendWithMergedColumn() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats sum(age) as sum by gender |"
                    + " append [ stats sum(age) as sum by state | sort sum ] | head 5",
                TEST_INDEX_ACCOUNT));
    verifySchemaInOrder(
        actual, schema("sum", "bigint"), schema("gender", "string"), schema("state", "string"));
    verifyDataRows(
        actual,
        rows(14947, "F", null),
        rows(15224, "M", null),
        rows(369, null, "NV"),
        rows(412, null, "NM"),
        rows(414, null, "AZ"));
  }

  public void testAppendWithConflictTypeColumn() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats sum(age) as sum by gender | append [ stats sum(age) as sum by"
                    + " state | sort sum | eval sum = cast(sum as double) ] | head 5",
                TEST_INDEX_ACCOUNT));
    verifySchemaInOrder(
        actual,
        schema("sum", "bigint"),
        schema("gender", "string"),
        schema("state", "string"),
        schema("sum0", "double"));
    verifyDataRows(
        actual,
        rows(14947, "F", null, null),
        rows(15224, "M", null, null),
        rows(null, null, "NV", 369d),
        rows(null, null, "NM", 412d),
        rows(null, null, "AZ", 414d));
  }
}
