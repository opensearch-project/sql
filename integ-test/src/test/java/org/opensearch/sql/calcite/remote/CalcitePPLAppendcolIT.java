/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLAppendcolIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testAppendCol() throws IOException {
    // Although the plans are identical, not pushing down resulting the cnt in the first two rows
    // being null
    enabledOnlyWhenPushdownIsEnabled();
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats sum(age) as sum by gender, state | sort gender, state |"
                    + " appendcol [ stats count(age) as cnt by gender | sort gender ] | fields"
                    + " gender, state, sum, cnt | head 10",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        actual,
        schema("gender", "string"),
        schema("state", "string"),
        schema("sum", "bigint"),
        schema("cnt", "bigint"));
    verifyDataRows(
        actual,
        rows("F", "AK", 317, 493),
        rows("F", "AL", 397, 507),
        rows("F", "AR", 229, null),
        rows("F", "AZ", 238, null),
        rows("F", "CA", 282, null),
        rows("F", "CO", 217, null),
        rows("F", "CT", 147, null),
        rows("F", "DC", 358, null),
        rows("F", "DE", 101, null),
        rows("F", "FL", 310, null));
  }

  @Test
  public void testAppendColOverride() throws IOException {
    // Although the plans are identical, not pushing down resulting the cnt in the first two rows
    // being null
    enabledOnlyWhenPushdownIsEnabled();
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats sum(age) as sum by gender, state | sort gender, state |"
                    + " appendcol override = true [ stats count(age) as cnt by gender | sort gender"
                    + " ] | fields gender, state, sum, cnt | head 10",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        actual,
        schema("gender", "string"),
        schema("state", "string"),
        schema("sum", "bigint"),
        schema("cnt", "bigint"));
    verifyDataRows(
        actual,
        rows("F", "AK", 317, 493),
        rows("M", "AL", 397, 507),
        rows("F", "AR", 229, null),
        rows("F", "AZ", 238, null),
        rows("F", "CA", 282, null),
        rows("F", "CO", 217, null),
        rows("F", "CT", 147, null),
        rows("F", "DC", 358, null),
        rows("F", "DE", 101, null),
        rows("F", "FL", 310, null));
  }
}
