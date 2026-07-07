/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/** Integration test for GitHub issue #5174: bin/chart NPE with null values. */
public class CalciteBinChartNullIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK_WITH_NULL_VALUES);
  }

  @Test
  public void testBinThenChartWithNullValuesShouldNotCauseNPE() throws IOException {
    // bin balance span=10000 produces null for documents without a balance field.
    // chart count() over bal_bin by gender should handle these null bin values safely.
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=10000 as bal_bin"
                    + " | chart count() over bal_bin by gender",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(
        result,
        schema("bal_bin", "string"),
        schema("gender", "string"),
        schema("count()", "bigint"));
    // Should only contain rows for non-null balance values (4 records with balance)
    verifyDataRows(
        result,
        rows("0-10000", "M", 1),
        rows("30000-40000", "F", 1),
        rows("30000-40000", "M", 1),
        rows("40000-50000", "F", 1));
  }

  @Test
  public void testBinThenChartSingleGroupWithNullValues() throws IOException {
    // chart with only row split (no column split): the simpler sort path
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=10000 as bal_bin | chart count() over bal_bin",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(result, schema("bal_bin", "string"), schema("count()", "bigint"));
    verifyDataRows(result, rows("0-10000", 1), rows("30000-40000", 2), rows("40000-50000", 1));
  }
}
