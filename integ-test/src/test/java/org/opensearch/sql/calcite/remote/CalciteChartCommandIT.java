/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

import java.io.IOException;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEquals;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

public class CalciteChartCommandIT extends PPLIntegTestCase {
    @Override
    protected void init() throws Exception {
        super.init();
        enableCalcite();
        loadIndex(Index.BANK);
        loadIndex(Index.BANK_WITH_NULL_VALUES);
        loadIndex(Index.OTELLOGS);
    }

    @Test
    public void testChartWithSingleGroupKey() throws IOException {
        JSONObject result1 = executeQuery(String.format("source=%s | chart avg(balance) by gender", TEST_INDEX_BANK));
        verifySchema(
                result1,
                schema("avg(balance)", "double"),
                schema("gender", "string"));
        verifyDataRows(result1, rows(40488, "F"), rows(16377.25, "M"));
        JSONObject result2 = executeQuery(String.format("source=%s | chart avg(balance) over gender", TEST_INDEX_BANK));
        assertJsonEquals(result1.toString(), result2.toString());
    }

    @Test
    public void testChartWithMultipleGroupKeys() throws IOException {
        JSONObject result1 = executeQuery(String.format("source=%s | chart avg(balance) by gender, age", TEST_INDEX_BANK));
        verifySchema(
                result1,
                schema("avg(balance)", "double"),
                schema("gender", "string"),
                schema("age", "string"));
        verifyDataRows(result1, rows(40488, "F", "36"), rows(16377.25, "M", 36));
        JSONObject result2 = executeQuery(String.format("source=%s | chart avg(balance) over gender, age", TEST_INDEX_BANK));
        assertJsonEquals(result1.toString(), result2.toString());
    }

    // TODOs:
    // Param nullstr: source=opensearch-sql_test_index_bank_with_null_values | eval age = cast(age as string) |  chart nullstr='nil' max(account_number) over gender by age
    // Param usenull: source=opensearch-sql_test_index_bank_with_null_values | eval age = cast(age as string) |  chart usenull=false nullstr='nil' max(account_number) over gender by age
    // Param limit = 0: source=bank | chart limit=0 avg(balance) over state by gender
    // SPAN:

}
