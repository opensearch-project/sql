/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteReverseCommandIT extends PPLIntegTestCase {

    @Override
    public void init() throws Exception {
        super.init();
        enableCalcite();
        disallowCalciteFallback();
        loadIndex(Index.BANK);
    }

    @Test
    public void testReverse() throws IOException {
        JSONObject result = executeQuery(String.format("source=%s | fields account_number | reverse", TEST_INDEX_BANK));
        verifySchema(result, schema("account_number", "bigint"));
        verifyDataRowsInOrder(result, rows(32), rows(25), rows(20), rows(18), rows(13), rows(6), rows(1));
    }

    @Test
    public void testReverseWithFields() throws IOException {
        JSONObject result = executeQuery(String.format("source=%s | fields account_number, firstname | reverse", TEST_INDEX_BANK));
        verifySchema(result, schema("account_number", "bigint"), schema("firstname", "string"));
        verifyDataRowsInOrder(
                result,
                rows(32, "Dillard"),
                rows(25, "Virginia"),
                rows(20, "Elinor"),
                rows(18, "Dale"),
                rows(13, "Nanette"),
                rows(6, "Hattie"),
                rows(1, "Amber JOHnny"));
    }

    @Test
    public void testReverseWithSort() throws IOException {
        JSONObject result = executeQuery(String.format("source=%s | sort account_number | fields account_number | reverse", TEST_INDEX_BANK));
        verifySchema(result, schema("account_number", "bigint"));
        verifyDataRowsInOrder(result, rows(32), rows(25), rows(20), rows(18), rows(13), rows(6), rows(1));
    }

    @Test
    public void testDoubleReverse() throws IOException {
        JSONObject result = executeQuery(String.format("source=%s | fields account_number | reverse | reverse", TEST_INDEX_BANK));
        verifySchema(result, schema("account_number", "bigint"));
        verifyDataRowsInOrder(result, rows(1), rows(6), rows(13), rows(18), rows(20), rows(25), rows(32));
    }

    @Test
    public void testReverseWithHead() throws IOException {
        JSONObject result = executeQuery(String.format("source=%s | fields account_number | reverse | head 3", TEST_INDEX_BANK));
        verifySchema(result, schema("account_number", "bigint"));
        verifyDataRowsInOrder(result, rows(32), rows(25), rows(20));
    }
}
