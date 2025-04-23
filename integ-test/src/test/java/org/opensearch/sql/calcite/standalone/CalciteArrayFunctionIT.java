/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

import java.io.IOException;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.*;

public class CalciteArrayFunctionIT extends CalcitePPLIntegTestCase {
    @Override
    public void init() throws IOException {
        super.init();
        loadIndex(Index.BANK);
    }

    @Test
    public void testForAll() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s | eval array = array(1, -1, 2), result = forall(array, x -> x > 0) | fields result | head 1",
                                TEST_INDEX_BANK));

        verifySchema(actual, schema("result", "boolean"));

        verifyDataRows(
                actual,
                rows(false));
    }

    @Test
    public void testForAll() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s | eval array = array(1, -1, 2), result = forall(array, x -> x > 0) | fields result | head 1",
                                TEST_INDEX_BANK));

        verifySchema(actual, schema("result", "boolean"));

        verifyDataRows(
                actual,
                rows(false));
    }
}
