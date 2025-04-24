/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

import java.io.IOException;
import java.util.List;

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
    public void testExists() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s | eval array = array(1, -1, 2), result = exists(array, x -> x > 0) | fields result | head 1",
                                TEST_INDEX_BANK));

        verifySchema(actual, schema("result", "boolean"));

        verifyDataRows(
                actual,
                rows(true));
    }

    @Test
    public void testFilter() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s | eval array = array(1, -1, 2), result = filter(array, x -> x > 0) | fields result | head 1",
                                TEST_INDEX_BANK));

        verifySchema(actual, schema("result", "array"));

        verifyDataRows(
                actual,
                rows(List.of(1, 2)));
    }

    @Test
    public void testTransform2() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s | eval array = array(1), result = transform(array, x -> x  * 10.0) | fields result | head 1",
                                TEST_INDEX_BANK));

        verifySchema(actual, schema("result", "array"));

        verifyDataRows(
                actual,
                rows(List.of(2, 3, 4)));
    }

    @Test
    public void testTransform() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s | eval array = array(1, 2, 3), result = transform(array, x -> x + 1) | fields result | head 1",
                                TEST_INDEX_BANK));

        verifySchema(actual, schema("result", "array"));

        verifyDataRows(
                actual,
                rows(List.of(2, 3, 4)));
    }

    @Test
    public void testTransformForTwoInput() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s | eval array = array(1, 2, 3), result = transform(array, (x, i) -> x + i) | fields result | head 1",
                                TEST_INDEX_BANK));

        verifySchema(actual, schema("result", "array"));

        verifyDataRows(
                actual,
                rows(List.of(1, 3, 5)));
    }

    @Test
    public void testReduce() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s | eval array = array(1, 2, 3), result = reduce(array, 0, (acc, x) -> acc + x), result2 = reduce(array, 10, (acc, x) -> acc + x) | fields result,result2 | head 1",
                                TEST_INDEX_BANK));

        verifySchema(actual, schema("result", "integer"), schema("result2", "integer"));

        verifyDataRows(
                actual,
                rows(6, 16, 60));
    }

    @Test
    public void testReduce2() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s | eval array = array(1, 2, 3), result3 = reduce(array, 0, (acc, x) -> acc + x, acc -> acc * 10.0) | fields result3 | head 1",
                                TEST_INDEX_BANK));

        verifySchema(actual, schema("result3", "integer"));

        verifyDataRows(
                actual,
                rows(6, 16, 60));
    }
}
