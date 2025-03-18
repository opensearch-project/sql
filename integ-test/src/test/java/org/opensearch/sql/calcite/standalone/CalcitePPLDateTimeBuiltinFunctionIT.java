/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY_WITH_NULL;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class CalcitePPLDateTimeBuiltinFunctionIT extends CalcitePPLIntegTestCase {
    @Override
    public void init() throws IOException {
        super.init();
        loadIndex(Index.STATE_COUNTRY);
        loadIndex(Index.STATE_COUNTRY_WITH_NULL);
    }

    @Test
    public void testDate() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| eval `DATE('2020-08-26')` = DATE('2020-08-26') "
                                        + "| eval `DATE(TIMESTAMP('2020-08-26 13:49:00'))` = DATE(TIMESTAMP('2020-08-26 13:49:00')) "
                                        + "| eval `DATE('2020-08-26 13:49')` = DATE('2020-08-26 13:49') "
                                        + "| fields `DATE('2020-08-26')`, `DATE(TIMESTAMP('2020-08-26 13:49:00'))`, `DATE('2020-08-26 13:49')` "
                                        + "| head 1", TEST_INDEX_STATE_COUNTRY));

        verifySchema(actual, schema("DATE('2020-08-26')", "date"),
                schema("DATE(TIMESTAMP('2020-08-26 13:49:00'))", "date"),
                schema("DATE('2020-08-26 13:49')", "date"));

        verifyDataRows(actual, rows(Date.valueOf("2020-08-26"),
                Date.valueOf("2020-08-26"),
                Date.valueOf("2020-08-26")));
    }

    @Test
    public void testTimestamp() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| eval `TIMESTAMP('2020-08-26 13:49:00')` = TIMESTAMP('2020-08-26 13:49:00')"
                                        + "| eval `TIMESTAMP(DATE('2020-08-26 13:49:00'))` = TIMESTAMP(DATE('2020-08-26 13:49:00'))"
                                        + "| eval `TIMESTAMP(TIMESTAMP('2020-08-26 13:49:00'))` = TIMESTAMP(TIMESTAMP('2020-08-26 13:49:00'))"
                                        + "| eval `TIMESTAMP(TIME('2020-08-26 13:49:00'))` = TIMESTAMP(TIME('2020-08-26 13:49:00'))"
                                        + "| eval `TIMESTAMP('2020-08-26 13:49:00', 2020-08-26 00:10:10)` = TIMESTAMP('2020-08-26 13:49:00', '2020-08-26 00:10:10')"
                                        + "| eval `TIMESTAMP('2020-08-26 13:49:00', TIMESTAMP(2020-08-26 00:10:10))` = TIMESTAMP('2020-08-26 13:49:00', TIMESTAMP('2020-08-26 00:10:10'))"
                                        + "| eval `TIMESTAMP('2020-08-26 13:49:00', DATE(2020-08-26 00:10:10))` = TIMESTAMP('2020-08-26 13:49:00', DATE('2020-08-26 00:10:10'))"
                                        + "| eval `TIMESTAMP('2020-08-26 13:49:00', TIME(00:10:10))` = TIMESTAMP('2020-08-26 13:49:00', TIME('00:10:10'))"
                                        + "| fields `TIMESTAMP('2020-08-26 13:49:00')`, `TIMESTAMP(DATE('2020-08-26 13:49:00'))`, `TIMESTAMP(TIMESTAMP('2020-08-26 13:49:00'))`,  `TIMESTAMP(TIME('2020-08-26 13:49:00'))`, "
                                        + "`TIMESTAMP('2020-08-26 13:49:00', 2020-08-26 00:10:10)`, `TIMESTAMP('2020-08-26 13:49:00', TIMESTAMP(2020-08-26 00:10:10))`, `TIMESTAMP('2020-08-26 13:49:00', DATE(2020-08-26 00:10:10))`, `TIMESTAMP('2020-08-26 13:49:00', TIME(00:10:10))`"
                                        + "| head 1", TEST_INDEX_STATE_COUNTRY));

        verifySchema(actual, schema("TIMESTAMP('2020-08-26 13:49:00')", "timestamp"),
                schema("TIMESTAMP(DATE('2020-08-26 13:49:00'))", "timestamp"),
                schema("TIMESTAMP(TIMESTAMP('2020-08-26 13:49:00'))", "timestamp"),
                schema("TIMESTAMP(TIME('2020-08-26 13:49:00'))", "timestamp"),
                schema("TIMESTAMP('2020-08-26 13:49:00', 2020-08-26 00:10:10)", "timestamp"),
                schema("TIMESTAMP('2020-08-26 13:49:00', TIMESTAMP(2020-08-26 00:10:10))", "timestamp"),
                schema("TIMESTAMP('2020-08-26 13:49:00', DATE(2020-08-26 00:10:10))", "timestamp"),
                schema("TIMESTAMP('2020-08-26 13:49:00', TIME(00:10:10))", "timestamp")
        );

        verifyDataRows(actual, rows("2020-08-26 13:49:00",
                "2020-08-26 00:00:00",
                "2020-08-26 13:49:00",
                "2025-03-18 13:49:00",
                "2020-08-26 13:59:10",
                "2020-08-26 13:59:10",
                "2020-08-26 13:49:00",
                "2020-08-26 13:59:10"
        ));
    }

    @Test
    public void testTime() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| eval `TIME('2020-08-26 13:49:00')` = TIME('2020-08-26 13:49:00')"
                                        + "| eval `TIME('2020-08-26 13:49')` = TIME('2020-08-26 13:49')"
                                        + "| eval `TIME('13:49')` = TIME('13:49')"
                                        + "| eval `TIME('13:49:00')` = TIME('13:49:00')"
                                        + "| eval `TIME(TIME('13:49:00'))` = TIME(TIME('13:49:00'))"
                                        + "| eval `TIME(TIMESTAMP('2024-08-06 13:49:00'))` = TIME(TIMESTAMP('2024-08-06 13:49:00'))"
                                        + "| eval `TIME(DATE('2024-08-06 13:49:00'))` = TIME(DATE('2024-08-06 13:49:00'))"
                                        + "| fields `TIME('2020-08-26 13:49:00')`, `TIME('2020-08-26 13:49')`, `TIME('13:49')`,  `TIME('13:49:00')`, "
                                        + "`TIME(TIME('13:49:00'))`, `TIME(TIMESTAMP('2024-08-06 13:49:00'))`, `TIME(DATE('2024-08-06 13:49:00'))`"
                                        + "| head 1", TEST_INDEX_STATE_COUNTRY));

        verifySchema(actual, schema("TIME('2020-08-26 13:49:00')", "time"),
                schema("TIME('2020-08-26 13:49')", "time"),
                schema("TIME('13:49')", "time"),
                schema("TIME('13:49:00')", "time"),
                schema("TIME(TIME('13:49:00'))", "time"),
                schema("TIME(TIMESTAMP('2024-08-06 13:49:00'))", "time"),
                schema("TIME(DATE('2024-08-06 13:49:00'))", "time")
        );

        verifyDataRows(actual, rows("13:49:00",
                "13:49:00",
                "13:49:00",
                "13:49:00",
                "13:49:00",
                "13:49:00",
                "00:00:00"
        ));
    }


}
