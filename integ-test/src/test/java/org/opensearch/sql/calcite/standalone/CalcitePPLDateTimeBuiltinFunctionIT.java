/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprYearweek;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.formatNow;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_FORMATS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY_WITH_NULL;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.expression.function.FunctionProperties;

public class CalcitePPLDateTimeBuiltinFunctionIT extends CalcitePPLIntegTestCase {
    @Override
    public void init() throws IOException {
        super.init();
        loadIndex(Index.STATE_COUNTRY);
        loadIndex(Index.STATE_COUNTRY_WITH_NULL);
        loadIndex(Index.DATE_FORMATS);
        initRelativeDocs();
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

    @Test
    public void testDateSubAndCount(){
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| where strict_date_optional_time > DATE_SUB(NOW(), INTERVAL 1 DAY) "
                                        + "| stats COUNT() AS CNT "
                                        , TEST_INDEX_DATE_FORMATS));
        verifySchema(actual,
                schema("CNT", "long")
        );

        // tmr, +month, now
        verifyDataRows(actual, rows(
                3
        ));

    }


    @Test
    public void testUnixTimeStampTwoArgument(){
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| eval from_unix = FROM_UNIXTIME(1220249547, '%%T')"
                                        + "| fields from_unix | head 1"
                                , TEST_INDEX_DATE_FORMATS));
        verifySchema(actual,
                schema("from_unix", "string")
        );
        verifyDataRows(actual, rows(
                "06:12:27"
        ));
    }


    @Test
    public void testUnixTimeStampAndFromUnixTime(){
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| eval from_unix = from_unixtime(1220249547)"
                                        + "| eval to_unix = unix_timestamp(from_unix)"
                                        + "| where unix_timestamp(from_unixtime(1700000001)) > 1700000000 " // don't do filter
                                        + "| fields from_unix, to_unix | head 1"
                                , TEST_INDEX_DATE_FORMATS));
        verifySchema(actual,
                schema("from_unix", "timestamp"),
                schema("to_unix", "double")
        );
        verifyDataRows(actual, rows(
                "2008-09-01 06:12:27", 1220249547.0
        ));
    }

    @Test
    public void testUtcTimes(){
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| eval timestamp=UTC_TIMESTAMP() "
                                        + "| eval time=UTC_TIME()"
                                        + "| eval date=UTC_DATE()"
                                        + "| fields timestamp, time, date "
                                , TEST_INDEX_DATE_FORMATS));
        verifySchema(actual,
                schema("timestamp", "timestamp"),
                schema("date", "date"),
                schema("time", "time")

        );
    }


    @Test
    public void testWeekAndWeekOfYear(){
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s | fields  strict_date_optional_time"
                                        + "| where YEAR(strict_date_optional_time) < 2000"
                                        + "| eval `WEEK(DATE(strict_date_optional_time))` = WEEK(DATE(strict_date_optional_time))"
                                        + "| eval `WEEK_OF_YEAR(DATE(strict_date_optional_time))` = WEEK_OF_YEAR(DATE(strict_date_optional_time))"
                                        + "| eval `WEEK(DATE(strict_date_optional_time), 1)` = WEEK(DATE(strict_date_optional_time), 1)"
                                        + "| eval `WEEK_OF_YEAR(DATE(strict_date_optional_time), 1)` = WEEK_OF_YEAR(DATE(strict_date_optional_time), 1)"
                                        + "| eval `WEEK(DATE('2008-02-20'))` = WEEK(DATE('2008-02-20')), `WEEK(DATE('2008-02-20'), 1)` = WEEK(DATE('2008-02-20'), 1)"
                                        + "| fields `WEEK(DATE(strict_date_optional_time))`, `WEEK_OF_YEAR(DATE(strict_date_optional_time))`, `WEEK(DATE(strict_date_optional_time), 1)`, `WEEK_OF_YEAR(DATE(strict_date_optional_time), 1)`, `WEEK(DATE('2008-02-20'))`, `WEEK(DATE('2008-02-20'), 1)`"
                                        + "| head 1 ", TEST_INDEX_DATE_FORMATS));

        verifySchema(actual,
                schema("WEEK(DATE(strict_date_optional_time))", "integer"),
                schema("WEEK_OF_YEAR(DATE(strict_date_optional_time))", "integer"),
                schema("WEEK(DATE(strict_date_optional_time), 1)", "integer"),
                schema("WEEK_OF_YEAR(DATE(strict_date_optional_time), 1)", "integer"),
                schema("WEEK(DATE('2008-02-20'))", "integer"),
                schema("WEEK(DATE('2008-02-20'), 1)", "integer")
        );

        verifyDataRows(actual, rows(
                15, 15, 15, 15, 7, 8
        ));
    }

    @Test
    public void testWeekAndWeekOfYearWithFilter(){
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s | fields  strict_date_optional_time"
                                        + "| where YEAR(strict_date_optional_time) < 2000"
                                        + "| where WEEK(DATE(strict_date_optional_time)) = 15"
                                        + "| stats COUNT() AS CNT "
                                        + "| head 1 ", TEST_INDEX_DATE_FORMATS));

        verifySchema(actual,
                schema("CNT", "long")
        );

        verifyDataRows(actual, rows(
                2
        ));
    }

    @Test
    public void testWeekDay(){
        int currentWeekDay = formatNow(new FunctionProperties().getQueryStartClock()).getDayOfWeek().getValue()
                - 1;
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| where YEAR(strict_date_optional_time) < 2000"
                                        + "| eval timestamp=weekday(TIMESTAMP(strict_date_optional_time)), time=weekday(TIME(strict_date_optional_time)), date=weekday(DATE(strict_date_optional_time))"
                                        + "| eval `weekday('2020-08-26')` = weekday('2020-08-26') "
                                        + "| fields timestamp, time, date, `weekday('2020-08-26')`"
                                        + "| head 1 ", TEST_INDEX_DATE_FORMATS));

        verifySchema(actual,
                schema("timestamp", "integer"),
                schema("time", "integer"),
                schema("date", "integer"),
                schema("weekday('2020-08-26')", "integer")
        );

        verifyDataRows(actual, rows(
                3, currentWeekDay, 3, 2
        ));
    }


    @Test
    public void testYearWeek(){
        int currentYearWeek = exprYearweek(new ExprDateValue(LocalDateTime.now(new FunctionProperties().getQueryStartClock()).toLocalDate()), new ExprIntegerValue(0)).integerValue();
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| where YEAR(strict_date_optional_time) < 2000"
                                        + "| eval timestamp=YEARWEEK(TIMESTAMP(strict_date_optional_time)), time=YEARWEEK(TIME(strict_date_optional_time)), date=YEARWEEK(DATE(strict_date_optional_time))"
                                        + "| eval `YEARWEEK('2020-08-26')` = YEARWEEK('2020-08-26') | eval `YEARWEEK('2019-01-05', 1)` = YEARWEEK('2019-01-05', 1) | fields timestamp, time, date, `YEARWEEK('2020-08-26')`, `YEARWEEK('2019-01-05', 1)`"
                                        + "| head 1 ", TEST_INDEX_DATE_FORMATS));

        verifySchema(actual,
                schema("timestamp", "integer"),
                schema("time", "integer"),
                schema("date", "integer"),
                schema("YEARWEEK('2020-08-26')", "integer"),
                schema("YEARWEEK('2019-01-05', 1)", "integer")
        );

        verifyDataRows(actual, rows(
                198415, currentYearWeek, 198415, 202034, 201901
        ));
    }

    @Test
    public void testYearWeekWithFilter(){
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| where YEARWEEK(strict_date_optional_time) < 200000"
                                        + "| stats COUNT() AS CNT"
                                        + "| head 1 ", TEST_INDEX_DATE_FORMATS));

        verifySchema(actual,
                schema("CNT", "long")
        );

        verifyDataRows(actual, rows(
                2
        ));
    }

    @Test
    public void testYear(){
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| where YEAR(strict_date_optional_time) = 1984 "
                                        + "| eval timestamp=YEAR(TIMESTAMP(strict_date_optional_time)), date=YEAR(DATE(strict_date_optional_time))"
                                        + "| eval `YEAR('2020-08-26')` = YEAR('2020-08-26') | fields timestamp, date, `YEAR('2020-08-26')`"
                                        + "| head 1 ", TEST_INDEX_DATE_FORMATS));

        verifySchema(actual,
                schema("timestamp", "integer"),
                schema("date", "integer"),
                schema("YEAR('2020-08-26')", "integer")
        );

        verifyDataRows(actual, rows(
                1984, 1984, 2020
        ));
    }


    private void initRelativeDocs() throws IOException {
        List<String> relativeList = List.of("NOW", "TMR", "+month", "-2wk", "-1d@d");
        int index = 0;
        for (String time: relativeList) {
            Request request =
                    new Request("PUT", "/opensearch-sql_test_index_date_formats/_doc/%s?refresh=true".formatted(index));
            request.setJsonEntity(
                    "{\"strict_date_optional_time\":\"%s\"}".formatted(convertTimeExpression(time))
            );


            index ++ ;
            client().performRequest(request);
        }
    }

    private String convertTimeExpression(String expression) {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
        ZonedDateTime result = now;

        switch (expression) {
            case "NOW":
                break;
            case "TMR": // Tomorrow
                result = now.plusDays(1).truncatedTo(ChronoUnit.DAYS);
                break;
            case "+month": // In one month
                result = now.plusMonths(1);
                break;
            case "-2wk": // Two weeks ago
                result = now.minusWeeks(2);
                break;
            case "-1d@d": // Yesterday
                result = now.minusDays(1).truncatedTo(ChronoUnit.DAYS);
                break;
            default:
                throw new IllegalArgumentException("Unknown time expression: " + expression);
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        return result.format(formatter);
    }

}
