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
import org.junit.Ignore;
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

    private static String getUtcDate() {
        return LocalDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
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
                getUtcDate() + " 13:49:00",
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
                                        + "| eval `TIME('13:49:00.123')` = TIME('13:49:00.123')"
                                        + "| eval `TIME(TIME('13:49:00'))` = TIME(TIME('13:49:00'))"
                                        + "| eval `TIME(TIMESTAMP('2024-08-06 13:49:00'))` = TIME(TIMESTAMP('2024-08-06 13:49:00'))"
                                        + "| eval `TIME(DATE('2024-08-06 13:49:00'))` = TIME(DATE('2024-08-06 13:49:00'))"
                                        + "| fields `TIME('2020-08-26 13:49:00')`, `TIME('2020-08-26 13:49')`, `TIME('13:49')`,  `TIME('13:49:00.123')`, "
                                        + "`TIME(TIME('13:49:00'))`, `TIME(TIMESTAMP('2024-08-06 13:49:00'))`, `TIME(DATE('2024-08-06 13:49:00'))`"
                                        + "| head 1", TEST_INDEX_STATE_COUNTRY));

        verifySchema(actual, schema("TIME('2020-08-26 13:49:00')", "time"),
                schema("TIME('2020-08-26 13:49')", "time"),
                schema("TIME('13:49')", "time"),
                schema("TIME('13:49:00.123')", "time"),
                schema("TIME(TIME('13:49:00'))", "time"),
                schema("TIME(TIMESTAMP('2024-08-06 13:49:00'))", "time"),
                schema("TIME(DATE('2024-08-06 13:49:00'))", "time")
        );

        verifyDataRows(actual, rows("13:49:00",
                "13:49:00",
                "13:49:00",
                "13:49:00.123",
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
                                        + "| where strict_date_optional_time > DATE_SUB(NOW(), INTERVAL 12 HOUR) "
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
    public void testTimeStrToDate(){
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| where YEAR(strict_date_optional_time) < 2000"
                                        + "| eval demo = str_to_date(\"01,5,2013\", \"%%d,%%m,%%Y\")"
                                        + "| where str_to_date(\"01,5,2013\", \"%%d,%%m,%%Y\")='2013-05-01 00:00:00'"
                                        + "| fields demo | head 1"
                                , TEST_INDEX_DATE_FORMATS));
        verifySchema(actual,
                schema("demo", "timestamp")
        );
        verifyDataRows(actual, rows(
                "2013-05-01 00:00:00"
        ));
    }

    @Test
    public void testTimeStrToDateReturnNull(){
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| where YEAR(strict_date_optional_time) < 2000"
                                        + "| eval demo = str_to_date(\"01,5,2013\", \"%%d,%%m\")"
                                        + "| where str_to_date(\"01,5,2013\", \"%%d,%%m,%%Y\")=TIMESTAMP('2013-05-01 00:00:00')"
                                        + "| fields demo | head 1"
                                , TEST_INDEX_DATE_FORMATS));
        verifySchema(actual,
                schema("demo", "timestamp")
        );
        verifyDataRows(actual, rows(
                "2013-05-01 00:00:00"
        ));
    }


    @Test
    public void testTimeFormat(){
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| where YEAR(strict_date_optional_time) < 2000"
                                        + "| eval timestamp=TIME_FORMAT(strict_date_optional_time, '%%h') "
                                        + "| eval time=TIME_FORMAT(time, '%%h')"
                                        + "| eval date=TIME_FORMAT(date, '%%h')"
                                        + "| eval string_value=TIME_FORMAT('1998-01-31 13:14:15.012345','%%h %%i' ) "
                                        + "| where TIME_FORMAT(strict_date_optional_time, '%%h')='09'"
                                        + "| fields timestamp, time, date, string_value | head 1"
                                , TEST_INDEX_DATE_FORMATS));
        verifySchema(actual,
                schema("timestamp", "string"),
                schema("time", "string"),
                schema("date", "string"),
                schema("string_value", "string")
        );
        verifyDataRows(actual, rows(
                "09", "09", "12", "01 14"
        ));
    }


    @Test
    public void testTimeToSec(){
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| where YEAR(strict_date_optional_time) < 2000"
                                        + "| eval timestamp=TIME_TO_SEC(strict_date_optional_time) "
                                        + "| eval time=TIME_TO_SEC(time)"
                                        + "| eval date=TIME_TO_SEC(date)"
                                        + "| eval long_value=TIME_TO_SEC('22:23:00') "
                                        + "| where TIME_TO_SEC('22:23:00')=80580"
                                        + "| fields timestamp, time, date, long_value | head 1"
                                , TEST_INDEX_DATE_FORMATS));
        verifySchema(actual,
                schema("timestamp", "long"),
                schema("time", "long"),
                schema("date", "long"),
                schema("long_value", "long")
        );
        verifyDataRows(actual, rows(
                32862, 32862, 0, 80580
        ));
    }


    @Test
    public void testSecToTime(){
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| where YEAR(strict_date_optional_time) < 2000"
                                        + "| eval long_value=SEC_TO_TIME(3601) "
                                        + "| eval double_value=SEC_TO_TIME(1234.123) "
                                        + "| fields long_value, double_value | head 1"
                                , TEST_INDEX_DATE_FORMATS));
        verifySchema(actual,
                schema("long_value", "time"),
                schema("double_value", "time")
        );
        verifyDataRows(actual, rows(
                "01:00:01", "00:20:34.123"
        ));
    }

    @Test
    public void testToSeconds(){
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| where YEAR(strict_date_optional_time) < 2000"
                                        + "| eval timestamp=to_seconds(strict_date_optional_time) "
                                        + "| eval time=to_seconds(time)"
                                        + "| eval date=to_seconds(date)"
                                        + "| eval string_value=to_seconds('2008-10-07')"
                                        + "| eval long_value = to_seconds(950228)"
                                        + "| where to_seconds(strict_date_optional_time) > 62617795199"
                                        + "| fields timestamp, time, date, string_value, long_value | head 1"
                                , TEST_INDEX_DATE_FORMATS));
        verifySchema(actual,
                schema("timestamp", "long"),
                schema("time", "long"),
                schema("date", "long"),
                schema("string_value", "long"),
                schema("long_value", "long")
        );
        verifyDataRows(actual, rows(
                62617828062L, 63909594462L, 62617795200L, 63390556800L, 62961148800L
        ));
    }


    @Test
    public void testToDays(){
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| where YEAR(strict_date_optional_time) < 2000"
                                        + "| eval timestamp=to_days(strict_date_optional_time) "
                                        + "| eval time=to_days(time)"
                                        + "| eval date=to_days(date)"
                                        + "| eval string_value=to_days('2008-10-07')"
                                        + "| where to_days(strict_date_optional_time) = 724743"
                                        + "| fields timestamp, time, date, string_value | head 1"
                                , TEST_INDEX_DATE_FORMATS));
        verifySchema(actual,
                schema("timestamp", "long"),
                schema("time", "long"),
                schema("date", "long"),
                schema("string_value", "long")
        );
        verifyDataRows(actual, rows(
                724743,739694,724743,733687
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

    @Test
    public void testAddDateAndSubDateWithConditionsAndRename() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s | eval lower = SUBDATE(date, 3), upper = ADDDATE(date, 1), ts ="
                                        + " ADDDATE(date, INTERVAL 1 DAY) | where strict_date < upper AND strict_date >"
                                        + " lower | rename strict_date as d | head 1 | fields lower, upper, d, ts",
                                TEST_INDEX_DATE_FORMATS));

        verifySchema(
                actual,
                schema("lower", "date"),
                schema("upper", "date"),
                schema("d", "date"),
                schema("ts", "timestamp"));
        verifyDataRows(actual, rows("1984-04-09", "1984-04-13", "1984-04-12", "1984-04-13 00:00:00"));
    }

    @Test
    public void testDateAddAndSub() {
        String expectedDate = getUtcDate();

        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s "
                                        + "| eval t1 = DATE_ADD(strict_date_optional_time, INTERVAL 1 HOUR) "
                                        + "| eval t2 = DATE_ADD(strict_date_optional_time, INTERVAL 1 DAY) "
                                        + "| eval t3 = DATE_ADD(strict_date, INTERVAL 1 HOUR) "
                                        + "| eval t4 = DATE_ADD('2020-08-26 01:01:01', INTERVAL 1 DAY) "
                                        + "| eval t5 = DATE_ADD(time, INTERVAL 1 HOUR) "
                                        + "| eval t6 = DATE_ADD(time, INTERVAL 5 HOUR) "
                                        + "| eval t7 = DATE_ADD(strict_date, INTERVAL 2 YEAR)"
                                        + "| eval t8 = DATE_ADD(DATE('2020-01-30'), INTERVAL 1 MONTH)" // edge case
                                        + "| eval t9 = DATE_ADD(DATE('2020-11-30'), INTERVAL 1 QUARTER)" // rare case
                                        + "| eval t10 = DATE_SUB(date, INTERVAL 31 DAY)"
                                        + "| eval t11 = DATE_SUB(basic_date_time, INTERVAL 1 HOUR)"
                                        + "| fields t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11 "
                                        + "| head 1",
                                TEST_INDEX_DATE_FORMATS));

        verifySchema(
                actual,
                schema("t1", "timestamp"),
                schema("t2", "timestamp"),
                schema("t3", "timestamp"),
                schema("t4", "timestamp"),
                schema("t5", "timestamp"),
                schema("t6", "timestamp"),
                schema("t7", "timestamp"),
                schema("t8", "timestamp"),
                schema("t9", "timestamp"),
                schema("t10", "timestamp"),
                schema("t11", "timestamp"));

        verifyDataRows(
                actual,
                rows(
                        "1984-04-12 10:07:42",
                        "1984-04-13 09:07:42",
                        "1984-04-12 01:00:00",
                        "2020-08-27 01:01:01",
                        expectedDate + " 10:07:42",
                        expectedDate + " 14:07:42",
                        "1986-04-12 00:00:00",
                        "2020-02-29 00:00:00",
                        "2021-02-28 00:00:00",
                        "1984-03-12 00:00:00",
                        "1984-04-12 08:07:42"));
    }

    @Test
    public void testDateAddWithComparisonAndConditions() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s | where date > DATE('1984-04-11') | eval tomorrow = DATE_ADD(date,"
                                        + " INTERVAL 1 DAY) | fields date, tomorrow",
                                TEST_INDEX_DATE_FORMATS));

        verifySchema(actual, schema("date", "date"), schema("tomorrow", "timestamp"));
        verifyDataRows(
                actual,
                rows("1984-04-12", "1984-04-13 00:00:00"),
                rows("1984-04-12", "1984-04-13 00:00:00"));
    }

    @Ignore
    @Test
    public void testComparisonBetweenDateAndTimestamp() {
        // TODO: Fix this
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s | where date > TIMESTAMP('1984-04-11 00:00:00') | stats COUNT() AS cnt",
                                TEST_INDEX_DATE_FORMATS));
        verifySchema(actual, schema("cnt", "long"));
        verifyDataRows(actual, rows(2));
    }

    @Test
    public void testQuarter() {
        JSONObject actual =
            executeQuery(
                String.format(
                    "source=%s "
                        + "| eval `QUARTER(DATE('2020-08-26'))` = QUARTER(DATE('2020-08-26')) "
                        + "| eval quarter2 = QUARTER(basic_date) "
                        + "| eval timestampQuarter2 = QUARTER(basic_date_time) "
                        + "| fields `QUARTER(DATE('2020-08-26'))`, quarter2, timestampQuarter2 "
                        + "| head 1",
                    TEST_INDEX_DATE_FORMATS));
        verifySchema(actual, schema("QUARTER(DATE('2020-08-26'))", "long"),
            schema("quarter2", "long"),
            schema("timestampQuarter2", "long"));
        verifyDataRows(actual, rows(3, 2, 2));
    }

    @Test
    public void testSecond() {
        JSONObject actual =
            executeQuery(
                String.format(
                    "source=%s "
                        + "| eval s = SECOND(TIMESTAMP('01:02:03')) "
                        + "| eval secondForTime = SECOND(basic_time) "
                        + "| eval secondForDate = SECOND(basic_date) "
                        + "| eval secondForTimestamp = SECOND(strict_date_optional_time_nanos) "
                        + "| fields s, secondForTime, secondForDate, secondForTimestamp "
                        + "| head 1",
                    TEST_INDEX_DATE_FORMATS));
        verifySchema(actual, schema("s", "long"),
            schema("secondForTime", "long"),
            schema("secondForDate", "long"),
            schema("secondForTimestamp", "long"));
        verifyDataRows(actual, rows(3, 42, 0, 42));
    }

    @Test
    public void testSecondOfMinute() {
        JSONObject actual =
            executeQuery(
                String.format(
                    "source=%s "
                        + "| eval s = second_of_minute(TIMESTAMP('01:02:03')) "
                        + "| eval secondForTime = second_of_minute(basic_time) "
                        + "| eval secondForDate = second_of_minute(basic_date) "
                        + "| eval secondForTimestamp = second_of_minute(strict_date_optional_time_nanos) "
                        + "| fields s, secondForTime, secondForDate, secondForTimestamp "
                        + "| head 1",
                    TEST_INDEX_DATE_FORMATS));
        verifySchema(actual, schema("s", "long"),
            schema("secondForTime", "long"),
            schema("secondForDate", "long"),
            schema("secondForTimestamp", "long"));
        verifyDataRows(actual, rows(3, 42, 0, 42));
    }

    @Test
    public void testConvertTz() {
        JSONObject actual =
            executeQuery(
                String.format(
                    "source=%s "
                        + "| eval r1 = convert_tz('2008-05-15 12:00:00', '+00:00', '+10:00') "
                        + "| eval r2 = convert_tz(TIMESTAMP('2008-05-15 12:00:00'), '+00:00', '+10:00') "
                        + "| eval r3 = convert_tz(strict_date_optional_time_nanos, '+00:00', '+10:00') "
                        + "| eval r4 = convert_tz('2008-05-15 12:00:00', '-00:00', '+00:00') "
                        + "| eval r5 = convert_tz('2008-05-15 12:00:00', '+10:00', '+11:00') "
                        + "| eval r6 = convert_tz('2021-05-12 11:34:50', '-08:00', '+09:00') "
                        + "| eval r7 = convert_tz('2021-05-12 11:34:50', '-12:00', '+12:00') "
                        + "| eval r8 = convert_tz('2021-05-12 13:00:00', '+09:30', '+05:45') "
                        + "| fields r1, r2, r3, r4, r5, r6, r7, r8"
                        + "| head 1",
                    TEST_INDEX_DATE_FORMATS));
        verifySchema(actual, schema("r1", "timestamp"),
            schema("r2", "timestamp"),
            schema("r3", "timestamp"),
            schema("r4", "timestamp"),
            schema("r5", "timestamp"),
            schema("r6", "timestamp"),
            schema("r7", "timestamp"),
            schema("r8", "timestamp")
        );
        verifyDataRows(actual, rows("2008-05-15 22:00:00", "2008-05-15 22:00:00",
            "1984-04-12 19:07:42", "2008-05-15 12:00:00", "2008-05-15 13:00:00", "2021-05-13 04:34:50",
            "2021-05-13 11:34:50", "2021-05-12 09:15:00"));
    }

    @Test
    public void testConvertTzWithInvalidResult() {
        JSONObject actual =
            executeQuery(
                String.format(
                    "source=%s "
                        + "| eval r1 = convert_tz('2021-05-30 11:34:50', '-17:00', '+08:00') "
                        + "| eval r2 = convert_tz('2021-05-12 11:34:50', '-12:00', '+15:00') "
                        + "| eval r3 = convert_tz('2021-05-12 11:34:50', '-12:00', 'test') "
                        + "| fields r1, r2, r3"
                        + "| head 1",
                    TEST_INDEX_DATE_FORMATS));
        verifySchema(actual, schema("r1", "timestamp"),
            schema("r2", "timestamp"),
            schema("r3", "timestamp")
        );
        verifyDataRows(actual, rows(null, null, null));
    }

    @Test
    public void testGetFormat() {
        JSONObject actual =
            executeQuery(
                String.format(
                    "source=%s "
                        + "| eval r1 = GET_FORMAT(DATE, 'USA') "
                        + "| eval r2 = GET_FORMAT(TIME, 'INTERNAL') "
                        + "| eval r3 = GET_FORMAT(TIMESTAMP, 'EUR') "
                        + "| eval r4 = GET_FORMAT(TIMESTAMP, 'UTC') "
                        + "| fields r1, r2, r3, r4"
                        + "| head 1",
                    TEST_INDEX_DATE_FORMATS));
        verifySchema(actual, schema("r1", "string"),
            schema("r2", "string"),
            schema("r3", "string"),
            schema("r4", "string")
        );
        verifyDataRows(actual, rows("%m.%d.%Y", "%H%i%s", "%Y-%m-%d %H.%i.%s", null));
    }

    // TODO: Complete IT for MICROSECOND unit once it's supported
    @Test
    public void testExtractWithSimpleFormats() {
        JSONObject actual =
            executeQuery(
                String.format(
                    "source=%s "
                        + "| eval r1 = extract(YEAR FROM '1997-01-01 00:00:00') "
                        + "| eval r2 = extract(YEAR FROM strict_date_optional_time_nanos) "
                        + "| eval r3 = extract(year FROM basic_date) "
                        + "| eval r4 = extract(QUARTER FROM strict_date_optional_time_nanos) "
                        + "| eval r5 = extract(quarter FROM basic_date) "
                        + "| eval r6 = extract(MONTH FROM strict_date_optional_time_nanos) "
                        + "| eval r7 = extract(month FROM basic_date) "
                        + "| eval r8 = extract(WEEK FROM strict_date_optional_time_nanos) "
                        + "| eval r9 = extract(week FROM basic_date) "
                        + "| eval r10 = extract(DAY FROM strict_date_optional_time_nanos) "
                        + "| eval r11 = extract(day FROM basic_date) "
                        + "| eval r12 = extract(HOUR FROM strict_date_optional_time_nanos) "
                        + "| eval r13 = extract(hour FROM basic_time) "
                        + "| eval r14 = extract(MINUTE FROM strict_date_optional_time_nanos) "
                        + "| eval r15 = extract(minute FROM basic_time) "
                        + "| eval r16 = extract(SECOND FROM strict_date_optional_time_nanos) "
                        + "| eval r17 = extract(second FROM basic_time) "
                        + "| eval r18 = extract(second FROM '09:07:42') "
                        + "| eval r19 = extract(day FROM '1984-04-12') "
//                        + "| eval r20 = extract(MICROSECOND FROM timestamp('1984-04-12 09:07:42.123456789')) "
                        + "| fields r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19 "
                        + "| head 1",
                    TEST_INDEX_DATE_FORMATS));
        verifySchema(actual, schema("r1", "long"),
            schema("r2", "long"),
            schema("r3", "long"),
            schema("r4", "long"),
            schema("r5", "long"),
            schema("r6", "long"),
            schema("r7", "long"),
            schema("r8", "long"),
            schema("r9", "long"),
            schema("r10", "long"),
            schema("r11", "long"),
            schema("r12", "long"),
            schema("r13", "long"),
            schema("r14", "long"),
            schema("r15", "long"),
            schema("r16", "long"),
            schema("r17", "long"),
            schema("r18", "long"),
            schema("r19", "long")
        );
        verifyDataRows(actual, rows(1997, 1984, 1984, 2, 2, 4, 4, 15, 15, 12, 12, 9, 9, 7, 7, 42, 42, 42, 12));
    }

    // TODO: Complete IT for MICROSECOND unit once it's supported
    @Test
    public void testExtractWithComplexFormats() {
        JSONObject actual =
            executeQuery(
                String.format(
                    "source=%s "
                        + "| eval r1 = extract(YEAR_MONTH FROM '1997-01-01 00:00:00') "
                        + "| eval r2 = extract(DAY_HOUR FROM strict_date_optional_time_nanos) "
                        + "| eval r3 = extract(DAY_HOUR FROM basic_date) "
                        + "| eval r4 = extract(DAY_MINUTE FROM strict_date_optional_time_nanos) "
                        + "| eval r5 = extract(DAY_MINUTE FROM basic_date) "
                        + "| eval r6 = extract(DAY_SECOND FROM strict_date_optional_time_nanos) "
                        + "| eval r7 = extract(DAY_SECOND FROM basic_date) "
                        + "| eval r8 = extract(HOUR_MINUTE FROM strict_date_optional_time_nanos) "
                        + "| eval r9 = extract(HOUR_MINUTE FROM basic_time) "
                        + "| eval r10 = extract(HOUR_SECOND FROM strict_date_optional_time_nanos) "
                        + "| eval r11 = extract(HOUR_SECOND FROM basic_time) "
                        + "| eval r12 = extract(MINUTE_SECOND FROM strict_date_optional_time_nanos) "
                        + "| eval r13 = extract(MINUTE_SECOND FROM basic_time) "
//                        + "| eval r14 = extract(DAY_MICROSECOND FROM strict_date_optional_time_nanos) "
//                        + "| eval r15 = extract(HOUR_MICROSECOND FROM strict_date_optional_time_nanos) "
//                        + "| eval r16 = extract(MINUTE_MICROSECOND FROM strict_date_optional_time_nanos) "
//                        + "| eval r17 = extract(SECOND_MICROSECOND FROM strict_date_optional_time_nanos) "
                        + "| fields r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13 "
                        + "| head 1",
                    TEST_INDEX_DATE_FORMATS));
        verifySchema(actual, schema("r1", "long"),
            schema("r2", "long"),
            schema("r3", "long"),
            schema("r4", "long"),
            schema("r5", "long"),
            schema("r6", "long"),
            schema("r7", "long"),
            schema("r8", "long"),
            schema("r9", "long"),
            schema("r10", "long"),
            schema("r11", "long"),
            schema("r12", "long"),
            schema("r13", "long")
        );
        verifyDataRows(actual, rows(199701, 1209, 1200, 120907, 120000, 12090742, 12000000, 907, 907, 90742, 90742, 742, 742));
    }
}
