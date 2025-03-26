package org.opensearch.sql.calcite.standalone;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Date;
import java.util.Objects;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_FORMATS_WITH_NULL;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

public class calciteBuiltinFunctionsNullIT extends CalcitePPLIntegTestCase {
    @Override
    public void init() throws IOException {
        super.init();
        loadIndex(Index.STATE_COUNTRY);
        loadIndex(Index.STATE_COUNTRY_WITH_NULL);
        loadIndex(Index.DATE_FORMATS);
        loadIndex(Index.DATE_FORMATS_WITH_NULL);
    }

    @Test
    public void testYearWeekInvalid() {
        assertThrows(Exception.class, () -> {
            // Code that should throw the exception
            JSONObject actual =
                    executeQuery(
                            String.format(
                                    "source=%s  | eval `YEARWEEK('2020-08-26')` = YEARWEEK('2020-15-26')",
                                    TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
    }

    @Test
    public void testYearWeekNull() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s  |  eval NullValue = YEARWEEK(date) | fields NullValue",
                                TEST_INDEX_DATE_FORMATS_WITH_NULL));


        verifySchema(
                actual,
                schema("NullValue", "integer"));
        JSONArray ret = actual.getJSONArray("datarows");
        for (int i = 0; i < ret.length(); i++) {
            Object o = ((JSONArray) ret.get(i)).get(0);
            assertEquals(JSONObject.NULL, o);
        }
    }

    @Test
    public void testYearInvalid() {
        assertThrows(Exception.class, () -> {
            // Code that should throw the exception
            JSONObject actual =
                    executeQuery(
                            String.format(
                                    "source=%s  | eval a = YEAR('2020-15-26')",
                                    TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
        assertThrows(Exception.class, () -> {
            // Code that should throw the exception
            JSONObject actual =
                    executeQuery(
                            String.format(
                                    "source=%s  | eval a = YEAR('2020-12-26 25:00:00')",
                                    TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
    }

    @Test
    public void testYearNull() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s  |  eval timestamp = YEAR(strict_date_optional_time), date=YEAR(date) | fields timestamp, date",
                                TEST_INDEX_DATE_FORMATS_WITH_NULL));


        verifySchema(
                actual,
                schema("timestamp", "integer"),
                schema("date", "integer"));
        JSONArray ret = (JSONArray) actual.getJSONArray("datarows").get(0);
        for (int i = 0; i < ret.length(); i++) {
            assertEquals(JSONObject.NULL, ret.get(i));
        }
    }


    @Test
    public void testWeekInvalid() {
        assertThrows(Exception.class, () -> {
            // Code that should throw the exception
            JSONObject actual =
                    executeQuery(
                            String.format(
                                    "source=%s  | eval a = WEEK('2020-15-26')",
                                    TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
        assertThrows(Exception.class, () -> {
            // Code that should throw the exception
            JSONObject actual =
                    executeQuery(
                            String.format(
                                    "source=%s  | eval a = WEEK('2020-12-26 25:00:00')",
                                    TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
    }

    @Test
    public void testWeekNull() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s  |  eval timestamp = WEEK(strict_date_optional_time), date=WEEK(date) | fields timestamp, date",
                                TEST_INDEX_DATE_FORMATS_WITH_NULL));


        verifySchema(
                actual,
                schema("timestamp", "integer"),
                schema("date", "integer"));
        JSONArray ret = (JSONArray) actual.getJSONArray("datarows").get(0);
        for (int i = 0; i < ret.length(); i++) {
            assertEquals(JSONObject.NULL, ret.get(i));
        }
    }

    @Test
    public void testWeekDayInvalid() {
        assertThrows(Exception.class, () -> {
            // Code that should throw the exception
            JSONObject actual =
                    executeQuery(
                            String.format(
                                    "source=%s  | eval a = WEEKDAY('2020-15-26')",
                                    TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
        assertThrows(Exception.class, () -> {
            // Code that should throw the exception
            JSONObject actual =
                    executeQuery(
                            String.format(
                                    "source=%s  | eval a = WEEKDAY('2020-12-26 25:00:00')",
                                    TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });

        assertThrows(Exception.class, () -> {
            // Code that should throw the exception
            JSONObject actual =
                    executeQuery(
                            String.format(
                                    "source=%s  | eval a = WEEKDAY('25:00:00')",
                                    TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
    }

    @Test
    public void testWeekDayNull() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s  |  eval timestamp = WEEKDAY(strict_date_optional_time), date=WEEKDAY(date), time=WEEKDAY(time) | fields timestamp, date, time",
                                TEST_INDEX_DATE_FORMATS_WITH_NULL));


        verifySchema(
                actual,
                schema("timestamp", "integer"),
                schema("date", "integer"),
                schema("time", "integer"));
        JSONArray ret = (JSONArray) actual.getJSONArray("datarows").get(0);
        for (int i = 0; i < ret.length(); i++) {
            assertEquals(JSONObject.NULL, ret.get(i));
        }
    }


    @Test
    public void testUnixTimestampInvalid() {
        assertThrows(Exception.class, () -> {
            // Code that should throw the exception
            JSONObject actual =
                    executeQuery(
                            String.format(
                                    "source=%s  | eval a = UNIX_TIMESTAMP('2020-15-26')",
                                    TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
        assertThrows(Exception.class, () -> {
            // Code that should throw the exception
            JSONObject actual =
                    executeQuery(
                            String.format(
                                    "source=%s  | eval a = UNIX_TIMESTAMP('2020-12-26 25:00:00')",
                                    TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
    }

    @Test
    public void testUnixTimestampNull() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s  |  eval timestamp = UNIX_TIMESTAMP(strict_date_optional_time), date=UNIX_TIMESTAMP(date), time=UNIX_TIMESTAMP(time) | fields timestamp, date, time",
                                TEST_INDEX_DATE_FORMATS_WITH_NULL));


        verifySchema(
                actual,
                schema("timestamp", "double"),
                schema("date", "double"),
                schema("time", "double"));
        JSONArray ret = (JSONArray) actual.getJSONArray("datarows").get(0);
        for (int i = 0; i < ret.length(); i++) {
            assertEquals(JSONObject.NULL, ret.get(i));
        }
    }

    @Test
    public void testToSecondsInvalid() {
        assertThrows(Exception.class, () -> {
            // Code that should throw the exception
            JSONObject actual =
                    executeQuery(
                            String.format(
                                    "source=%s  | eval a = UNIX_TIMESTAMP('2020-15-26')",
                                    TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
        assertThrows(Exception.class, () -> {
            // Code that should throw the exception
            JSONObject actual =
                    executeQuery(
                            String.format(
                                    "source=%s  | eval a = UNIX_TIMESTAMP('2020-12-26 25:00:00')",
                                    TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
    }

    @Test
    public void testToSecondsNull() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s  |  eval timestamp = SECOND(strict_date_optional_time), date=SECOND(date) | fields timestamp, date",
                                TEST_INDEX_DATE_FORMATS_WITH_NULL));


        verifySchema(
                actual,
                schema("timestamp", "integer"),
                schema("date", "integer"));
        JSONArray ret = (JSONArray) actual.getJSONArray("datarows").get(0);
        for (int i = 0; i < ret.length(); i++) {
            assertEquals(JSONObject.NULL, ret.get(i));
        }
    }

    @Test
    public void testDatetimeInvalid() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s  |  eval timestamp = DATETIME('2025-12-01 15:02:61'), date=DATETIME('2025-12-02'), time=DATETIME('16:00:61'), convert1= DATETIME('2025-12-01 12:02:61') " +
                                        "| fields timestamp, date, time, convert1",
                                TEST_INDEX_DATE_FORMATS_WITH_NULL));


        verifySchema(
                actual,
                schema("timestamp", "timestamp"),
                schema("date", "timestamp"),
                schema("time", "timestamp"),
                schema("convert1", "timestamp"));
        JSONArray ret = (JSONArray) actual.getJSONArray("datarows").get(0);
        for (int i = 0; i < ret.length(); i++) {
            assertEquals(JSONObject.NULL, ret.get(i));
        }
    }

    @Test
    public void testStrTDateInvalid1() {
        assertThrows(Exception.class, () -> {
            // Code that should throw the exception
            JSONObject actual =
                    executeQuery(
                            String.format(
                                    "source=%s  | eval a = str_to_date('01,13,2013', '%%d,%%m,%%Y')",
                                    TEST_INDEX_DATE_FORMATS_WITH_NULL));
        });
    }

    @Test
    public void testStrTDateInvalid2() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s  |  eval timestamp = STR_TO_DATE('2025-13-02', '2025-13-02')" +
                                        "| fields timestamp",
                                TEST_INDEX_DATE_FORMATS_WITH_NULL));


        verifySchema(
                actual,
                schema("timestamp", "timestamp"));
        JSONArray ret = (JSONArray) actual.getJSONArray("datarows").get(0);
        for (int i = 0; i < ret.length(); i++) {
            assertEquals(JSONObject.NULL, ret.get(i));
        }
    }

    @Test
    public void testConvertTZInvalid() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s  |  eval  a =CONVERT_TZ('2025-13-02', '+10:00', '-10:00'), b =CONVERT_TZ('2025-10-02', '+10:00', '-10:00'), c =CONVERT_TZ('2025-12-02 10:61:61', '+10:00', '-10:00'), " +
                                        "d = CONVERT_TZ('2025-12-02 12:61:61', '+10:00:00', '-10:00')" +
                                        "| fields a, b, c, d",
                                TEST_INDEX_DATE_FORMATS_WITH_NULL));


        verifySchema(
                actual,
                schema("a", "timestamp"),
                schema("b", "timestamp"),
                schema("c", "timestamp"),
                schema("d", "timestamp"));
        JSONArray ret = (JSONArray) actual.getJSONArray("datarows").get(0);
        for (int i = 0; i < ret.length(); i++) {
            assertEquals(JSONObject.NULL, ret.get(i));
        }
    }


}
