package org.opensearch.sql.calcite.standalone;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.SemanticCheckException;

import java.io.IOException;
import java.sql.Date;

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

        verifyDataRows(
                actual,
                rows((Object) null));
    }

    /**
     * (DATE/TIMESTAMP/TIME, INTERVAL) -> TIMESTAMP
     *
     * (DATE, LONG) -> DATE
     *
     * (TIMESTAMP/TIME, LONG) -> TIMESTAMP
     */
    @Test
    public void testAddSubDateNull() {
        JSONObject actual =
                executeQuery(String.format(
                        "source=%s  |  eval n1 = ADDDATE(date_time, INTERVAL 1 DAY), " +
                                "n2 = ADDDATE(date, 1), n3 = SUBDATE(time, 1) | fields n1, n2, n3",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL));

        verifySchema(actual, schema("n1", "timestamp"),
                schema("n2", "date"),
                schema("n3", "timestamp"));
        verifyDataRows(actual, rows(null, null, null));
    }

    /**
     * (DATE/TIMESTAMP, DATE/TIMESTAMP/TIME) -> TIMESTAMP
     *
     * (TIME, DATE/TIMESTAMP/TIME) -> TIME
     */
    @Test
    public void testAddTimeNull() {
        JSONObject actual =
                executeQuery(String.format(
                        "source=%s  |  eval n1 = ADDTIME(date_time, date_time), " +
                                "n2 = ADDTIME(date, date), n3 = ADDTIME(time, time) | fields n1, n2, n3",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL));
        verifySchema(actual, schema("n1", "timestamp"),
                schema("n2", "timestamp"),
                schema("n3", "time"));
        verifyDataRows(actual, rows(null, null, null));
    }

    /**
     * (DATE/TIMESTAMP/TIME, INTERVAL) -> TIMESTAMP
     */
    @Test
    public void testDateAddSubNull() {
        JSONObject actual =
                executeQuery(String.format(
                        "source=%s  |  eval n1 = DATE_ADD(date_time, INTERVAL 1 DAY), " +
                                "n2 = DATE_ADD(date, INTERVAL 1 DAY), n3 = DATE_SUB(time, INTERVAL 1 DAY) | fields n1, n2, n3",
                        TEST_INDEX_DATE_FORMATS_WITH_NULL));
        verifySchema(actual, schema("n1", "timestamp"),
                schema("n2", "timestamp"),
                schema("n3", "timestamp"));
        verifyDataRows(actual, rows(null, null, null));
    }

    /*
    STRING/DATE/TIMESTAMP
     */
    @Test
    public void testDateNull() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s  |  eval d1 = DATE(date), d2 = DATE(date_time) | fields d1, d2",
                                TEST_INDEX_DATE_FORMATS_WITH_NULL));

        verifySchema(actual, schema("d1", "date"), schema("d2", "date"));

        verifyDataRows(actual, rows(null, null));
    }

    @Test
    public void testDateInvalid() {
        Exception semanticException = assertThrows(
                SemanticCheckException.class,
                () -> executeQuery(
                        String.format(
                                "source=%s  | eval d1 = DATE('2020-08-26'), d2 = DATE('2020-15-26') | fields d1, d2",
                                TEST_INDEX_DATE_FORMATS_WITH_NULL)));
        verifyErrorMessageContains(semanticException, "date:2020-15-26 in unsupported format, please use 'yyyy-MM-dd'");
    }

    /**
     * STRING/TIME/TIMESTAMP -> INTEGER
     */
    @Test
    public void testHourNull() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s  |  eval h2 = HOUR(date_time), h3 = HOUR(time) | fields h2, h3",
                                TEST_INDEX_DATE_FORMATS_WITH_NULL));
        verifySchema(actual, schema("h2", "integer"), schema("h3", "integer"));
        verifyDataRows(actual, rows(null, null));
    }

    @Test
    public void testTimeInvalid() {
        assertThrows(
                SemanticCheckException.class,
                () -> executeQuery(
                        String.format(
                                "source=%s  | eval t1 = TIME('13:69:00') | fields t1",
                                TEST_INDEX_DATE_FORMATS_WITH_NULL)));
    }
}
