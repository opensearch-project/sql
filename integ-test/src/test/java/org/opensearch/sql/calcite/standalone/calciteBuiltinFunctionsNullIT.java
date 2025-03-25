package org.opensearch.sql.calcite.standalone;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

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
                rows(null));
    }



}
