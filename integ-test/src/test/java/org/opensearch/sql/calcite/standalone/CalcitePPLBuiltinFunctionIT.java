/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.calcite.standalone;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.time.Instant;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Objects;

import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_OCCUPATION;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

public class CalcitePPLBuiltinFunctionIT extends CalcitePPLIntegTestCase {
    @Override
    public void init() throws IOException {
        super.init();
        loadIndex(Index.STATE_COUNTRY);
    }

    @Test
    public void testSqrtAndPow() {
        JSONObject actual = executeQuery(
                String.format(
                        "source=%s | where sqrt(pow(age, 2)) = 30.0 | fields name, age",
                        TEST_INDEX_STATE_COUNTRY));

        verifySchema(
                actual,
                schema("name", "string"),
                schema("age", "integer"));

        verifyDataRows(
                actual,
                rows("Hello", 30));
    }


}
