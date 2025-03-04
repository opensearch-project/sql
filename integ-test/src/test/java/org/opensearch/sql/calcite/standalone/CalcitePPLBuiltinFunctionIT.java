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
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;

public class CalcitePPLBuiltinFunctionIT extends CalcitePPLIntegTestCase {
    @Override
    public void init() throws IOException {
        super.init();
        Request request1 = new Request("PUT", "/test/_doc/1?refresh=true");
        request1.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
        client().performRequest(request1);
        Request request2 = new Request("PUT", "/test/_doc/2?refresh=true");
        request2.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
        client().performRequest(request2);
        Request request3 = new Request("PUT", "/test_name_null/_doc/1?refresh=true");
        request3.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
        client().performRequest(request3);
        Request request4 = new Request("PUT", "/test_name_null/_doc/2?refresh=true");
        request4.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
        client().performRequest(request4);
        Request request5 = new Request("PUT", "/test_name_null/_doc/3?refresh=true");
        request5.setJsonEntity("{\"name\": null, \"age\": 30}");
        client().performRequest(request5);

        Request request6 = new Request("PUT", "/people/_doc/2?refresh=true");
        request6.setJsonEntity("{\"name\": \"DummyEntityForMathVerification\", \"age\": 24}");
        client().performRequest(request6);
        loadIndex(Index.BANK);
    }

    @Test
    public void testTakeAggregation() {
        String actual = execute("source=test | stats take(name, 1)");
        assertEquals(
                "{\n"
                        + "  \"schema\": [\n"
                        + "    {\n"
                        + "      \"name\": \"take(name, 1)\",\n"
                        + "      \"type\": \"array\"\n"
                        + "    }\n"
                        + "  ],\n"
                        + "  \"datarows\": [\n"
                        + "    [\n"
                        + "      [\n        \"hello\"\n      ]\n"
                        + "    ]\n"
                        + "  ],\n"
                        + "  \"total\": 1,\n"
                        + "  \"size\": 1\n"
                        + "}",
                actual);
    }

    @Test
    public void testSqrt() {
        testSimplePPL(
                "source=people | eval `SQRT(4)` = SQRT(4), `SQRT(4.41)` = SQRT(4.41) | fields `SQRT(4)`,"
                        + " `SQRT(4.41)`",
                List.of(Math.sqrt(4), Math.sqrt(4.41)));
    }

    private static JsonArray parseAndGetFirstDataRow(String executionResult) {
        JsonObject sqrtResJson = JsonParser.parseString(executionResult).getAsJsonObject();
        JsonArray dataRows = sqrtResJson.getAsJsonArray("datarows");
        return dataRows.get(0).getAsJsonArray();
    }

    private void testSimplePPL(String query, List<Object> expectedValues) {
        String execResult = execute(query);
        JsonArray dataRow = parseAndGetFirstDataRow(execResult);
        assertEquals(expectedValues.size(), dataRow.size());
        for (int i = 0; i < expectedValues.size(); i++) {
            Object expected = expectedValues.get(i);
            if (Objects.isNull(expected)) {
                Object actual = dataRow.get(i);
                assertNull(actual);
            } else if (expected instanceof BigDecimal) {
                Number actual = dataRow.get(i).getAsNumber();
                assertEquals(expected, actual);
            } else if (expected instanceof Double || expected instanceof Float) {
                Number actual = dataRow.get(i).getAsNumber();
                assertDoubleUlpEquals(((Number) expected).doubleValue(), actual.doubleValue(), 8);
            } else if (expected instanceof Long || expected instanceof Integer) {
                Number actual = dataRow.get(i).getAsNumber();
                assertEquals(((Number) expected).longValue(), actual.longValue());
            } else if (expected instanceof String) {
                String actual = dataRow.get(i).getAsString();
                assertEquals(expected, actual);
            } else if (expected instanceof Boolean) {
                Boolean actual = dataRow.get(i).getAsBoolean();
                assertEquals(expected, actual);
            } else {
                fail("Unsupported number type: " + expected.getClass().getName());
            }
        }
    }


}
