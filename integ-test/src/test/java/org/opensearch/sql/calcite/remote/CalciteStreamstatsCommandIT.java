/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import java.util.List;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteStreamstatsCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
    loadIndex(Index.BANK_TWO);
    loadIndex(Index.LOGS);
  }

  @Test
  public void testStreamstats() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max",
                TEST_INDEX_STATE_COUNTRY));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("cnt", "bigint"),
        schema("avg", "double"),
        schema("min", "int"),
        schema("max", "int"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 3, 41.666666666666664, 25, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4, 36.25, 20, 70));
  }

  @Test
  public void testStreamstatsWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("cnt", "bigint"),
        schema("avg", "double"),
        schema("min", "int"),
        schema("max", "int"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 3, 41.666666666666664, 25, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4, 36.25, 20, 70),
        rows(null, "Canada", null, 4, 2023, 10, 5, 31, 10, 70),
        rows("Kevin", null, null, 4, 2023, null, 6, 31, 10, 70));
  }

  @Test
  public void testStreamstatsBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by country",
                TEST_INDEX_STATE_COUNTRY));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("cnt", "bigint"),
        schema("avg", "double"),
        schema("min", "int"),
        schema("max", "int"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25));
  }

  @Test
  public void testStreamstatsByWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by country",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("cnt", "bigint"),
        schema("avg", "double"),
        schema("min", "int"),
        schema("max", "int"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows(null, "Canada", null, 4, 2023, 10, 3, 18.333333333333332, 10, 25),
        rows("Kevin", null, null, 4, 2023, null, 1, null, null, null));

    actual =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by state",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));
    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 1, 20, 20, 20),
        rows(null, "Canada", null, 4, 2023, 10, 1, 10, 10, 10),
        rows("Kevin", null, null, 4, 2023, null, 2, 10, 10, 10));
  }

  @Test
  public void testStreamstatsBySpan() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by span(age, 10) as age_span",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25));
  }

  @Test
  public void testStreamstatsBySpanWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by span(age, 10) as age_span",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows(null, "Canada", null, 4, 2023, 10, 1, 10, 10, 10),
        rows("Kevin", null, null, 4, 2023, null, 1, null, null, null));
  }

  @Test
  public void testStreamstatsByMultiplePartitions1() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by span(age, 10) as age_span, country",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25));
  }

  @Test
  public void testStreamstatsByMultiplePartitions2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by span(age, 10) as age_span, state",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 1, 20, 20, 20));
  }

  @Test
  public void testStreamstatsByMultiplePartitionsWithNull1() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by span(age, 10) as age_span, country",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows(null, "Canada", null, 4, 2023, 10, 1, 10, 10, 10),
        rows("Kevin", null, null, 4, 2023, null, 1, null, null, null));
  }

  @Test
  public void testStreamstatsByMultiplePartitionsWithNull2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by span(age, 10) as age_span, state",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 1, 20, 20, 20),
        rows(null, "Canada", null, 4, 2023, 10, 1, 10, 10, 10),
        rows("Kevin", null, null, 4, 2023, null, 1, null, null, null));
  }

  @Test
  public void testStreamstatsCurrent() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats current=false avg(age) as prev_avg",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 50),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 41.666666666666664));
  }

  @Test
  public void testStreamstatsCurrentWithNUll() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats current=false avg(age) as prev_avg",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 50),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 41.666666666666664),
        rows(null, "Canada", null, 4, 2023, 10, 36.25),
        rows("Kevin", null, null, 4, 2023, null, 31));
  }

  @Test
  public void testStreamstatsWindow() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats window = 3 avg(age) as avg", TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 41.666666666666664),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 25));
  }

  @Test
  public void testStreamstatsWindowWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats window = 3 avg(age) as avg",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 41.666666666666664),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 25),
        rows(null, "Canada", null, 4, 2023, 10, 18.333333333333332),
        rows("Kevin", null, null, 4, 2023, null, 15));
  }

  public void testStreamstatsBigWindow() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats window = 10 avg(age) as avg", TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 41.666666666666664),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 36.25));
  }

  @Test
  public void testStreamstatsWindowError() {
    Throwable e =
        assertThrowsWithReplace(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | streamstats window=-1 avg(age) as avg",
                        TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(e, "Window size must be >= 0, but got: -1");
  }

  @Test
  public void testStreamstatsCurrentAndWindow() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats current = false window = 2 avg(age) as avg",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 50),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 27.5));
  }

  @Test
  public void testStreamstatsCurrentAndWindowWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats current = false window = 2 avg(age) as avg",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 50),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 27.5),
        rows(null, "Canada", null, 4, 2023, 10, 22.5),
        rows("Kevin", null, null, 4, 2023, null, 15));
  }

  @Test
  public void testStreamstatsGlobal() throws IOException {
    final int docId = 5;
    Request insertRequest =
        new Request(
            "PUT", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_STATE_COUNTRY, docId));
    insertRequest.setJsonEntity(
        "{\"name\": \"Jay\",\"age\": 40,\"state\":"
            + " \"Quebec\",\"country\": \"USA\",\"year\": 2023,\"month\":"
            + " 4}\n");
    client().performRequest(insertRequest);

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats window=2 global=false avg(age) as avg by country",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 22.5),
        rows("Jay", "USA", "Quebec", 4, 2023, 40, 35));

    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | streamstats window=2 global=true avg(age) as avg by country",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual2,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 22.5),
        rows("Jay", "USA", "Quebec", 4, 2023, 40, 40));

    Request deleteRequest =
        new Request(
            "DELETE", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_STATE_COUNTRY, docId));
    client().performRequest(deleteRequest);
  }

  @Test
  public void testStreamstatsGlobalWithNull() throws IOException {
    final int docId = 7;
    Request insertRequest =
        new Request(
            "PUT",
            String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_STATE_COUNTRY_WITH_NULL, docId));
    insertRequest.setJsonEntity(
        "{\"name\": \"Jay\",\"age\": 40,\"state\":"
            + " \"Quebec\",\"country\": \"USA\",\"year\": 2023,\"month\":"
            + " 4}\n");
    client().performRequest(insertRequest);

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats window=2 global=false avg(age) as avg by country",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 22.5),
        rows(null, "Canada", null, 4, 2023, 10, 15),
        rows("Kevin", null, null, 4, 2023, null, null),
        rows("Jay", "USA", "Quebec", 4, 2023, 40, 35));

    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | streamstats window=2 global=true avg(age) as avg by country",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual2,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 22.5),
        rows(null, "Canada", null, 4, 2023, 10, 15),
        rows("Kevin", null, null, 4, 2023, null, null),
        rows("Jay", "USA", "Quebec", 4, 2023, 40, 40));

    Request deleteRequest =
        new Request(
            "DELETE",
            String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_STATE_COUNTRY_WITH_NULL, docId));
    client().performRequest(deleteRequest);
  }

  @Test
  public void testStreamstatsReset() throws IOException {
    final int docId = 5;
    Request insertRequest =
        new Request(
            "PUT", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_STATE_COUNTRY, docId));
    insertRequest.setJsonEntity(
        "{\"name\": \"Jay\",\"age\": 28,\"state\":"
            + " \"Quebec\",\"country\": \"USA\",\"year\": 2023,\"month\":"
            + " 4}\n");
    client().performRequest(insertRequest);

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats window=2 reset_before=age>29 avg(age) as avg by country",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 22.5),
        rows("Jay", "USA", "Quebec", 4, 2023, 28, 28));

    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | streamstats window=2 reset_after=age>22 avg(age) as avg by country",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual2,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20),
        rows("Jay", "USA", "Quebec", 4, 2023, 28, 28));

    Request deleteRequest =
        new Request(
            "DELETE", String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_STATE_COUNTRY, docId));
    client().performRequest(deleteRequest);
  }

  @Test
  public void testStreamstatsResetWithNull() throws IOException {
    final int docId = 7;
    Request insertRequest =
        new Request(
            "PUT",
            String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_STATE_COUNTRY_WITH_NULL, docId));
    insertRequest.setJsonEntity(
        "{\"name\": \"Jay\",\"age\": 28,\"state\":"
            + " \"Quebec\",\"country\": \"USA\",\"year\": 2023,\"month\":"
            + " 4}\n");
    client().performRequest(insertRequest);

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats window=2 reset_before=age>29 avg(age) as avg by country",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 22.5),
        rows(null, "Canada", null, 4, 2023, 10, 15),
        rows("Kevin", null, null, 4, 2023, null, null),
        rows("Jay", "USA", "Quebec", 4, 2023, 28, 28));

    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | streamstats window=2 reset_after=age>22 avg(age) as avg by country",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual2,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20),
        rows(null, "Canada", null, 4, 2023, 10, 15),
        rows("Kevin", null, null, 4, 2023, null, null),
        rows("Jay", "USA", "Quebec", 4, 2023, 28, 28));

    Request deleteRequest =
        new Request(
            "DELETE",
            String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_STATE_COUNTRY_WITH_NULL, docId));
    client().performRequest(deleteRequest);
  }

  @Test
  public void testUnsupportedWindowFunctions() {
    List<String> unsupported = List.of("PERCENTILE_APPROX", "PERCENTILE");
    for (String u : unsupported) {
      Throwable e =
          assertThrowsWithReplace(
              UnsupportedOperationException.class,
              () ->
                  executeQuery(
                      String.format(
                          "source=%s | streamstats %s(age)", TEST_INDEX_STATE_COUNTRY, u)));
      verifyErrorMessageContains(e, "Unexpected window function: " + u);
    }
  }

  @Test
  public void testMultipleStreamstats() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats avg(age) as avg_age by state, country | streamstats"
                    + " avg(avg_age) as avg_state_age by country",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20, 22.5));
  }

  @Test
  public void testMultipleStreamstatsWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats avg(age) as avg_age by state, country | streamstats"
                    + " avg(avg_age) as avg_state_age by country",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20, 22.5),
        rows(null, "Canada", null, 4, 2023, 10, 10, 18.333333333333332),
        rows("Kevin", null, null, 4, 2023, null, null, null));
  }

  @Test
  public void testStreamstatsAndEventstats() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats avg(age) as avg_age| streamstats"
                    + " avg(age) as avg_age_stream",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 36.25, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 36.25, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 36.25, 41.666666666666664),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 36.25, 36.25));
  }

  @Test
  public void testStreamstatsAndSort() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort age | streamstats window = 2 avg(age) as avg_age ",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 22.5),
        rows("Hello", "USA", "New York", 4, 2023, 30, 27.5),
        rows("Jake", "USA", "California", 4, 2023, 70, 50));
  }

  @Test
  public void testLeftJoinWithStreamstats() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s as l | left join left=l right=r on l.country = r.country [ source=%s |"
                    + " streamstats window=2 avg(age) as avg_age]",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows(
            "John", "Canada", "Ontario", 4, 2023, 25, "John", "Canada", "Ontario", 4, 2023, 25,
            27.5),
        rows(
            "John", "Canada", "Ontario", 4, 2023, 25, "Jane", "Canada", "Quebec", 4, 2023, 20,
            22.5),
        rows("John", "Canada", "Ontario", 4, 2023, 25, null, "Canada", null, 4, 2023, 10, 15),
        rows(
            "Jane", "Canada", "Quebec", 4, 2023, 20, "John", "Canada", "Ontario", 4, 2023, 25,
            27.5),
        rows(
            "Jane", "Canada", "Quebec", 4, 2023, 20, "Jane", "Canada", "Quebec", 4, 2023, 20, 22.5),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, null, "Canada", null, 4, 2023, 10, 15),
        rows(
            "Jake", "USA", "California", 4, 2023, 70, "Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Jake", "USA", "California", 4, 2023, 70, "Hello", "USA", "New York", 4, 2023, 30, 50),
        rows("Hello", "USA", "New York", 4, 2023, 30, "Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, "Hello", "USA", "New York", 4, 2023, 30, 50));
  }

  @Test
  public void testWhereInWithStreamstatsSubquery() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where country in [ source=%s | streamstats window=2 avg(age) as"
                    + " avg_age | where avg_age > 40 | fields country ]",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30));
  }

  @Test
  public void testMultipleStreamstatsWithEval() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats avg(age) as avg_age by country, state, name | eval"
                    + " avg_age_divide_20 = avg_age - 20 | streamstats avg(avg_age_divide_20) as"
                    + " avg_state_age by country, state | where avg_state_age > 0 | streamstats"
                    + " count(avg_state_age) as count_country_age_greater_20 by country",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70, 50, 50, 1),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30, 10, 10, 2),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25, 5, 5, 1));
  }

  @Test
  public void testStreamstatsEmptyRows() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where name = 'non-existed' | streamstats count(), avg(age), min(age),"
                    + " max(age), stddev_pop(age), stddev_samp(age), var_pop(age), var_samp(age)",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));
    verifyNumOfRows(actual, 0);

    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | where name = 'non-existed' | streamstats count(), avg(age), min(age),"
                    + " max(age), stddev_pop(age), stddev_samp(age), var_pop(age), var_samp(age) by"
                    + " country",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));
    verifyNumOfRows(actual2, 0);
  }

  @Test
  public void testStreamstatsVariance() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats stddev_pop(age), stddev_samp(age), var_pop(age),"
                    + " var_samp(age)",
                TEST_INDEX_STATE_COUNTRY));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("stddev_pop(age)", "double"),
        schema("stddev_samp(age)", "double"),
        schema("var_pop(age)", "double"),
        schema("var_samp(age)", "double"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 0, null, 0, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 20, 28.284271247461902, 400, 800),
        rows(
            "John",
            "Canada",
            "Ontario",
            4,
            2023,
            25,
            20.138409955990955,
            24.66441431158124,
            405.55555555555566,
            608.3333333333335),
        rows(
            "Jane",
            "Canada",
            "Quebec",
            4,
            2023,
            20,
            19.803724397193573,
            22.86737122335374,
            392.1875,
            522.9166666666666));
  }

  @Test
  public void testStreamstatsVarianceWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats stddev_pop(age), stddev_samp(age), var_pop(age),"
                    + " var_samp(age)",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("stddev_pop(age)", "double"),
        schema("stddev_samp(age)", "double"),
        schema("var_pop(age)", "double"),
        schema("var_samp(age)", "double"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 0, null, 0, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 20, 28.284271247461902, 400, 800),
        rows(
            "John",
            "Canada",
            "Ontario",
            4,
            2023,
            25,
            20.138409955990955,
            24.66441431158124,
            405.55555555555566,
            608.3333333333335),
        rows(
            "Jane",
            "Canada",
            "Quebec",
            4,
            2023,
            20,
            19.803724397193573,
            22.86737122335374,
            392.1875,
            522.9166666666666),
        rows(null, "Canada", null, 4, 2023, 10, 20.591260281974, 23.021728866442675, 424, 530),
        rows("Kevin", null, null, 4, 2023, null, 20.591260281974, 23.021728866442675, 424, 530));
  }

  @Test
  public void testStreamstatsVarianceBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats stddev_pop(age), stddev_samp(age), var_pop(age),"
                    + " var_samp(age) by country",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 0, null, 0, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 20, 28.284271247461902, 400, 800),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 0, null, 0, null),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2.5, 3.5355339059327378, 6.25, 12.5));
  }

  @Test
  public void testStreamstatsVarianceBySpan() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where country != 'USA' | streamstats stddev_samp(age) by span(age,"
                    + " 10)",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("John", "Canada", "Ontario", 4, 2023, 25, null),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 3.5355339059327378));
  }

  @Test
  public void testStreamstatsVarianceWithNullBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats stddev_pop(age), stddev_samp(age), var_pop(age),"
                    + " var_samp(age) by country",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 0, null, 0, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 20, 28.284271247461902, 400, 800),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 0, null, 0, null),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2.5, 3.5355339059327378, 6.25, 12.5),
        rows(
            null,
            "Canada",
            null,
            4,
            2023,
            10,
            6.2360956446232345,
            7.6376261582597325,
            38.88888888888888,
            58.333333333333314),
        rows("Kevin", null, null, 4, 2023, null, null, null, null, null));
  }

  @Test
  public void testStreamstatsDistinctCount() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats dc(state) as dc_state", TEST_INDEX_STATE_COUNTRY));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("dc_state", "bigint"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 3),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4));
  }

  @Test
  public void testStreamstatsDistinctCountByCountry() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats dc(state) as dc_state by country",
                TEST_INDEX_STATE_COUNTRY));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("dc_state", "bigint"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2));
  }

  @Test
  public void testStreamstatsDistinctCountFunction() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats distinct_count(country) as dc_country",
                TEST_INDEX_STATE_COUNTRY));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("dc_country", "bigint"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 2),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2));
  }

  @Test
  public void testStreamstatsDistinctCountWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats dc(state) as dc_state",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("dc_state", "bigint"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 3),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4),
        rows(null, "Canada", null, 4, 2023, 10, 4),
        rows("Kevin", null, null, 4, 2023, null, 4));
  }

  @Test
  public void testStreamstatsEarliestAndLatest() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats earliest(message), latest(message) by server",
                TEST_INDEX_LOGS));
    verifySchema(
        actual,
        schema("created_at", "timestamp"),
        schema("server", "string"),
        schema("@timestamp", "timestamp"),
        schema("message", "string"),
        schema("level", "string"),
        schema("earliest(message)", "string"),
        schema("latest(message)", "string"));
    verifyDataRows(
        actual,
        rows(
            "2023-01-05 00:00:00",
            "server1",
            "2023-01-01 00:00:00",
            "Database connection failed",
            "ERROR",
            "Database connection failed",
            "Database connection failed"),
        rows(
            "2023-01-04 00:00:00",
            "server2",
            "2023-01-02 00:00:00",
            "Service started",
            "INFO",
            "Service started",
            "Service started"),
        rows(
            "2023-01-03 00:00:00",
            "server1",
            "2023-01-03 00:00:00",
            "High memory usage",
            "WARN",
            "Database connection failed",
            "High memory usage"),
        rows(
            "2023-01-02 00:00:00",
            "server3",
            "2023-01-04 00:00:00",
            "Disk space low",
            "ERROR",
            "Disk space low",
            "Disk space low"),
        rows(
            "2023-01-01 00:00:00",
            "server2",
            "2023-01-05 00:00:00",
            "Backup completed",
            "INFO",
            "Service started",
            "Backup completed"));
  }
}
