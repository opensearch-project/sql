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
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.TestsConstants;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLEventstatsIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();

    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
    loadIndex(Index.BANK_TWO);
  }

  @Test
  public void testEventstat() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max | fields name, country, state, month, year, age, cnt, avg, min, max",
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
        rows("John", "Canada", "Ontario", 4, 2023, 25, 4, 36.25, 20, 70),
        rows("Jake", "USA", "California", 4, 2023, 70, 4, 36.25, 20, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4, 36.25, 20, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 4, 36.25, 20, 70));
  }

  @Test
  public void testEventstatWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max | fields name, country, state, month, year, age, cnt, avg, min, max",
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
        rows(null, "Canada", null, 4, 2023, 10, 6, 31.0, 10, 70),
        rows("Kevin", null, null, 4, 2023, null, 6, 31, 10, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 6, 31.0, 10, 70),
        rows("Jake", "USA", "California", 4, 2023, 70, 6, 31.0, 10, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 6, 31.0, 10, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 6, 31.0, 10, 70));
  }

  @Test
  public void testEventstatBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                + " as max by country | fields name, country, state, month, year, age, cnt,"
                + " avg, min, max",
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
        rows("John", "Canada", "Ontario", 4, 2023, 25, 2, 22.5, 20, 25),
        rows("Jake", "USA", "California", 4, 2023, 70, 2, 50, 30, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50, 30, 70));
  }

  @Test
  public void testEventstatByWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                + " as max by country | fields name, country, state, month, year, age, cnt,"
                + " avg, min, max",
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
        rows("Kevin", null, null, 4, 2023, null, 1, null, null, null),
        rows(null, "Canada", null, 4, 2023, 10, 3, 18.333333333333332, 10, 25),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 3, 18.333333333333332, 10, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 3, 18.333333333333332, 10, 25),
        rows("Jake", "USA", "California", 4, 2023, 70, 2, 50, 30, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50, 30, 70));

    actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                + " as max by state | fields name, country, state, month, year, age, cnt, avg,"
                + " min, max",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));
    verifyDataRows(
        actual,
        rows(null, "Canada", null, 4, 2023, 10, 2, 10, 10, 10),
        rows("Kevin", null, null, 4, 2023, null, 2, 10, 10, 10),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 1, 20, 20, 20),
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30));
  }

  @Test
  public void testEventstatBySpan() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                + " as max by span(age, 10) as age_span | fields name, country, state, month,"
                + " year, age, cnt, avg, min, max",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("John", "Canada", "Ontario", 4, 2023, 25, 2, 22.5, 20, 25),
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30));
  }

  @Test
  public void testEventstatBySpanWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                + " as max by span(age, 10) as age_span | fields name, country, state, month,"
                + " year, age, cnt, avg, min, max",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows(null, "Canada", null, 4, 2023, 10, 1, 10, 10, 10),
        rows("Kevin", null, null, 4, 2023, null, 1, null, null, null),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 2, 22.5, 20, 25),
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30));
  }

  @Test
  public void testEventstatByMultiplePartitions1() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                + " as max by span(age, 10) as age_span, country | fields name, country, state,"
                + " month, year, age, cnt, avg, min, max",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("John", "Canada", "Ontario", 4, 2023, 25, 2, 22.5, 20, 25),
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30));
  }

  @Test
  public void testEventstatByMultiplePartitions2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                + " as max by span(age, 10) as age_span, state | fields name, country, state,"
                + " month, year, age, cnt, avg, min, max",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 1, 20, 20, 20),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30));
  }

  @Test
  public void testEventstatByMultiplePartitionsWithNull1() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                + " as max by span(age, 10) as age_span, country | fields name, country, state,"
                + " month, year, age, cnt, avg, min, max",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows(null, "Canada", null, 4, 2023, 10, 1, 10, 10, 10),
        rows("Kevin", null, null, 4, 2023, null, 1, null, null, null),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 2, 22.5, 20, 25),
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30));
  }

  @Test
  public void testEventstatByMultiplePartitionsWithNull2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                + " as max by span(age, 10) as age_span, state | fields name, country, state,"
                + " month, year, age, cnt, avg, min, max",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows(null, "Canada", null, 4, 2023, 10, 1, 10, 10, 10),
        rows("Kevin", null, null, 4, 2023, null, 1, null, null, null),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 1, 20, 20, 20),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30));
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
                          "source=%s | eventstats %s(age)", TEST_INDEX_STATE_COUNTRY, u)));
      verifyErrorMessageContains(e, "Unexpected window function: " + u);
    }
  }

  @Ignore("DC should fail in window function")
  public void testDistinctCountShouldFail() throws IOException {
    Request request1 =
        new Request("PUT", "/" + TestsConstants.TEST_INDEX_STATE_COUNTRY + "/_doc/5?refresh=true");
    request1.setJsonEntity(
        "{\"name\":\"Jim\",\"age\":27,\"state\":\"Ontario\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request1);
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats distinct_count(state) by country",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("John", "Canada", "Ontario", 4, 2023, 25, 3),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 3),
        rows("Jim", "Canada", "Ontario", 4, 2023, 27, 3),
        rows("Jake", "USA", "California", 4, 2023, 70, 2),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2));
  }

  @Test
  public void testMultipleEventstat() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats avg(age) as avg_age by state, country | eventstats"
                + " avg(avg_age) as avg_state_age by country | fields name, country, state,"
                + " month, year, age, avg_age, avg_state_age",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20.0, 22.5),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25.0, 22.5),
        rows("Jake", "USA", "California", 4, 2023, 70, 70.0, 50.0),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30.0, 50.0));
  }

  @Test
  public void testMultipleEventstatWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats avg(age) as avg_age by state, country | eventstats"
                + " avg(avg_age) as avg_state_age by country | fields name, country, state,"
                + " month, year, age, avg_age, avg_state_age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Kevin", null, null, 4, 2023, null, null, null),
        rows(null, "Canada", null, 4, 2023, 10, 10, 18.333333333333332),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20.0, 18.333333333333332),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25.0, 18.333333333333332),
        rows("Jake", "USA", "California", 4, 2023, 70, 70.0, 50.0),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30.0, 50.0));
  }

  @Test
  public void testMultipleEventstatWithEval() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats avg(age) as avg_age by country, state, name | eval"
                    + " avg_age_divide_20 = avg_age - 20 | eventstats avg(avg_age_divide_20) as"
                    + " avg_state_age by country, state | where avg_state_age > 0 | eventstats"
                + " count(avg_state_age) as count_country_age_greater_20 by country | fields"
                + " name, country, state, month, year, age, avg_age, avg_age_divide_20,"
                + " avg_state_age, count_country_age_greater_20",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25, 5, 5, 1),
        rows("Jake", "USA", "California", 4, 2023, 70, 70, 50, 50, 2),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30, 10, 10, 2));
  }

  @Test
  public void testEventstatEmptyRows() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where name = 'non-existed' | eventstats count(), avg(age), min(age),"
                    + " max(age), stddev_pop(age), stddev_samp(age), var_pop(age), var_samp(age)",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));
    verifyNumOfRows(actual, 0);

    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | where name = 'non-existed' | eventstats count(), avg(age), min(age),"
                    + " max(age), stddev_pop(age), stddev_samp(age), var_pop(age), var_samp(age) by"
                    + " country",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));
    verifyNumOfRows(actual2, 0);
  }

  @Test
  public void testEventstatVariance() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats stddev_pop(age), stddev_samp(age), var_pop(age),"
                + " var_samp(age) | fields name, country, state, month, year, age,"
                + " `stddev_pop(age)`, `stddev_samp(age)`, `var_pop(age)`, `var_samp(age)`",
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
        rows(
            "John",
            "Canada",
            "Ontario",
            4,
            2023,
            25,
            19.803724397193573,
            22.86737122335374,
            392.1875,
            522.9166666666666),
        rows(
            "Jake",
            "USA",
            "California",
            4,
            2023,
            70,
            19.803724397193573,
            22.86737122335374,
            392.1875,
            522.9166666666666),
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
        rows(
            "Hello",
            "USA",
            "New York",
            4,
            2023,
            30,
            19.803724397193573,
            22.86737122335374,
            392.1875,
            522.9166666666666));
  }

  @Test
  public void testEventstatVarianceWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats stddev_pop(age), stddev_samp(age), var_pop(age),"
                + " var_samp(age) | fields name, country, state, month, year, age,"
                + " `stddev_pop(age)`, `stddev_samp(age)`, `var_pop(age)`, `var_samp(age)`",
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
        rows(null, "Canada", null, 4, 2023, 10, 20.591260281974, 23.021728866442675, 424, 530),
        rows("Kevin", null, null, 4, 2023, null, 20.591260281974, 23.021728866442675, 424, 530),
        rows(
            "John",
            "Canada",
            "Ontario",
            4,
            2023,
            25,
            20.591260281974,
            23.021728866442675,
            424,
            530),
        rows(
            "Jake",
            "USA",
            "California",
            4,
            2023,
            70,
            20.591260281974,
            23.021728866442675,
            424,
            530),
        rows(
            "Jane", "Canada", "Quebec", 4, 2023, 20, 20.591260281974, 23.021728866442675, 424, 530),
        rows(
            "Hello",
            "USA",
            "New York",
            4,
            2023,
            30,
            20.591260281974,
            23.021728866442675,
            424,
            530));
  }

  @Test
  public void testEventstatVarianceBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats stddev_pop(age), stddev_samp(age), var_pop(age),"
                + " var_samp(age) by country | fields name, country, state, month, year, age,"
                + " `stddev_pop(age)`, `stddev_samp(age)`, `var_pop(age)`, `var_samp(age)`",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("John", "Canada", "Ontario", 4, 2023, 25, 2.5, 3.5355339059327378, 6.25, 12.5),
        rows("Jake", "USA", "California", 4, 2023, 70, 20, 28.284271247461902, 400, 800),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2.5, 3.5355339059327378, 6.25, 12.5),
        rows("Hello", "USA", "New York", 4, 2023, 30, 20, 28.284271247461902, 400, 800));
  }

  @Test
  public void testEventstatVarianceBySpan() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                    "source=%s | where country != 'USA' | eventstats stddev_samp(age) by span(age, 10)"
                    + " | fields name, country, state, month, year, age, `stddev_samp(age)`",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("John", "Canada", "Ontario", 4, 2023, 25, 3.5355339059327378),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 3.5355339059327378));
  }

  @Test
  public void testEventstatVarianceWithNullBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats stddev_pop(age), stddev_samp(age), var_pop(age),"
                + " var_samp(age) by country | fields name, country, state, month, year, age,"
                + " `stddev_pop(age)`, `stddev_samp(age)`, `var_pop(age)`, `var_samp(age)`",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
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
        rows("Kevin", null, null, 4, 2023, null, null, null, null, null),
        rows(
            "John",
            "Canada",
            "Ontario",
            4,
            2023,
            25,
            6.2360956446232345,
            7.6376261582597325,
            38.88888888888888,
            58.333333333333314),
        rows("Jake", "USA", "California", 4, 2023, 70, 20, 28.284271247461902, 400, 800),
        rows(
            "Jane",
            "Canada",
            "Quebec",
            4,
            2023,
            20,
            6.2360956446232345,
            7.6376261582597325,
            38.88888888888888,
            58.333333333333314),
        rows("Hello", "USA", "New York", 4, 2023, 30, 20, 28.284271247461902, 400, 800));
  }

  @Ignore
  @Test
  public void testEventstatEarliestAndLatest() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats earliest(birthdate), latest(birthdate) | head 1",
                TEST_INDEX_BANK_TWO));
    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("firstname", "string"),
        schema("address", "string"),
        schema("birthdate", "timestamp"),
        schema("gender", "string"),
        schema("city", "string"),
        schema("lastname", "string"),
        schema("balance", "bigint"),
        schema("employer", "string"),
        schema("state", "string"),
        schema("age", "int"),
        schema("email", "string"),
        schema("male", "boolean"),
        schema("earliest(birthdate)", "timestamp"),
        schema("latest(birthdate)", "timestamp"));
    verifyDataRows(
        actual,
        rows(
            1,
            "Amber JOHnny",
            "880 Holmes Lane",
            "2017-10-23 00:00:00",
            "M",
            "Brogan",
            "Duke Willmington",
            39225,
            "Pyrami",
            "IL",
            32,
            "amberduke@pyrami.com",
            true,
            "1970-01-18 20:22:32",
            "2018-08-19 00:00:00"));
  }
}
