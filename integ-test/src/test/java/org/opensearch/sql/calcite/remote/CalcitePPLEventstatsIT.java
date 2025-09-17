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
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLEventstatsIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.BANK);
    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
    loadIndex(Index.LOGS);
  }

  @Test
  public void testEventstats() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
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
        rows("John", "Canada", "Ontario", 4, 2023, 25, 4, 36.25, 20, 70),
        rows("Jake", "USA", "California", 4, 2023, 70, 4, 36.25, 20, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4, 36.25, 20, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 4, 36.25, 20, 70));
  }

  @Test
  public void testEventstatsWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
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
        rows(null, "Canada", null, 4, 2023, 10, 6, 31.0, 10, 70),
        rows("Kevin", null, null, 4, 2023, null, 6, 31, 10, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 6, 31.0, 10, 70),
        rows("Jake", "USA", "California", 4, 2023, 70, 6, 31.0, 10, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 6, 31.0, 10, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 6, 31.0, 10, 70));
  }

  @Test
  public void testEventstatsBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
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
        rows("John", "Canada", "Ontario", 4, 2023, 25, 2, 22.5, 20, 25),
        rows("Jake", "USA", "California", 4, 2023, 70, 2, 50, 30, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50, 30, 70));
  }

  @Test
  public void testEventstatsByWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
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
                    + " as max by state",
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
  public void testEventstatsBySpan() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by span(age, 10) as age_span",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("John", "Canada", "Ontario", 4, 2023, 25, 2, 22.5, 20, 25),
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30));
  }

  @Test
  public void testEventstatsBySpanWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by span(age, 10) as age_span",
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
  public void testEventstatsByMultiplePartitions1() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by span(age, 10) as age_span, country",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("John", "Canada", "Ontario", 4, 2023, 25, 2, 22.5, 20, 25),
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30));
  }

  @Test
  public void testEventstatsByMultiplePartitions2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by span(age, 10) as age_span, state",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 1, 20, 20, 20),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30));
  }

  @Test
  public void testEventstatsByMultiplePartitionsWithNull1() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by span(age, 10) as age_span, country",
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
  public void testEventstatsByMultiplePartitionsWithNull2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by span(age, 10) as age_span, state",
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

  @Test
  public void testMultipleEventstats() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats avg(age) as avg_age by state, country | eventstats"
                    + " avg(avg_age) as avg_state_age by country",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20.0, 22.5),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25.0, 22.5),
        rows("Jake", "USA", "California", 4, 2023, 70, 70.0, 50.0),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30.0, 50.0));
  }

  @Test
  public void testMultipleEventstatsWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats avg(age) as avg_age by state, country | eventstats"
                    + " avg(avg_age) as avg_state_age by country",
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
  public void testMultipleEventstatsWithEval() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats avg(age) as avg_age by country, state, name | eval"
                    + " avg_age_divide_20 = avg_age - 20 | eventstats avg(avg_age_divide_20) as"
                    + " avg_state_age by country, state | where avg_state_age > 0 | eventstats"
                    + " count(avg_state_age) as count_country_age_greater_20 by country",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25, 5, 5, 1),
        rows("Jake", "USA", "California", 4, 2023, 70, 70, 50, 50, 2),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30, 10, 10, 2));
  }

  @Test
  public void testEventstatsEmptyRows() throws IOException {
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
  public void testEventstatsVariance() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats stddev_pop(age), stddev_samp(age), var_pop(age),"
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
  public void testEventstatsVarianceWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats stddev_pop(age), stddev_samp(age), var_pop(age),"
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
  public void testEventstatsVarianceBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats stddev_pop(age), stddev_samp(age), var_pop(age),"
                    + " var_samp(age) by country",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("John", "Canada", "Ontario", 4, 2023, 25, 2.5, 3.5355339059327378, 6.25, 12.5),
        rows("Jake", "USA", "California", 4, 2023, 70, 20, 28.284271247461902, 400, 800),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2.5, 3.5355339059327378, 6.25, 12.5),
        rows("Hello", "USA", "New York", 4, 2023, 30, 20, 28.284271247461902, 400, 800));
  }

  @Test
  public void testEventstatsVarianceBySpan() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where country != 'USA' | eventstats stddev_samp(age) by span(age, 10)",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("John", "Canada", "Ontario", 4, 2023, 25, 3.5355339059327378),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 3.5355339059327378));
  }

  @Test
  public void testEventstatsVarianceWithNullBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats stddev_pop(age), stddev_samp(age), var_pop(age),"
                    + " var_samp(age) by country",
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

  @Test
  public void testEventstatsDistinctCount() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats dc(state) as dc_state", TEST_INDEX_STATE_COUNTRY));

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
        rows("John", "Canada", "Ontario", 4, 2023, 25, 4),
        rows("Jake", "USA", "California", 4, 2023, 70, 4),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4),
        rows("Hello", "USA", "New York", 4, 2023, 30, 4));
  }

  @Test
  public void testEventstatsDistinctCountByCountry() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats dc(state) as dc_state by country",
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
        rows("John", "Canada", "Ontario", 4, 2023, 25, 2),
        rows("Jake", "USA", "California", 4, 2023, 70, 2),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2));
  }

  @Test
  public void testEventstatsDistinctCountFunction() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats distinct_count(country) as dc_country",
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
        rows("John", "Canada", "Ontario", 4, 2023, 25, 2),
        rows("Jake", "USA", "California", 4, 2023, 70, 2),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2));
  }

  @Test
  public void testEventstatsDistinctCountWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats dc(state) as dc_state",
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
        rows(null, "Canada", null, 4, 2023, 10, 4),
        rows("Kevin", null, null, 4, 2023, null, 4),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 4),
        rows("Jake", "USA", "California", 4, 2023, 70, 4),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4),
        rows("Hello", "USA", "New York", 4, 2023, 30, 4));
  }

  @Test
  public void testEventstatsEarliestAndLatest() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eventstats earliest(message), latest(message) by server",
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
            "High memory usage"),
        rows(
            "2023-01-04 00:00:00",
            "server2",
            "2023-01-02 00:00:00",
            "Service started",
            "INFO",
            "Service started",
            "Backup completed"),
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
