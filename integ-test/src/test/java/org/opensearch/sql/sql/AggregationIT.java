/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NULL_MISSING;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySome;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class AggregationIT extends SQLIntegTestCase {
  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.NULL_MISSING);
    loadIndex(Index.CALCS);
  }

  @Test
  public void testFilteredAggregatePushDown() throws IOException {
    JSONObject response =
        executeQuery("SELECT COUNT(*) FILTER(WHERE age > 35) FROM " + TEST_INDEX_BANK);
    verifySchema(response, schema("COUNT(*) FILTER(WHERE age > 35)", null, "integer"));
    verifyDataRows(response, rows(3));
  }

  @Test
  public void testFilteredAggregateNotPushDown() throws IOException {
    JSONObject response =
        executeQuery(
            "SELECT COUNT(*) FILTER(WHERE age > 35) FROM (SELECT * FROM "
                + TEST_INDEX_BANK
                + ") AS a");
    verifySchema(response, schema("COUNT(*) FILTER(WHERE age > 35)", null, "integer"));
    verifyDataRows(response, rows(3));
  }

  @Test
  public void testPushDownAggregationOnNullValues() throws IOException {
    // OpenSearch aggregation query (MetricAggregation)
    var response =
        executeQuery(
            String.format(
                "SELECT min(`int`), max(`int`), avg(`int`), min(`dbl`), max(`dbl`), avg(`dbl`) "
                    + "FROM %s WHERE `key` = 'null'",
                TEST_INDEX_NULL_MISSING));
    verifySchema(
        response,
        schema("min(`int`)", null, "integer"),
        schema("max(`int`)", null, "integer"),
        schema("avg(`int`)", null, "double"),
        schema("min(`dbl`)", null, "double"),
        schema("max(`dbl`)", null, "double"),
        schema("avg(`dbl`)", null, "double"));
    verifyDataRows(response, rows(null, null, null, null, null, null));
  }

  @Test
  public void testPushDownAggregationOnMissingValues() throws IOException {
    // OpenSearch aggregation query (MetricAggregation)
    var response =
        executeQuery(
            String.format(
                "SELECT min(`int`), max(`int`), avg(`int`), min(`dbl`), max(`dbl`), avg(`dbl`) "
                    + "FROM %s WHERE `key` = 'null'",
                TEST_INDEX_NULL_MISSING));
    verifySchema(
        response,
        schema("min(`int`)", null, "integer"),
        schema("max(`int`)", null, "integer"),
        schema("avg(`int`)", null, "double"),
        schema("min(`dbl`)", null, "double"),
        schema("max(`dbl`)", null, "double"),
        schema("avg(`dbl`)", null, "double"));
    verifyDataRows(response, rows(null, null, null, null, null, null));
  }

  @Test
  public void testInMemoryAggregationOnNullValues() throws IOException {
    // In-memory aggregation performed by the plugin
    var response =
        executeQuery(
            String.format(
                "SELECT"
                    + " min(`int`) over (PARTITION BY `key`), max(`int`) over (PARTITION BY `key`),"
                    + " avg(`int`) over (PARTITION BY `key`), min(`dbl`) over (PARTITION BY `key`),"
                    + " max(`dbl`) over (PARTITION BY `key`), avg(`dbl`) over (PARTITION BY `key`)"
                    + " FROM %s WHERE `key` = 'null'",
                TEST_INDEX_NULL_MISSING));
    verifySchema(
        response,
        schema("min(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("max(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("avg(`int`) over (PARTITION BY `key`)", null, "double"),
        schema("min(`dbl`) over (PARTITION BY `key`)", null, "double"),
        schema("max(`dbl`) over (PARTITION BY `key`)", null, "double"),
        schema("avg(`dbl`) over (PARTITION BY `key`)", null, "double"));
    verifyDataRows(
        response, // 4 rows with null values
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null));
  }

  @Test
  public void testInMemoryAggregationOnMissingValues() throws IOException {
    // In-memory aggregation performed by the plugin
    var response =
        executeQuery(
            String.format(
                "SELECT"
                    + " min(`int`) over (PARTITION BY `key`), max(`int`) over (PARTITION BY `key`),"
                    + " avg(`int`) over (PARTITION BY `key`), min(`dbl`) over (PARTITION BY `key`),"
                    + " max(`dbl`) over (PARTITION BY `key`), avg(`dbl`) over (PARTITION BY `key`)"
                    + " FROM %s WHERE `key` = 'missing'",
                TEST_INDEX_NULL_MISSING));
    verifySchema(
        response,
        schema("min(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("max(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("avg(`int`) over (PARTITION BY `key`)", null, "double"),
        schema("min(`dbl`) over (PARTITION BY `key`)", null, "double"),
        schema("max(`dbl`) over (PARTITION BY `key`)", null, "double"),
        schema("avg(`dbl`) over (PARTITION BY `key`)", null, "double"));
    verifyDataRows(
        response, // 4 rows with null values
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null));
  }

  @Test
  public void testInMemoryAggregationOnNullValuesReturnsNull() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT "
                    + " max(int0) over (PARTITION BY `datetime1`),"
                    + " min(int0) over (PARTITION BY `datetime1`),"
                    + " avg(int0) over (PARTITION BY `datetime1`)"
                    + "from %s where int0 IS NULL;",
                TEST_INDEX_CALCS));
    verifySchema(
        response,
        schema("max(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("min(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("avg(int0) over (PARTITION BY `datetime1`)", null, "double"));
    verifySome(response.getJSONArray("datarows"), rows(null, null, null));
  }

  @Test
  public void testInMemoryAggregationOnAllValuesAndOnNotNullReturnsSameResult() throws IOException {
    var responseNotNulls =
        executeQuery(
            String.format(
                "SELECT "
                    + " max(int0) over (PARTITION BY `datetime1`),"
                    + " min(int0) over (PARTITION BY `datetime1`),"
                    + " avg(int0) over (PARTITION BY `datetime1`)"
                    + "from %s where int0 IS NOT NULL;",
                TEST_INDEX_CALCS));
    var responseAllValues =
        executeQuery(
            String.format(
                "SELECT "
                    + " max(int0) over (PARTITION BY `datetime1`),"
                    + " min(int0) over (PARTITION BY `datetime1`),"
                    + " avg(int0) over (PARTITION BY `datetime1`)"
                    + "from %s;",
                TEST_INDEX_CALCS));
    verifySchema(
        responseNotNulls,
        schema("max(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("min(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("avg(int0) over (PARTITION BY `datetime1`)", null, "double"));
    verifySchema(
        responseAllValues,
        schema("max(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("min(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("avg(int0) over (PARTITION BY `datetime1`)", null, "double"));
    assertEquals(responseNotNulls.query("/datarows/0/0"), responseAllValues.query("/datarows/0/0"));
    assertEquals(responseNotNulls.query("/datarows/0/1"), responseAllValues.query("/datarows/0/1"));
    assertEquals(responseNotNulls.query("/datarows/0/2"), responseAllValues.query("/datarows/0/2"));
  }

  @Test
  public void testPushDownAggregationOnNullNumericValuesReturnsNull() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT " + "max(int0), min(int0), avg(int0) from %s where int0 IS NULL;",
                TEST_INDEX_CALCS));
    verifySchema(
        response,
        schema("max(int0)", null, "integer"),
        schema("min(int0)", null, "integer"),
        schema("avg(int0)", null, "double"));
    verifyDataRows(response, rows(null, null, null));
  }

  @Test
  public void testPushDownAggregationOnNullDateTimeValuesFromTableReturnsNull() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT " + "max(datetime1), min(datetime1), avg(datetime1) from %s",
                TEST_INDEX_CALCS));
    verifySchema(
        response,
        schema("max(datetime1)", null, "timestamp"),
        schema("min(datetime1)", null, "timestamp"),
        schema("avg(datetime1)", null, "timestamp"));
    verifyDataRows(response, rows(null, null, null));
  }

  @Test
  public void testPushDownAggregationOnNullDateValuesReturnsNull() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT max(CAST(NULL AS date)), min(CAST(NULL AS date)), avg(CAST(NULL AS date))"
                    + " from %s",
                TEST_INDEX_CALCS));
    verifySchema(
        response,
        schema("max(CAST(NULL AS date))", null, "date"),
        schema("min(CAST(NULL AS date))", null, "date"),
        schema("avg(CAST(NULL AS date))", null, "date"));
    verifyDataRows(response, rows(null, null, null));
  }

  @Test
  public void testPushDownAggregationOnNullTimeValuesReturnsNull() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT max(CAST(NULL AS time)), min(CAST(NULL AS time)), avg(CAST(NULL AS time))"
                    + " from %s",
                TEST_INDEX_CALCS));
    verifySchema(
        response,
        schema("max(CAST(NULL AS time))", null, "time"),
        schema("min(CAST(NULL AS time))", null, "time"),
        schema("avg(CAST(NULL AS time))", null, "time"));
    verifyDataRows(response, rows(null, null, null));
  }

  @Test
  public void testPushDownAggregationOnNullTimeStampValuesReturnsNull() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT max(CAST(NULL AS timestamp)), min(CAST(NULL AS timestamp)), avg(CAST(NULL"
                    + " AS timestamp)) from %s",
                TEST_INDEX_CALCS));
    verifySchema(
        response,
        schema("max(CAST(NULL AS timestamp))", null, "timestamp"),
        schema("min(CAST(NULL AS timestamp))", null, "timestamp"),
        schema("avg(CAST(NULL AS timestamp))", null, "timestamp"));
    verifyDataRows(response, rows(null, null, null));
  }

  @Test
  public void testPushDownAggregationOnNullDateTimeValuesReturnsNull() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT " + "max(timestamp(NULL)), min(timestamp(NULL)), avg(timestamp(NULL)) from %s",
                TEST_INDEX_CALCS));
    verifySchema(
        response,
        schema("max(timestamp(NULL))", null, "timestamp"),
        schema("min(timestamp(NULL))", null, "timestamp"),
        schema("avg(timestamp(NULL))", null, "timestamp"));
    verifyDataRows(response, rows(null, null, null));
  }

  @Test
  public void testPushDownAggregationOnAllValuesAndOnNotNullReturnsSameResult() throws IOException {
    var responseNotNulls =
        executeQuery(
            String.format(
                "SELECT " + "max(int0), min(int0), avg(int0) from %s where int0 IS NOT NULL;",
                TEST_INDEX_CALCS));
    var responseAllValues =
        executeQuery(
            String.format(
                "SELECT " + "max(int0), min(int0), avg(int0) from %s;", TEST_INDEX_CALCS));
    verifySchema(
        responseNotNulls,
        schema("max(int0)", null, "integer"),
        schema("min(int0)", null, "integer"),
        schema("avg(int0)", null, "double"));
    verifySchema(
        responseAllValues,
        schema("max(int0)", null, "integer"),
        schema("min(int0)", null, "integer"),
        schema("avg(int0)", null, "double"));
    assertEquals(responseNotNulls.query("/datarows/0/0"), responseAllValues.query("/datarows/0/0"));
    assertEquals(responseNotNulls.query("/datarows/0/1"), responseAllValues.query("/datarows/0/1"));
    assertEquals(responseNotNulls.query("/datarows/0/2"), responseAllValues.query("/datarows/0/2"));
  }

  @Test
  public void testPushDownAndInMemoryAggregationReturnTheSameResult() throws IOException {
    // Playing with 'over (PARTITION BY `datetime1`)' - `datetime1` column has the same value for
    // all rows
    // so partitioning by this column has no sense and doesn't (shouldn't) affect the results
    // Aggregations with `OVER` clause are executed in memory (in SQL plugin memory),
    // Aggregations without it are performed the OpenSearch node itself (pushed down to opensearch)
    // Going to compare results of `min`, `max` and `avg` aggregation on all numeric columns in
    // `calcs`
    var columns = List.of("int0", "int1", "int2", "int3", "num0", "num1", "num2", "num3", "num4");
    var aggregations = List.of("min", "max", "avg");
    var inMemoryAggregQuery = new StringBuilder("SELECT ");
    var pushDownAggregQuery = new StringBuilder("SELECT ");
    for (var col : columns) {
      for (var aggreg : aggregations) {
        inMemoryAggregQuery.append(
            String.format(" %s(%s) over (PARTITION BY `datetime1`),", aggreg, col));
        pushDownAggregQuery.append(String.format(" %s(%s),", aggreg, col));
      }
    }
    // delete last comma
    inMemoryAggregQuery.deleteCharAt(inMemoryAggregQuery.length() - 1);
    pushDownAggregQuery.deleteCharAt(pushDownAggregQuery.length() - 1);

    var responseInMemory =
        executeQuery(inMemoryAggregQuery.append("from " + TEST_INDEX_CALCS).toString());
    var responsePushDown =
        executeQuery(pushDownAggregQuery.append("from " + TEST_INDEX_CALCS).toString());

    for (int i = 0; i < columns.size() * aggregations.size(); i++) {
      assertEquals(
          ((Number) responseInMemory.query("/datarows/0/" + i)).doubleValue(),
          ((Number) responsePushDown.query("/datarows/0/" + i)).doubleValue(),
          0.0000001); // a minor delta is affordable
    }
  }

  public void testMinIntegerPushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT min(int2)" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("min(int2)", null, "integer"));
    verifyDataRows(response, rows(-9));
  }

  @Test
  public void testMaxIntegerPushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT max(int2)" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(int2)", null, "integer"));
    verifyDataRows(response, rows(9));
  }

  @Test
  public void testAvgIntegerPushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT avg(int2)" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(int2)", null, "double"));
    verifyDataRows(response, rows(-0.8235294117647058D));
  }

  @Test
  public void testMinDoublePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT min(num3)" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("min(num3)", null, "double"));
    verifyDataRows(response, rows(-19.96D));
  }

  @Test
  public void testMaxDoublePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT max(num3)" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(num3)", null, "double"));
    verifyDataRows(response, rows(12.93D));
  }

  @Test
  public void testAvgDoublePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT avg(num3)" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(num3)", null, "double"));
    verifyDataRows(response, rows(-6.12D));
  }

  @Test
  public void testMinIntegerInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT min(int2)" + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("min(int2) OVER(PARTITION BY datetime1)", null, "integer"));
    verifySome(response.getJSONArray("datarows"), rows(-9));
  }

  @Test
  public void testMaxIntegerInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT max(int2)" + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(int2) OVER(PARTITION BY datetime1)", null, "integer"));
    verifySome(response.getJSONArray("datarows"), rows(9));
  }

  @Test
  public void testAvgIntegerInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT avg(int2)" + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(int2) OVER(PARTITION BY datetime1)", null, "double"));
    verifySome(response.getJSONArray("datarows"), rows(-0.8235294117647058D));
  }

  @Test
  public void testMinDoubleInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT min(num3)" + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("min(num3) OVER(PARTITION BY datetime1)", null, "double"));
    verifySome(response.getJSONArray("datarows"), rows(-19.96D));
  }

  @Test
  public void testMaxDoubleInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT max(num3)" + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(num3) OVER(PARTITION BY datetime1)", null, "double"));
    verifySome(response.getJSONArray("datarows"), rows(12.93D));
  }

  @Test
  public void testAvgDoubleInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT avg(num3)" + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(num3) OVER(PARTITION BY datetime1)", null, "double"));
    verifySome(response.getJSONArray("datarows"), rows(-6.12D));
  }

  @Test
  public void testMaxDatePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT max(date0)" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(date0)", null, "date"));
    verifyDataRows(response, rows("2004-06-19"));
  }

  @Test
  public void testAvgDatePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT avg(date0)" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(date0)", null, "date"));
    verifyDataRows(response, rows("1992-04-23"));
  }

  @Test
  public void testMinDateTimePushedDown() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT min(timestamp(CAST(time0 AS STRING)))" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("min(timestamp(CAST(time0 AS STRING)))", null, "timestamp"));
    verifyDataRows(response, rows("1899-12-30 21:07:32"));
  }

  @Test
  public void testMaxDateTimePushedDown() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT max(timestamp(CAST(time0 AS STRING)))" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(timestamp(CAST(time0 AS STRING)))", null, "timestamp"));
    verifyDataRows(response, rows("1900-01-01 20:36:00"));
  }

  @Test
  public void testAvgDateTimePushedDown() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT avg(timestamp(CAST(time0 AS STRING)))" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(timestamp(CAST(time0 AS STRING)))", null, "timestamp"));
    verifyDataRows(response, rows("1900-01-01 03:35:00.236"));
  }

  @Test
  public void testMinTimePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT min(time1)" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("min(time1)", null, "time"));
    verifyDataRows(response, rows("00:05:57"));
  }

  @Test
  public void testMaxTimePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT max(time1)" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(time1)", null, "time"));
    verifyDataRows(response, rows("22:50:16"));
  }

  @Test
  public void testAvgTimePushedDown() throws IOException {
    var response = executeQuery(String.format("SELECT avg(time1)" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(time1)", null, "time"));
    verifyDataRows(response, rows("13:06:36.25"));
  }

  @Test
  public void testMinTimeStampPushedDown() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT min(CAST(datetime0 AS timestamp))" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("min(CAST(datetime0 AS timestamp))", null, "timestamp"));
    verifyDataRows(response, rows("2004-07-04 22:49:28"));
  }

  @Test
  public void testMaxTimeStampPushedDown() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT max(CAST(datetime0 AS timestamp))" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(CAST(datetime0 AS timestamp))", null, "timestamp"));
    verifyDataRows(response, rows("2004-08-02 07:59:23"));
  }

  @Test
  public void testAvgTimeStampPushedDown() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT avg(CAST(datetime0 AS timestamp))" + " from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(CAST(datetime0 AS timestamp))", null, "timestamp"));
    verifyDataRows(response, rows("2004-07-20 10:38:09.705"));
  }

  @Test
  public void testMinDateInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT min(date0)" + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("min(date0) OVER(PARTITION BY datetime1)", null, "date"));
    verifySome(response.getJSONArray("datarows"), rows("1972-07-04"));
  }

  @Test
  public void testMaxDateInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT max(date0)" + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(date0) OVER(PARTITION BY datetime1)", null, "date"));
    verifySome(response.getJSONArray("datarows"), rows("2004-06-19"));
  }

  @Test
  public void testAvgDateInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT avg(date0)" + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(date0) OVER(PARTITION BY datetime1)", null, "date"));
    verifySome(response.getJSONArray("datarows"), rows("1992-04-23"));
  }

  @Test
  public void testMinDateTimeInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT min(timestamp(CAST(time0 AS STRING)))"
                    + " OVER(PARTITION BY datetime1) from %s",
                TEST_INDEX_CALCS));
    verifySchema(
        response,
        schema(
            "min(timestamp(CAST(time0 AS STRING))) OVER(PARTITION BY datetime1)", null, "timestamp"));
    verifySome(response.getJSONArray("datarows"), rows("1899-12-30 21:07:32"));
  }

  @Test
  public void testMaxDateTimeInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT max(timestamp(CAST(time0 AS STRING)))"
                    + " OVER(PARTITION BY datetime1) from %s",
                TEST_INDEX_CALCS));
    verifySchema(
        response,
        schema(
            "max(timestamp(CAST(time0 AS STRING))) OVER(PARTITION BY datetime1)", null, "timestamp"));
    verifySome(response.getJSONArray("datarows"), rows("1900-01-01 20:36:00"));
  }

  @Test
  public void testAvgDateTimeInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT avg(timestamp(CAST(time0 AS STRING)))"
                    + " OVER(PARTITION BY datetime1) from %s",
                TEST_INDEX_CALCS));
    verifySchema(
        response,
        schema(
            "avg(timestamp(CAST(time0 AS STRING))) OVER(PARTITION BY datetime1)", null, "timestamp"));
    verifySome(response.getJSONArray("datarows"), rows("1900-01-01 03:35:00.236"));
  }

  @Test
  public void testMinTimeInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT min(time1)" + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("min(time1) OVER(PARTITION BY datetime1)", null, "time"));
    verifySome(response.getJSONArray("datarows"), rows("00:05:57"));
  }

  @Test
  public void testMaxTimeInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT max(time1)" + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("max(time1) OVER(PARTITION BY datetime1)", null, "time"));
    verifySome(response.getJSONArray("datarows"), rows("22:50:16"));
  }

  @Test
  public void testAvgTimeInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT avg(time1)" + " OVER(PARTITION BY datetime1) from %s", TEST_INDEX_CALCS));
    verifySchema(response, schema("avg(time1) OVER(PARTITION BY datetime1)", null, "time"));
    verifySome(response.getJSONArray("datarows"), rows("13:06:36.25"));
  }

  @Test
  public void testMinTimeStampInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT min(CAST(datetime0 AS timestamp))"
                    + " OVER(PARTITION BY datetime1) from %s",
                TEST_INDEX_CALCS));
    verifySchema(
        response,
        schema(
            "min(CAST(datetime0 AS timestamp)) OVER(PARTITION BY datetime1)", null, "timestamp"));
    verifySome(response.getJSONArray("datarows"), rows("2004-07-04 22:49:28"));
  }

  @Test
  public void testMaxTimeStampInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT max(CAST(datetime0 AS timestamp))"
                    + " OVER(PARTITION BY datetime1) from %s",
                TEST_INDEX_CALCS));
    verifySchema(
        response,
        schema(
            "max(CAST(datetime0 AS timestamp)) OVER(PARTITION BY datetime1)", null, "timestamp"));
    verifySome(response.getJSONArray("datarows"), rows("2004-08-02 07:59:23"));
  }

  @Test
  public void testAvgTimeStampInMemory() throws IOException {
    var response =
        executeQuery(
            String.format(
                "SELECT avg(CAST(datetime0 AS timestamp))"
                    + " OVER(PARTITION BY datetime1) from %s",
                TEST_INDEX_CALCS));
    verifySchema(
        response,
        schema(
            "avg(CAST(datetime0 AS timestamp)) OVER(PARTITION BY datetime1)", null, "timestamp"));
    verifySome(response.getJSONArray("datarows"), rows("2004-07-20 10:38:09.705"));
  }

  protected JSONObject executeQuery(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }
}
