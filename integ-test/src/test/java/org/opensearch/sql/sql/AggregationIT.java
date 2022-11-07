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
    JSONObject response = executeQuery(
        "SELECT COUNT(*) FILTER(WHERE age > 35) FROM " + TEST_INDEX_BANK);
    verifySchema(response, schema("COUNT(*) FILTER(WHERE age > 35)", null, "integer"));
    verifyDataRows(response, rows(3));
  }

  @Test
  public void testFilteredAggregateNotPushDown() throws IOException {
    JSONObject response = executeQuery(
        "SELECT COUNT(*) FILTER(WHERE age > 35) FROM (SELECT * FROM " + TEST_INDEX_BANK
            + ") AS a");
    verifySchema(response, schema("COUNT(*) FILTER(WHERE age > 35)", null, "integer"));
    verifyDataRows(response, rows(3));
  }

  @Test
  public void testPushDownAggregationOnNullValues() throws IOException {
    // OpenSearch aggregation query (MetricAggregation)
    var response = executeQuery(String.format(
        "SELECT min(`int`), max(`int`), avg(`int`), min(`dbl`), max(`dbl`), avg(`dbl`) " +
        "FROM %s WHERE `key` = 'null'", TEST_INDEX_NULL_MISSING));
    verifySchema(response,
        schema("min(`int`)", null, "integer"), schema("max(`int`)", null, "integer"),
        schema("avg(`int`)", null, "double"), schema("min(`dbl`)", null, "double"),
        schema("max(`dbl`)", null, "double"), schema("avg(`dbl`)", null, "double"));
    verifyDataRows(response, rows(null, null, null, null, null, null));
  }

  @Test
  public void testPushDownAggregationOnMissingValues() throws IOException {
    // OpenSearch aggregation query (MetricAggregation)
    var response = executeQuery(String.format(
        "SELECT min(`int`), max(`int`), avg(`int`), min(`dbl`), max(`dbl`), avg(`dbl`) " +
        "FROM %s WHERE `key` = 'null'", TEST_INDEX_NULL_MISSING));
    verifySchema(response,
        schema("min(`int`)", null, "integer"), schema("max(`int`)", null, "integer"),
        schema("avg(`int`)", null, "double"), schema("min(`dbl`)", null, "double"),
        schema("max(`dbl`)", null, "double"), schema("avg(`dbl`)", null, "double"));
    verifyDataRows(response, rows(null, null, null, null, null, null));
  }

  @Test
  public void testInMemoryAggregationOnNullValues() throws IOException {
    // In-memory aggregation performed by the plugin
    var response = executeQuery(String.format("SELECT"
        + " min(`int`) over (PARTITION BY `key`), max(`int`) over (PARTITION BY `key`),"
        + " avg(`int`) over (PARTITION BY `key`), min(`dbl`) over (PARTITION BY `key`),"
        + " max(`dbl`) over (PARTITION BY `key`), avg(`dbl`) over (PARTITION BY `key`)"
        + " FROM %s WHERE `key` = 'null'", TEST_INDEX_NULL_MISSING));
    verifySchema(response,
        schema("min(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("max(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("avg(`int`) over (PARTITION BY `key`)", null, "double"),
        schema("min(`dbl`) over (PARTITION BY `key`)", null, "double"),
        schema("max(`dbl`) over (PARTITION BY `key`)", null, "double"),
        schema("avg(`dbl`) over (PARTITION BY `key`)", null, "double"));
    verifyDataRows(response, // 4 rows with null values
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null));
  }

  @Test
  public void testInMemoryAggregationOnMissingValues() throws IOException {
    // In-memory aggregation performed by the plugin
    var response = executeQuery(String.format("SELECT"
        + " min(`int`) over (PARTITION BY `key`), max(`int`) over (PARTITION BY `key`),"
        + " avg(`int`) over (PARTITION BY `key`), min(`dbl`) over (PARTITION BY `key`),"
        + " max(`dbl`) over (PARTITION BY `key`), avg(`dbl`) over (PARTITION BY `key`)"
        + " FROM %s WHERE `key` = 'missing'", TEST_INDEX_NULL_MISSING));
    verifySchema(response,
        schema("min(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("max(`int`) over (PARTITION BY `key`)", null, "integer"),
        schema("avg(`int`) over (PARTITION BY `key`)", null, "double"),
        schema("min(`dbl`) over (PARTITION BY `key`)", null, "double"),
        schema("max(`dbl`) over (PARTITION BY `key`)", null, "double"),
        schema("avg(`dbl`) over (PARTITION BY `key`)", null, "double"));
    verifyDataRows(response, // 4 rows with null values
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null),
        rows(null, null, null, null, null, null));
  }

  @Test
  public void testInMemoryAggregationOnNullValuesReturnsNull() throws IOException {
    var response = executeQuery(String.format("SELECT "
        + " max(int0) over (PARTITION BY `datetime1`),"
        + " min(int0) over (PARTITION BY `datetime1`),"
        + " avg(int0) over (PARTITION BY `datetime1`)"
        + "from %s where int0 IS NULL;", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("max(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("min(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("avg(int0) over (PARTITION BY `datetime1`)", null, "double"));
    verifySome(response.getJSONArray("datarows"), rows(null, null, null));
  }

  @Test
  public void testInMemoryAggregationOnAllValuesAndOnNotNullReturnsSameResult() throws IOException {
    var responseNotNulls = executeQuery(String.format("SELECT "
        + " max(int0) over (PARTITION BY `datetime1`),"
        + " min(int0) over (PARTITION BY `datetime1`),"
        + " avg(int0) over (PARTITION BY `datetime1`)"
        + "from %s where int0 IS NOT NULL;", TEST_INDEX_CALCS));
    var responseAllValues = executeQuery(String.format("SELECT "
        + " max(int0) over (PARTITION BY `datetime1`),"
        + " min(int0) over (PARTITION BY `datetime1`),"
        + " avg(int0) over (PARTITION BY `datetime1`)"
        + "from %s;", TEST_INDEX_CALCS));
    verifySchema(responseNotNulls,
        schema("max(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("min(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("avg(int0) over (PARTITION BY `datetime1`)", null, "double"));
    verifySchema(responseAllValues,
        schema("max(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("min(int0) over (PARTITION BY `datetime1`)", null, "integer"),
        schema("avg(int0) over (PARTITION BY `datetime1`)", null, "double"));
    assertEquals(responseNotNulls.query("/datarows/0/0"), responseAllValues.query("/datarows/0/0"));
    assertEquals(responseNotNulls.query("/datarows/0/1"), responseAllValues.query("/datarows/0/1"));
    assertEquals(responseNotNulls.query("/datarows/0/2"), responseAllValues.query("/datarows/0/2"));
  }

  @Test
  public void testPushDownAggregationOnNullValuesReturnsNull() throws IOException {
    var response = executeQuery(String.format("SELECT "
        + "max(int0), min(int0), avg(int0) from %s where int0 IS NULL;", TEST_INDEX_CALCS));
    verifySchema(response,
        schema("max(int0)", null, "integer"),
        schema("min(int0)", null, "integer"),
        schema("avg(int0)", null, "double"));
    verifyDataRows(response, rows(null, null, null));
  }

  @Test
  public void testPushDownAggregationOnAllValuesAndOnNotNullReturnsSameResult() throws IOException {
    var responseNotNulls = executeQuery(String.format("SELECT "
        + "max(int0), min(int0), avg(int0) from %s where int0 IS NOT NULL;", TEST_INDEX_CALCS));
    var responseAllValues = executeQuery(String.format("SELECT "
        + "max(int0), min(int0), avg(int0) from %s;", TEST_INDEX_CALCS));
    verifySchema(responseNotNulls,
        schema("max(int0)", null, "integer"),
        schema("min(int0)", null, "integer"),
        schema("avg(int0)", null, "double"));
    verifySchema(responseAllValues,
        schema("max(int0)", null, "integer"),
        schema("min(int0)", null, "integer"),
        schema("avg(int0)", null, "double"));
    assertEquals(responseNotNulls.query("/datarows/0/0"), responseAllValues.query("/datarows/0/0"));
    assertEquals(responseNotNulls.query("/datarows/0/1"), responseAllValues.query("/datarows/0/1"));
    assertEquals(responseNotNulls.query("/datarows/0/2"), responseAllValues.query("/datarows/0/2"));
  }

  @Test
  public void testPushDownAndInMemoryAggregationReturnTheSameResult() throws IOException {
    // Playing with 'over (PARTITION BY `datetime1`)' - `datetime1` column has the same value for all rows
    // so partitioning by this column has no sense and doesn't (shouldn't) affect the results
    // Aggregations with `OVER` clause are executed in memory (in SQL plugin memory),
    // Aggregations without it are performed the OpenSearch node itself (pushed down to opensearch)
    // Going to compare results of `min`, `max` and `avg` aggregation on all numeric columns in `calcs`
    var columns = List.of("int0", "int1", "int2", "int3", "num0", "num1", "num2", "num3", "num4");
    var aggregations = List.of("min", "max", "avg");
    var inMemoryAggregQuery = new StringBuilder("SELECT ");
    var pushDownAggregQuery = new StringBuilder("SELECT ");
    for (var col : columns) {
      for (var aggreg : aggregations) {
        inMemoryAggregQuery.append(String.format(" %s(%s) over (PARTITION BY `datetime1`),", aggreg, col));
        pushDownAggregQuery.append(String.format(" %s(%s),", aggreg, col));
      }
    }
    // delete last comma
    inMemoryAggregQuery.deleteCharAt(inMemoryAggregQuery.length() - 1);
    pushDownAggregQuery.deleteCharAt(pushDownAggregQuery.length() - 1);

    var responseInMemory = executeQuery(
        inMemoryAggregQuery.append("from " + TEST_INDEX_CALCS).toString());
    var responsePushDown = executeQuery(
        pushDownAggregQuery.append("from " + TEST_INDEX_CALCS).toString());

    for (int i = 0; i < columns.size() * aggregations.size(); i++) {
      assertEquals(
          ((Number)responseInMemory.query("/datarows/0/" + i)).doubleValue(),
          ((Number)responsePushDown.query("/datarows/0/" + i)).doubleValue(),
          0.0000001); // a minor delta is affordable
    }
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
