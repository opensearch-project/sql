/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NULL_MISSING;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import java.io.IOException;
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
  }

  @Test
  public void testFilteredAggregatePushedDown() throws IOException {
    JSONObject response = executeQuery(
        "SELECT COUNT(*) FILTER(WHERE age > 35) FROM " + TEST_INDEX_BANK);
    verifySchema(response, schema("COUNT(*) FILTER(WHERE age > 35)", null, "integer"));
    verifyDataRows(response, rows(3));
  }

  @Test
  public void testFilteredAggregateNotPushedDown() throws IOException {
    JSONObject response = executeQuery(
        "SELECT COUNT(*) FILTER(WHERE age > 35) FROM (SELECT * FROM " + TEST_INDEX_BANK
            + ") AS a");
    verifySchema(response, schema("COUNT(*) FILTER(WHERE age > 35)", null, "integer"));
    verifyDataRows(response, rows(3));
  }

  @Test
  public void testPushedDownAggregationOnNullValues() throws IOException {
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
  public void testPushedDownAggregationOnMissingValues() throws IOException {
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
