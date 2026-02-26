/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.clickbench;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * Integration tests for the ClickHouse SQL dialect endpoint.
 * Tests the full pipeline: REST request with dialect=clickhouse
 * -> preprocessing -> Calcite parsing -> execution -> JSON response.
 *
 * <p>Validates Requirements 10.2, 10.3, 10.4, 10.5.
 */
public class ClickHouseDialectIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.CLICK_BENCH);
    updateClusterSettings(
        new ClusterSetting(
            "persistent",
            Settings.Key.CALCITE_ENGINE_ENABLED.getKeyValue(),
            "true"));
  }

  @Test
  public void testBasicDialectQuery() throws IOException {
    JSONObject result = executeClickHouseQuery("SELECT 1 AS val");
    assertValidJdbcResponse(result);
  }

  @Test
  public void testDialectQueryReturnsJdbcFormat() throws IOException {
    JSONObject result = executeClickHouseQuery("SELECT 42 AS answer");
    Assert.assertTrue(result.has("schema"));
    Assert.assertTrue(result.has("datarows"));
    Assert.assertTrue(result.has("total"));
    Assert.assertTrue(result.has("size"));
    Assert.assertTrue(result.has("status"));
  }

  @Test
  public void testFormatClauseStripped() throws IOException {
    JSONObject result = executeClickHouseQuery("SELECT 1 AS val FORMAT JSONEachRow");
    assertValidJdbcResponse(result);
  }

  @Test
  public void testSettingsClauseStripped() throws IOException {
    JSONObject result =
        executeClickHouseQuery("SELECT 1 AS val SETTINGS max_threads=2, max_block_size=1000");
    assertValidJdbcResponse(result);
  }

  @Test
  public void testFormatAndSettingsClausesStripped() throws IOException {
    JSONObject result =
        executeClickHouseQuery("SELECT 1 AS val FORMAT JSON SETTINGS max_threads=2");
    assertValidJdbcResponse(result);
  }

  @Test
  public void testUnregisteredDialectReturns400() throws IOException {
    try {
      executeDialectQuery("SELECT 1", "nonexistent_dialect");
      Assert.fail("Expected ResponseException for unregistered dialect");
    } catch (ResponseException e) {
      Assert.assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
      String body = getResponseBody(e.getResponse(), true);
      Assert.assertTrue(body.contains("nonexistent_dialect"));
      Assert.assertTrue(body.contains("clickhouse"));
    }
  }

  @Test
  public void testSyntaxErrorReturns400() throws IOException {
    try {
      executeClickHouseQuery("SELECT FROM WHERE");
      Assert.fail("Expected ResponseException for syntax error");
    } catch (ResponseException e) {
      Assert.assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
    }
  }

  @Test
  public void testCalciteDisabledReturns400() throws IOException {
    try {
      updateClusterSettings(
          new ClusterSetting(
              "persistent",
              Settings.Key.CALCITE_ENGINE_ENABLED.getKeyValue(),
              "false"));
      try {
        executeClickHouseQuery("SELECT 1");
        Assert.fail("Expected ResponseException when Calcite is disabled");
      } catch (ResponseException e) {
        Assert.assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
      }
    } finally {
      updateClusterSettings(
          new ClusterSetting(
              "persistent",
              Settings.Key.CALCITE_ENGINE_ENABLED.getKeyValue(),
              "true"));
    }
  }

  // ===== Time-series query tests (Requirement 10.2) =====

  @Test
  public void testTimeSeriesWithToStartOfDay() throws IOException {
    JSONObject result =
        executeClickHouseQuery(
            "SELECT toStartOfDay(EventTime) AS time_bucket, count() AS cnt "
                + "FROM hits GROUP BY time_bucket ORDER BY time_bucket ASC LIMIT 10");
    assertValidJdbcResponse(result);
    Assert.assertTrue(result.getJSONArray("datarows").length() >= 1);
  }

  @Test
  public void testTimeSeriesWithToStartOfHour() throws IOException {
    JSONObject result =
        executeClickHouseQuery(
            "SELECT toStartOfHour(EventTime) AS hour_bucket, count() AS hits "
                + "FROM hits GROUP BY hour_bucket ORDER BY hour_bucket ASC LIMIT 10");
    assertValidJdbcResponse(result);
    Assert.assertTrue(result.getJSONArray("datarows").length() >= 1);
  }

  @Test
  public void testTimeSeriesWithToStartOfMonth() throws IOException {
    JSONObject result =
        executeClickHouseQuery(
            "SELECT toStartOfMonth(EventTime) AS month_bucket, count() AS cnt "
                + "FROM hits GROUP BY month_bucket ORDER BY month_bucket ASC");
    assertValidJdbcResponse(result);
    Assert.assertTrue(result.getJSONArray("datarows").length() >= 1);
  }

  // ===== Type-conversion query tests (Requirement 10.3) =====

  @Test
  public void testToDateTimeInWhereClause() throws IOException {
    JSONObject result =
        executeClickHouseQuery(
            "SELECT CounterID, count() AS cnt FROM hits "
                + "WHERE EventTime >= toDateTime('2013-07-01 00:00:00') "
                + "AND EventTime <= toDateTime('2013-07-31 23:59:59') "
                + "GROUP BY CounterID ORDER BY cnt DESC LIMIT 10");
    assertValidJdbcResponse(result);
    Assert.assertTrue(result.getJSONArray("datarows").length() >= 1);
  }

  @Test
  public void testToDateInWhereClause() throws IOException {
    JSONObject result =
        executeClickHouseQuery(
            "SELECT count() AS cnt FROM hits "
                + "WHERE EventDate >= toDate('2013-07-01') "
                + "AND EventDate <= toDate('2013-07-31')");
    assertValidJdbcResponse(result);
    Assert.assertTrue(result.getJSONArray("datarows").length() >= 1);
  }

  @Test
  public void testToInt32InSelect() throws IOException {
    JSONObject result =
        executeClickHouseQuery("SELECT toInt32(RegionID) AS region_int FROM hits LIMIT 1");
    assertValidJdbcResponse(result);
    Assert.assertEquals(1, result.getJSONArray("datarows").length());
  }

  // ===== Aggregate query tests (Requirements 10.4, 10.5) =====

  @Test
  public void testUniqAggregate() throws IOException {
    JSONObject result =
        executeClickHouseQuery("SELECT uniq(UserID) AS unique_users FROM hits");
    assertValidJdbcResponse(result);
    long val = result.getJSONArray("datarows").getJSONArray(0).getLong(0);
    Assert.assertTrue(val >= 1);
  }

  @Test
  public void testCountNoArgs() throws IOException {
    JSONObject result = executeClickHouseQuery("SELECT count() AS total FROM hits");
    assertValidJdbcResponse(result);
    long val = result.getJSONArray("datarows").getJSONArray(0).getLong(0);
    Assert.assertTrue(val >= 1);
  }

  @Test
  public void testCombinedAggregates() throws IOException {
    JSONObject result =
        executeClickHouseQuery(
            "SELECT count() AS total_hits, uniq(UserID) AS unique_users, "
                + "uniq(CounterID) AS unique_counters FROM hits");
    assertValidJdbcResponse(result);
    JSONArray row = result.getJSONArray("datarows").getJSONArray(0);
    Assert.assertTrue(row.getLong(0) >= 1);
    Assert.assertTrue(row.getLong(1) >= 1);
    Assert.assertTrue(row.getLong(2) >= 1);
  }

  @Test
  public void testAggregateWithGroupBy() throws IOException {
    JSONObject result =
        executeClickHouseQuery(
            "SELECT RegionID, count() AS hits, uniq(UserID) AS users "
                + "FROM hits GROUP BY RegionID ORDER BY hits DESC LIMIT 5");
    assertValidJdbcResponse(result);
    Assert.assertTrue(result.getJSONArray("datarows").length() >= 1);
  }

  // ===== FORMAT/SETTINGS with real index (Requirement 9.2) =====

  @Test
  public void testFormatStrippedWithRealIndex() throws IOException {
    JSONObject result =
        executeClickHouseQuery("SELECT count() AS cnt FROM hits FORMAT JSONEachRow");
    assertValidJdbcResponse(result);
    Assert.assertTrue(result.getJSONArray("datarows").getJSONArray(0).getLong(0) >= 1);
  }

  @Test
  public void testSettingsStrippedWithRealIndex() throws IOException {
    JSONObject result =
        executeClickHouseQuery(
            "SELECT count() AS cnt FROM hits SETTINGS max_threads=2, max_block_size=1000");
    assertValidJdbcResponse(result);
    Assert.assertTrue(result.getJSONArray("datarows").getJSONArray(0).getLong(0) >= 1);
  }

  // ===== Combined Grafana-style queries =====

  @Test
  public void testGrafanaStyleTimeSeries() throws IOException {
    JSONObject result =
        executeClickHouseQuery(
            "SELECT toStartOfDay(EventTime) AS time_bucket, "
                + "count() AS hits, uniq(UserID) AS unique_users "
                + "FROM hits "
                + "WHERE EventTime >= toDateTime('2013-07-01 00:00:00') "
                + "AND EventTime <= toDateTime('2013-07-31 23:59:59') "
                + "GROUP BY time_bucket ORDER BY time_bucket ASC LIMIT 100");
    assertValidJdbcResponse(result);
    Assert.assertTrue(result.getJSONArray("datarows").length() >= 1);
    Assert.assertEquals(3, result.getJSONArray("schema").length());
  }

  @Test
  public void testGrafanaStyleWithFormatSettings() throws IOException {
    JSONObject result =
        executeClickHouseQuery(
            "SELECT toStartOfDay(EventTime) AS day, count() AS cnt "
                + "FROM hits "
                + "WHERE EventTime >= toDateTime('2013-07-01 00:00:00') "
                + "GROUP BY day ORDER BY day ASC LIMIT 10 "
                + "FORMAT JSONEachRow SETTINGS max_threads=2");
    assertValidJdbcResponse(result);
    Assert.assertTrue(result.getJSONArray("datarows").length() >= 1);
  }

  @Test
  public void testOrderByWithLimit() throws IOException {
    JSONObject result =
        executeClickHouseQuery(
            "SELECT CounterID, count() AS cnt FROM hits "
                + "GROUP BY CounterID ORDER BY cnt DESC LIMIT 3");
    assertValidJdbcResponse(result);
    JSONArray datarows = result.getJSONArray("datarows");
    Assert.assertTrue(datarows.length() >= 1);
    Assert.assertTrue(datarows.length() <= 3);
  }

  // ===== Helper methods =====

  private JSONObject executeClickHouseQuery(String sql) throws IOException {
    return executeDialectQuery(sql, "clickhouse");
  }

  private JSONObject executeDialectQuery(String sql, String dialect) throws IOException {
    String endpoint =
        String.format(Locale.ROOT, "/_plugins/_sql?dialect=%s&format=jdbc", dialect);
    Request request = new Request("POST", endpoint);
    request.setJsonEntity(
        String.format(Locale.ROOT, "{\"query\": \"%s\"}", escapeSql(sql)));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String body = getResponseBody(response, true);
    return new JSONObject(body);
  }

  private void assertValidJdbcResponse(JSONObject response) {
    Assert.assertTrue("Response must have 'schema'", response.has("schema"));
    Assert.assertTrue("Response must have 'datarows'", response.has("datarows"));
  }

  private static String escapeSql(String sql) {
    return sql.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
