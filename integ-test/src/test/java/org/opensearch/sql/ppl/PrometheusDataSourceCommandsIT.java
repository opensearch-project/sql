/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.ppl;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.LABELS;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEquals;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceStatus;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.util.TestUtils;

public class PrometheusDataSourceCommandsIT extends PPLIntegTestCase {

  /**
   * Integ tests are dependent on self generated metrics in prometheus instance. When running
   * individual integ tests there is no time for generation of metrics in the test prometheus
   * instance. This method gives prometheus time to generate metrics on itself.
   *
   * @throws InterruptedException
   */
  @BeforeClass
  protected static void metricGenerationWait() throws InterruptedException {
    Thread.sleep(10000);
  }

  @Override
  public void init() throws Exception {
    super.init();
    DataSourceMetadata createDSM =
        new DataSourceMetadata.Builder()
            .setName("my_prometheus")
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(ImmutableMap.of("prometheus.uri", "http://localhost:9090"))
            .build();
    Request createRequest = getCreateDataSourceRequest(createDSM);
    Response response = client().performRequest(createRequest);
    Assert.assertEquals(201, response.getStatusLine().getStatusCode());
  }

  @After
  protected void deleteDataSourceMetadata() throws IOException {
    Request deleteRequest = getDeleteDataSourceRequest("my_prometheus");
    Response deleteResponse = client().performRequest(deleteRequest);
    Assert.assertEquals(204, deleteResponse.getStatusLine().getStatusCode());
  }

  @Test
  @SneakyThrows
  public void testSourceMetricCommand() {
    JSONObject response = executeQuery("source=my_prometheus.prometheus_http_requests_total");
    verifySchema(
        response,
        schema(VALUE, "double"),
        schema(TIMESTAMP, "timestamp"),
        schema("handler", "string"),
        schema("code", "string"),
        schema("instance", "string"),
        schema("job", "string"));
    Assertions.assertTrue(response.getInt("size") > 0);
    Assertions.assertEquals(6, response.getJSONArray("datarows").getJSONArray(0).length());
    JSONArray firstRow = response.getJSONArray("datarows").getJSONArray(0);
    for (int i = 0; i < firstRow.length(); i++) {
      Assertions.assertNotNull(firstRow.get(i));
      Assertions.assertTrue(StringUtils.isNotEmpty(firstRow.get(i).toString()));
    }
  }

  @Test
  @SneakyThrows
  public void testSourceMetricCommandWithTimestamp() {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    // Generate timestamp string for one hour less than the current time
    String timestamp = LocalDateTime.now().minusHours(1).format(formatter);
    String query =
        "source=my_prometheus.prometheus_http_requests_total | where @timestamp > '"
            + timestamp
            + "'  | sort + @timestamp | head 5";

    JSONObject response = executeQuery(query);
    verifySchema(
        response,
        schema(VALUE, "double"),
        schema(TIMESTAMP, "timestamp"),
        schema("handler", "string"),
        schema("code", "string"),
        schema("instance", "string"),
        schema("job", "string"));
    // <TODO>Currently, data is not injected into prometheus,
    // so asserting on result is not possible. Verifying only schema.
  }

  @Test
  @SneakyThrows
  public void testMetricAvgAggregationCommand() {
    JSONObject response =
        executeQuery(
            "source=`my_prometheus`.`prometheus_http_requests_total` | stats avg(@value) as `agg`"
                + " by span(@timestamp, 15s), `handler`, `job`");
    verifySchema(
        response,
        schema("agg", "double"),
        schema("span(@timestamp,15s)", "timestamp"),
        schema("handler", "string"),
        schema("job", "string"));
    Assertions.assertTrue(response.getInt("size") > 0);
    Assertions.assertEquals(4, response.getJSONArray("datarows").getJSONArray(0).length());
    JSONArray firstRow = response.getJSONArray("datarows").getJSONArray(0);
    for (int i = 0; i < firstRow.length(); i++) {
      Assertions.assertNotNull(firstRow.get(i));
      Assertions.assertTrue(StringUtils.isNotEmpty(firstRow.get(i).toString()));
    }
  }

  @Test
  @SneakyThrows
  public void testMetricAvgAggregationCommandWithAlias() {
    JSONObject response =
        executeQuery(
            "source=my_prometheus.prometheus_http_requests_total | stats avg(@value) as agg by"
                + " span(@timestamp, 15s), `handler`, job");
    verifySchema(
        response,
        schema("agg", "double"),
        schema("span(@timestamp,15s)", "timestamp"),
        schema("handler", "string"),
        schema("job", "string"));
    Assertions.assertTrue(response.getInt("size") > 0);
    Assertions.assertEquals(4, response.getJSONArray("datarows").getJSONArray(0).length());
    JSONArray firstRow = response.getJSONArray("datarows").getJSONArray(0);
    for (int i = 0; i < firstRow.length(); i++) {
      Assertions.assertNotNull(firstRow.get(i));
      Assertions.assertTrue(StringUtils.isNotEmpty(firstRow.get(i).toString()));
    }
  }

  @Test
  @SneakyThrows
  public void testMetricMaxAggregationCommand() {
    JSONObject response =
        executeQuery(
            "source=my_prometheus.prometheus_http_requests_total | stats max(@value) by"
                + " span(@timestamp, 15s)");
    verifySchema(
        response, schema("max(@value)", "double"), schema("span(@timestamp,15s)", "timestamp"));
    Assertions.assertTrue(response.getInt("size") > 0);
    Assertions.assertEquals(2, response.getJSONArray("datarows").getJSONArray(0).length());
    JSONArray firstRow = response.getJSONArray("datarows").getJSONArray(0);
    for (int i = 0; i < firstRow.length(); i++) {
      Assertions.assertNotNull(firstRow.get(i));
      Assertions.assertTrue(StringUtils.isNotEmpty(firstRow.get(i).toString()));
    }
  }

  @Test
  @SneakyThrows
  public void testMetricMinAggregationCommand() {
    JSONObject response =
        executeQuery(
            "source=my_prometheus.prometheus_http_requests_total | stats min(@value) by"
                + " span(@timestamp, 15s), handler");
    verifySchema(
        response,
        schema("min(@value)", "double"),
        schema("span(@timestamp,15s)", "timestamp"),
        schema("handler", "string"));
    Assertions.assertTrue(response.getInt("size") > 0);
    Assertions.assertEquals(3, response.getJSONArray("datarows").getJSONArray(0).length());
    JSONArray firstRow = response.getJSONArray("datarows").getJSONArray(0);
    for (int i = 0; i < firstRow.length(); i++) {
      Assertions.assertNotNull(firstRow.get(i));
      Assertions.assertTrue(StringUtils.isNotEmpty(firstRow.get(i).toString()));
    }
  }

  @Test
  @SneakyThrows
  public void testMetricCountAggregationCommand() {
    JSONObject response =
        executeQuery(
            "source=my_prometheus.prometheus_http_requests_total | stats count() by"
                + " span(@timestamp, 15s), handler, job");
    verifySchema(
        response,
        schema("count()", "int"),
        schema("span(@timestamp,15s)", "timestamp"),
        schema("handler", "string"),
        schema("job", "string"));
    Assertions.assertTrue(response.getInt("size") > 0);
    Assertions.assertEquals(4, response.getJSONArray("datarows").getJSONArray(0).length());
    JSONArray firstRow = response.getJSONArray("datarows").getJSONArray(0);
    for (int i = 0; i < firstRow.length(); i++) {
      Assertions.assertNotNull(firstRow.get(i));
      Assertions.assertTrue(StringUtils.isNotEmpty(firstRow.get(i).toString()));
    }
  }

  @Test
  @SneakyThrows
  public void testMetricSumAggregationCommand() {
    JSONObject response =
        executeQuery(
            "source=my_prometheus.prometheus_http_requests_total | stats sum(@value) by"
                + " span(@timestamp, 15s), handler, job");
    verifySchema(
        response,
        schema("sum(@value)", "double"),
        schema("span(@timestamp,15s)", "timestamp"),
        schema("handler", "string"),
        schema("job", "string"));
    Assertions.assertTrue(response.getInt("size") > 0);
    Assertions.assertEquals(4, response.getJSONArray("datarows").getJSONArray(0).length());
    JSONArray firstRow = response.getJSONArray("datarows").getJSONArray(0);
    for (int i = 0; i < firstRow.length(); i++) {
      Assertions.assertNotNull(firstRow.get(i));
      Assertions.assertTrue(StringUtils.isNotEmpty(firstRow.get(i).toString()));
    }
  }

  @Test
  @SneakyThrows
  public void testQueryRange() {
    long currentTimestamp = new Date().getTime();
    JSONObject response =
        executeQuery(
            "source=my_prometheus.query_range('prometheus_http_requests_total',"
                + ((currentTimestamp / 1000) - 3600)
                + ","
                + currentTimestamp / 1000
                + ", "
                + "'14'"
                + ")");
    verifySchema(
        response, schema(LABELS, "struct"), schema(VALUE, "array"), schema(TIMESTAMP, "array"));
    Assertions.assertTrue(response.getInt("size") > 0);
  }

  @Test
  public void explainQueryRange() throws Exception {
    String expected = loadFromFile("expectedOutput/ppl/explain_query_range.json");
    assertJsonEquals(
        expected,
        explainQueryToString(
            "source = my_prometheus"
                + ".query_range('prometheus_http_requests_total',1689281439,1689291439,14)"));
  }

  @Test
  public void testExplainForQueryExemplars() throws Exception {
    String expected = loadFromFile("expectedOutput/ppl/explain_query_exemplars.json");
    assertJsonEquals(
        expected,
        explainQueryToString(
            "source = my_prometheus."
                + "query_exemplars('app_ads_ad_requests_total',1689228292,1689232299)"));
  }

  @Test
  public void testQueryOnDisabledDataSource() throws IOException {
    DataSourceMetadata deletedDSM =
        new DataSourceMetadata.Builder()
            .setName("disabled_prometheus")
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(ImmutableMap.of("prometheus.uri", "http://localhost:9090"))
            .setDataSourceStatus(DataSourceStatus.DISABLED)
            .build();
    Request createRequest = getCreateDataSourceRequest(deletedDSM);
    Response response = client().performRequest(createRequest);
    Assert.assertEquals(201, response.getStatusLine().getStatusCode());

    try {
      executeQuery(
          "source=disabled_prometheus.prometheus_http_requests_total | stats sum(@value) by"
              + " span(@timestamp, 15s), handler, job");
    } catch (ResponseException ex) {
      response = ex.getResponse();
    }
    JSONObject result = new JSONObject(TestUtils.getResponseBody(response));
    assertThat(result.getInt("status"), equalTo(400));
    JSONObject error = result.getJSONObject("error");
    assertThat(error.getString("reason"), equalTo("Invalid Query"));
    assertThat(error.getString("details"), equalTo("Datasource disabled_prometheus is disabled."));
    assertThat(error.getString("type"), equalTo("DatasourceDisabledException"));

    Request deleteRequest = getDeleteDataSourceRequest("disabled_prometheus");
    Response deleteResponse = client().performRequest(deleteRequest);
    Assert.assertEquals(204, deleteResponse.getStatusLine().getStatusCode());
  }

  String loadFromFile(String filename) throws Exception {
    URI uri = Resources.getResource(filename).toURI();
    return new String(Files.readAllBytes(Paths.get(uri)));
  }
}
