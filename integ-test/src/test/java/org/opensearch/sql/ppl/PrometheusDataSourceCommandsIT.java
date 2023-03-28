/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.text.SimpleDateFormat;
import java.util.Date;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PrometheusDataSourceCommandsIT extends PPLIntegTestCase {


  @Override
  protected void init() throws Exception {
    loadIndex(Index.DATASOURCES);
  }

  @Test
  @SneakyThrows
  public void testSourceMetricCommand() {
    JSONObject response =
        executeQuery("source=my_prometheus.prometheus_http_requests_total");
    verifySchema(response,
        schema(VALUE, "double"),
        schema(TIMESTAMP,  "timestamp"),
        schema("handler",  "string"),
        schema("code",  "string"),
        schema("instance",  "string"),
        schema("job",  "string"));
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
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String query = "source=my_prometheus.prometheus_http_requests_total | where @timestamp > '"
        + format.format(new Date(System.currentTimeMillis() - 3600 * 1000))
        + "'  | sort + @timestamp | head 5";

    JSONObject response =
        executeQuery(query);
    verifySchema(response,
        schema(VALUE, "double"),
        schema(TIMESTAMP,  "timestamp"),
        schema("handler",  "string"),
        schema("code",  "string"),
        schema("instance",  "string"),
        schema("job",  "string"));
    // <TODO>Currently, data is not injected into prometheus,
    // so asserting on result is not possible. Verifying only schema.
  }

  @Test
  @SneakyThrows
  public void testMetricAvgAggregationCommand() {
    JSONObject response =
        executeQuery("source=`my_prometheus`.`prometheus_http_requests_total` | stats avg(@value) as `agg` by span(@timestamp, 15s), `handler`, `job`");
    verifySchema(response,
        schema("agg",  "double"),
        schema("span(@timestamp,15s)", "timestamp"),
        schema("`handler`", "string"),
        schema("`job`", "string"));
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
        executeQuery("source=my_prometheus.prometheus_http_requests_total | stats avg(@value) as agg by span(@timestamp, 15s), `handler`, job");
    verifySchema(response,
        schema("agg",  "double"),
        schema("span(@timestamp,15s)", "timestamp"),
        schema("`handler`", "string"),
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
        executeQuery("source=my_prometheus.prometheus_http_requests_total | stats max(@value) by span(@timestamp, 15s)");
    verifySchema(response,
        schema("max(@value)",  "double"),
        schema("span(@timestamp,15s)", "timestamp"));
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
        executeQuery("source=my_prometheus.prometheus_http_requests_total | stats min(@value) by span(@timestamp, 15s), handler");
    verifySchema(response,
        schema("min(@value)",  "double"),
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
        executeQuery("source=my_prometheus.prometheus_http_requests_total | stats count() by span(@timestamp, 15s), handler, job");
    verifySchema(response,
        schema("count()",  "integer"),
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
        executeQuery("source=my_prometheus.prometheus_http_requests_total | stats sum(@value) by span(@timestamp, 15s), handler, job");
    verifySchema(response,
        schema("sum(@value)",  "double"),
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

}
