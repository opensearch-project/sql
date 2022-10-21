/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.prometheus.constants.TestConstants.ENDTIME;
import static org.opensearch.sql.prometheus.constants.TestConstants.QUERY;
import static org.opensearch.sql.prometheus.constants.TestConstants.STARTTIME;
import static org.opensearch.sql.prometheus.constants.TestConstants.STEP;
import static org.opensearch.sql.prometheus.utils.TestUtils.getJson;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.prometheus.client.PrometheusClient;

@ExtendWith(MockitoExtension.class)
public class PrometheusMetricScanTest {

  @Mock
  private PrometheusClient prometheusClient;

  @Test
  @SneakyThrows
  void testQueryResponseIterator() {
    PrometheusMetricScan prometheusMetricScan = new PrometheusMetricScan(prometheusClient);
    prometheusMetricScan.getRequest().getPromQl().append(QUERY);
    prometheusMetricScan.getRequest().setStartTime(STARTTIME);
    prometheusMetricScan.getRequest().setEndTime(ENDTIME);
    prometheusMetricScan.getRequest().setStep(STEP);

    when(prometheusClient.queryRange(any(), any(), any(), any()))
        .thenReturn(new JSONObject(getJson("query_range_result.json")));
    prometheusMetricScan.open();
    Assertions.assertTrue(prometheusMetricScan.hasNext());
    ExprTupleValue firstRow = new ExprTupleValue(new LinkedHashMap<>() {{
        put("@timestamp", new ExprTimestampValue(Instant.ofEpochMilli(1435781430781L)));
        put("@value", new ExprDoubleValue(1));
        put("metric", new ExprStringValue(
            "{\"instance\":\"localhost:9090\",\"__name__\":\"up\",\"job\":\"prometheus\"}"));
      }
    });
    assertEquals(firstRow, prometheusMetricScan.next());
    Assertions.assertTrue(prometheusMetricScan.hasNext());
    ExprTupleValue secondRow = new ExprTupleValue(new LinkedHashMap<>() {{
        put("@timestamp", new ExprTimestampValue(Instant.ofEpochMilli(1435781430781L)));
        put("@value", new ExprDoubleValue(0));
        put("metric", new ExprStringValue(
            "{\"instance\":\"localhost:9091\",\"__name__\":\"up\",\"job\":\"node\"}"));
      }
    });
    assertEquals(secondRow, prometheusMetricScan.next());
    Assertions.assertFalse(prometheusMetricScan.hasNext());
  }

  @Test
  @SneakyThrows
  void testEmptyQueryResponseIterator() {
    PrometheusMetricScan prometheusMetricScan = new PrometheusMetricScan(prometheusClient);
    prometheusMetricScan.getRequest().getPromQl().append(QUERY);
    prometheusMetricScan.getRequest().setStartTime(STARTTIME);
    prometheusMetricScan.getRequest().setEndTime(ENDTIME);
    prometheusMetricScan.getRequest().setStep(STEP);

    when(prometheusClient.queryRange(any(), any(), any(), any()))
        .thenReturn(new JSONObject(getJson("empty_query_range_result.json")));
    prometheusMetricScan.open();
    Assertions.assertFalse(prometheusMetricScan.hasNext());
  }

  @Test
  @SneakyThrows
  void testEmptyQueryWithNoMatrixKeyInResultJson() {
    PrometheusMetricScan prometheusMetricScan = new PrometheusMetricScan(prometheusClient);
    prometheusMetricScan.getRequest().getPromQl().append(QUERY);
    prometheusMetricScan.getRequest().setStartTime(STARTTIME);
    prometheusMetricScan.getRequest().setEndTime(ENDTIME);
    prometheusMetricScan.getRequest().setStep(STEP);

    when(prometheusClient.queryRange(any(), any(), any(), any()))
        .thenReturn(new JSONObject(getJson("no_matrix_query_range_result.json")));
    RuntimeException runtimeException
        = Assertions.assertThrows(RuntimeException.class, prometheusMetricScan::open);
    assertEquals(
        "Unexpected Result Type: vector during Prometheus Response Parsing. "
            + "'matrix' resultType is expected", runtimeException.getMessage());
  }

  @Test
  @SneakyThrows
  void testEmptyQueryWithException() {
    PrometheusMetricScan prometheusMetricScan = new PrometheusMetricScan(prometheusClient);
    prometheusMetricScan.getRequest().getPromQl().append(QUERY);
    prometheusMetricScan.getRequest().setStartTime(STARTTIME);
    prometheusMetricScan.getRequest().setEndTime(ENDTIME);
    prometheusMetricScan.getRequest().setStep(STEP);

    when(prometheusClient.queryRange(any(), any(), any(), any()))
        .thenThrow(new IOException("Error Message"));
    RuntimeException runtimeException
        = assertThrows(RuntimeException.class, prometheusMetricScan::open);
    assertEquals("Error fetching data from prometheus server. Error Message",
        runtimeException.getMessage());
  }

  @Test
  @SneakyThrows
  void testExplain() {
    PrometheusMetricScan prometheusMetricScan = new PrometheusMetricScan(prometheusClient);
    prometheusMetricScan.getRequest().getPromQl().append(QUERY);
    prometheusMetricScan.getRequest().setStartTime(STARTTIME);
    prometheusMetricScan.getRequest().setEndTime(ENDTIME);
    prometheusMetricScan.getRequest().setStep(STEP);
    assertEquals(
        "PrometheusQueryRequest(promQl=test_query, startTime=1664767694133, "
            + "endTime=1664771294133, step=14)",
        prometheusMetricScan.explain());
  }

}
