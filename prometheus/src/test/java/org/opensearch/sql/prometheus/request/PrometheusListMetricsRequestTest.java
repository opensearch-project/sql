/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.request.system.PrometheusListMetricsRequest;
import org.opensearch.sql.prometheus.request.system.model.MetricMetadata;

@ExtendWith(MockitoExtension.class)
public class PrometheusListMetricsRequestTest {

  @Mock private PrometheusClient prometheusClient;

  @Test
  @SneakyThrows
  void testSearch() {
    Map<String, List<MetricMetadata>> metricsResult = new HashMap<>();
    metricsResult.put(
        "go_gc_duration_seconds",
        Collections.singletonList(
            new MetricMetadata(
                "summary", "A summary of the pause duration of garbage collection cycles.", "")));
    metricsResult.put(
        "go_goroutines",
        Collections.singletonList(
            new MetricMetadata("gauge", "Number of goroutines that currently exist.", "")));
    when(prometheusClient.getAllMetrics()).thenReturn(metricsResult);
    PrometheusListMetricsRequest prometheusListMetricsRequest =
        new PrometheusListMetricsRequest(
            prometheusClient, new DataSourceSchemaName("prometheus", "information_schema"));
    List<ExprValue> result = prometheusListMetricsRequest.search();
    assertEquals(expectedRow(), result.get(0));
    assertEquals(2, result.size());
    verify(prometheusClient, times(1)).getAllMetrics();
  }

  @Test
  @SneakyThrows
  void testSearchWhenIOException() {
    when(prometheusClient.getAllMetrics()).thenThrow(new IOException("ERROR Message"));
    PrometheusListMetricsRequest prometheusListMetricsRequest =
        new PrometheusListMetricsRequest(
            prometheusClient, new DataSourceSchemaName("prometheus", "information_schema"));
    RuntimeException exception =
        assertThrows(RuntimeException.class, prometheusListMetricsRequest::search);
    assertEquals(
        "Error while fetching metric list for from prometheus: ERROR Message",
        exception.getMessage());
    verify(prometheusClient, times(1)).getAllMetrics();
  }

  private ExprTupleValue expectedRow() {
    LinkedHashMap<String, ExprValue> valueMap = new LinkedHashMap<>();
    valueMap.put("TABLE_CATALOG", stringValue("prometheus"));
    valueMap.put("TABLE_SCHEMA", stringValue("default"));
    valueMap.put("TABLE_NAME", stringValue("go_gc_duration_seconds"));
    valueMap.put("TABLE_TYPE", stringValue("summary"));
    valueMap.put("UNIT", stringValue(""));
    valueMap.put(
        "REMARKS", stringValue("A summary of the pause duration of garbage collection cycles."));
    return new ExprTupleValue(valueMap);
  }
}
