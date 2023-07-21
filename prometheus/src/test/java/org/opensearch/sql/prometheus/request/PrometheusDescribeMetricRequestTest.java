/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.prometheus.constants.TestConstants.METRIC_NAME;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;

import java.io.IOException;
import java.util.ArrayList;
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
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.request.system.PrometheusDescribeMetricRequest;

@ExtendWith(MockitoExtension.class)
public class PrometheusDescribeMetricRequestTest {

  @Mock
  private PrometheusClient prometheusClient;

  @Test
  @SneakyThrows
  void testGetFieldTypes() {
    when(prometheusClient.getLabels(METRIC_NAME)).thenReturn(new ArrayList<String>() {{
        add("call");
        add("code");
      }
    });
    Map<String, ExprType> expected = new HashMap<>() {{
        put("call", ExprCoreType.STRING);
        put("code", ExprCoreType.STRING);
        put(VALUE, ExprCoreType.DOUBLE);
        put(TIMESTAMP, ExprCoreType.TIMESTAMP);
      }};
    PrometheusDescribeMetricRequest prometheusDescribeMetricRequest
        = new PrometheusDescribeMetricRequest(prometheusClient,
                new DataSourceSchemaName("prometheus", "default"), METRIC_NAME);
    assertEquals(expected, prometheusDescribeMetricRequest.getFieldTypes());
    verify(prometheusClient, times(1)).getLabels(METRIC_NAME);
  }


  @Test
  @SneakyThrows
  void testGetFieldTypesWithEmptyMetricName() {
    Map<String, ExprType> expected = new HashMap<>() {{
        put(VALUE, ExprCoreType.DOUBLE);
        put(TIMESTAMP, ExprCoreType.TIMESTAMP);
      }};
    assertThrows(NullPointerException.class,
        () -> new PrometheusDescribeMetricRequest(prometheusClient,
            new DataSourceSchemaName("prometheus", "default"),
             null));
  }


  @Test
  @SneakyThrows
  void testGetFieldTypesWhenException() {
    when(prometheusClient.getLabels(METRIC_NAME)).thenThrow(new RuntimeException("ERROR Message"));
    PrometheusDescribeMetricRequest prometheusDescribeMetricRequest
        = new PrometheusDescribeMetricRequest(prometheusClient,
            new DataSourceSchemaName("prometheus", "default"), METRIC_NAME);
    RuntimeException exception = assertThrows(RuntimeException.class,
        prometheusDescribeMetricRequest::getFieldTypes);
    verify(prometheusClient, times(1)).getLabels(METRIC_NAME);
    assertEquals("ERROR Message", exception.getMessage());
  }

  @Test
  @SneakyThrows
  void testGetFieldTypesWhenIOException() {
    when(prometheusClient.getLabels(METRIC_NAME)).thenThrow(new IOException("ERROR Message"));
    PrometheusDescribeMetricRequest prometheusDescribeMetricRequest
        = new PrometheusDescribeMetricRequest(prometheusClient,
            new DataSourceSchemaName("prometheus", "default"), METRIC_NAME);
    RuntimeException exception = assertThrows(RuntimeException.class,
        prometheusDescribeMetricRequest::getFieldTypes);
    assertEquals("Error while fetching labels for http_requests_total"
        + " from prometheus: ERROR Message", exception.getMessage());
    verify(prometheusClient, times(1)).getLabels(METRIC_NAME);
  }

  @Test
  @SneakyThrows
  void testSearch() {
    when(prometheusClient.getLabels(METRIC_NAME)).thenReturn(new ArrayList<>() {
      {
        add("call");
      }
    });
    PrometheusDescribeMetricRequest prometheusDescribeMetricRequest
        = new PrometheusDescribeMetricRequest(prometheusClient,
            new DataSourceSchemaName("test", "default"), METRIC_NAME);
    List<ExprValue> result = prometheusDescribeMetricRequest.search();
    assertEquals(3, result.size());
    assertEquals(expectedRow(), result.get(0));
    verify(prometheusClient, times(1)).getLabels(METRIC_NAME);
  }

  private ExprValue expectedRow() {
    LinkedHashMap<String, ExprValue> valueMap = new LinkedHashMap<>();
    valueMap.put("TABLE_CATALOG", stringValue("test"));
    valueMap.put("TABLE_SCHEMA", stringValue("default"));
    valueMap.put("TABLE_NAME", stringValue(METRIC_NAME));
    valueMap.put("COLUMN_NAME", stringValue("call"));
    valueMap.put("DATA_TYPE", stringValue(ExprCoreType.STRING.legacyTypeName().toLowerCase()));
    return new ExprTupleValue(valueMap);
  }

}
