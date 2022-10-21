/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.prometheus.constants.TestConstants.METRIC_NAME;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.METRIC;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.prometheus.client.PrometheusClient;

@ExtendWith(MockitoExtension.class)
public class PrometheusDescribeMetricRequestTest {

  @Mock
  private PrometheusClient prometheusClient;

  @Test
  @SneakyThrows
  void testGetFieldTypes() {
    when(prometheusClient.getLabels(METRIC_NAME)).thenReturn(new ArrayList<String>() {{
        add("__name__");
        add("call");
        add("code");
      }
    });
    Map<String, ExprType> expected = new HashMap<>() {{
        put("__name__", ExprCoreType.STRING);
        put("call", ExprCoreType.STRING);
        put("code", ExprCoreType.STRING);
        put(VALUE, ExprCoreType.DOUBLE);
        put(TIMESTAMP, ExprCoreType.TIMESTAMP);
        put(METRIC, ExprCoreType.STRING);
      }};
    PrometheusDescribeMetricRequest prometheusDescribeMetricRequest
        = new PrometheusDescribeMetricRequest(prometheusClient, METRIC_NAME);
    assertEquals(expected, prometheusDescribeMetricRequest.getFieldTypes());
    verify(prometheusClient, times(1)).getLabels(METRIC_NAME);
  }


  @Test
  @SneakyThrows
  void testGetFieldTypesWithEmptyMetricName() {
    Map<String, ExprType> expected = new HashMap<>() {{
        put(VALUE, ExprCoreType.DOUBLE);
        put(TIMESTAMP, ExprCoreType.TIMESTAMP);
        put(METRIC, ExprCoreType.STRING);
      }};
    PrometheusDescribeMetricRequest prometheusDescribeMetricRequest
        = new PrometheusDescribeMetricRequest(prometheusClient, null);
    assertEquals(expected, prometheusDescribeMetricRequest.getFieldTypes());
    verifyNoInteractions(prometheusClient);
  }


  @Test
  @SneakyThrows
  void testGetFieldTypesWhenException() {
    when(prometheusClient.getLabels(METRIC_NAME)).thenThrow(new RuntimeException("ERROR Message"));
    PrometheusDescribeMetricRequest prometheusDescribeMetricRequest
        = new PrometheusDescribeMetricRequest(prometheusClient, METRIC_NAME);
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
        = new PrometheusDescribeMetricRequest(prometheusClient, METRIC_NAME);
    RuntimeException exception = assertThrows(RuntimeException.class,
        prometheusDescribeMetricRequest::getFieldTypes);
    assertEquals("Error while fetching labels for http_requests_total"
        + " from prometheus: ERROR Message", exception.getMessage());
    verify(prometheusClient, times(1)).getLabels(METRIC_NAME);
  }

}
