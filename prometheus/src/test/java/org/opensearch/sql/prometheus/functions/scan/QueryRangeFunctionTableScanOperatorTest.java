/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.functions.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.prometheus.constants.TestConstants.ENDTIME;
import static org.opensearch.sql.prometheus.constants.TestConstants.QUERY;
import static org.opensearch.sql.prometheus.constants.TestConstants.STARTTIME;
import static org.opensearch.sql.prometheus.constants.TestConstants.STEP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.LABELS;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;
import static org.opensearch.sql.prometheus.utils.TestUtils.getJson;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;

@ExtendWith(MockitoExtension.class)
class QueryRangeFunctionTableScanOperatorTest {
  @Mock
  private PrometheusClient prometheusClient;

  @Test
  @SneakyThrows
  void testQueryResponseIterator() {

    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    prometheusQueryRequest.setPromQl(QUERY);
    prometheusQueryRequest.setStartTime(STARTTIME);
    prometheusQueryRequest.setEndTime(ENDTIME);
    prometheusQueryRequest.setStep(STEP);

    QueryRangeFunctionTableScanOperator queryRangeFunctionTableScanOperator
        = new QueryRangeFunctionTableScanOperator(prometheusClient, prometheusQueryRequest);

    when(prometheusClient.queryRange(any(), any(), any(), any()))
        .thenReturn(new JSONObject(getJson("query_range_result.json")));
    queryRangeFunctionTableScanOperator.open();
    Assertions.assertTrue(queryRangeFunctionTableScanOperator.hasNext());
    LinkedHashMap<String, ExprValue> labelsMap = new LinkedHashMap<>() {{
        put("instance", new ExprStringValue("localhost:9090"));
        put("__name__", new ExprStringValue("up"));
        put("job", new ExprStringValue("prometheus"));
      }};
    ExprTupleValue firstRow = new ExprTupleValue(new LinkedHashMap<>() {{
        put(LABELS, new ExprTupleValue(labelsMap));
        put(TIMESTAMP, new ExprCollectionValue(Collections
            .singletonList(new ExprTimestampValue(Instant.ofEpochMilli(1435781430781L)))));
        put(VALUE, new ExprCollectionValue(Collections.singletonList(new ExprDoubleValue(1))));
      }
    });

    assertEquals(firstRow, queryRangeFunctionTableScanOperator.next());
    Assertions.assertTrue(queryRangeFunctionTableScanOperator.hasNext());

    LinkedHashMap<String, ExprValue> labelsMap2 = new LinkedHashMap<>() {{
        put("instance", new ExprStringValue("localhost:9091"));
        put("__name__", new ExprStringValue("up"));
        put("job", new ExprStringValue("node"));
      }};
    ExprTupleValue secondRow = new ExprTupleValue(new LinkedHashMap<>() {{
        put(LABELS, new ExprTupleValue(labelsMap2));
        put(TIMESTAMP, new ExprCollectionValue(Collections
            .singletonList(new ExprTimestampValue(Instant.ofEpochMilli(1435781430781L)))));
        put(VALUE, new ExprCollectionValue(Collections.singletonList(new ExprDoubleValue(0))));
      }
    });
    assertEquals(secondRow, queryRangeFunctionTableScanOperator.next());
    Assertions.assertFalse(queryRangeFunctionTableScanOperator.hasNext());
  }

  @Test
  @SneakyThrows
  void testEmptyQueryWithNoMatrixKeyInResultJson() {
    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    prometheusQueryRequest.setPromQl(QUERY);
    prometheusQueryRequest.setStartTime(STARTTIME);
    prometheusQueryRequest.setEndTime(ENDTIME);
    prometheusQueryRequest.setStep(STEP);

    QueryRangeFunctionTableScanOperator queryRangeFunctionTableScanOperator
        = new QueryRangeFunctionTableScanOperator(prometheusClient, prometheusQueryRequest);

    when(prometheusClient.queryRange(any(), any(), any(), any()))
        .thenReturn(new JSONObject(getJson("no_matrix_query_range_result.json")));
    RuntimeException runtimeException
        = assertThrows(RuntimeException.class, queryRangeFunctionTableScanOperator::open);
    assertEquals(
        "Unexpected Result Type: vector during Prometheus Response Parsing. "
            + "'matrix' resultType is expected", runtimeException.getMessage());
  }

  @Test
  @SneakyThrows
  void testQuerySchema() {
    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    prometheusQueryRequest.setPromQl(QUERY);
    prometheusQueryRequest.setStartTime(STARTTIME);
    prometheusQueryRequest.setEndTime(ENDTIME);
    prometheusQueryRequest.setStep(STEP);

    QueryRangeFunctionTableScanOperator queryRangeFunctionTableScanOperator
        = new QueryRangeFunctionTableScanOperator(prometheusClient, prometheusQueryRequest);

    when(prometheusClient.queryRange(any(), any(), any(), any()))
        .thenReturn(new JSONObject(getJson("query_range_result.json")));
    queryRangeFunctionTableScanOperator.open();
    ArrayList<ExecutionEngine.Schema.Column> columns = new ArrayList<>();
    columns.add(new ExecutionEngine.Schema.Column(TIMESTAMP, TIMESTAMP, ExprCoreType.ARRAY));
    columns.add(new ExecutionEngine.Schema.Column(VALUE, VALUE, ExprCoreType.ARRAY));
    columns.add(new ExecutionEngine.Schema.Column(LABELS, LABELS, ExprCoreType.STRUCT));
    ExecutionEngine.Schema expectedSchema = new ExecutionEngine.Schema(columns);
    assertEquals(expectedSchema, queryRangeFunctionTableScanOperator.schema());
  }

  @Test
  @SneakyThrows
  void testEmptyQueryWithException() {
    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    prometheusQueryRequest.setPromQl(QUERY);
    prometheusQueryRequest.setStartTime(STARTTIME);
    prometheusQueryRequest.setEndTime(ENDTIME);
    prometheusQueryRequest.setStep(STEP);

    QueryRangeFunctionTableScanOperator queryRangeFunctionTableScanOperator
        = new QueryRangeFunctionTableScanOperator(prometheusClient, prometheusQueryRequest);

    when(prometheusClient.queryRange(any(), any(), any(), any()))
        .thenThrow(new IOException("Error Message"));
    RuntimeException runtimeException
        = assertThrows(RuntimeException.class, queryRangeFunctionTableScanOperator::open);
    assertEquals("Error fetching data from prometheus server: Error Message",
        runtimeException.getMessage());
  }


  @Test
  @SneakyThrows
  void testExplain() {
    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    prometheusQueryRequest.setPromQl(QUERY);
    prometheusQueryRequest.setStartTime(STARTTIME);
    prometheusQueryRequest.setEndTime(ENDTIME);
    prometheusQueryRequest.setStep(STEP);

    QueryRangeFunctionTableScanOperator queryRangeFunctionTableScanOperator
        = new QueryRangeFunctionTableScanOperator(prometheusClient, prometheusQueryRequest);

    Assertions.assertEquals("query_range(test_query, 1664767694133, 1664771294133, 14)",
        queryRangeFunctionTableScanOperator.explain());
  }

  @Test
  @SneakyThrows
  void testClose() {
    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    prometheusQueryRequest.setPromQl(QUERY);
    prometheusQueryRequest.setStartTime(STARTTIME);
    prometheusQueryRequest.setEndTime(ENDTIME);
    prometheusQueryRequest.setStep(STEP);

    QueryRangeFunctionTableScanOperator queryRangeFunctionTableScanOperator
        = new QueryRangeFunctionTableScanOperator(prometheusClient, prometheusQueryRequest);
    queryRangeFunctionTableScanOperator.close();
  }
}
