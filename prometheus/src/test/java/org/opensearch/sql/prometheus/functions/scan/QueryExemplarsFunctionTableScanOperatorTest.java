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
import static org.opensearch.sql.prometheus.utils.TestUtils.getJson;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import lombok.SneakyThrows;
import org.json.JSONArray;
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
import org.opensearch.sql.prometheus.request.PrometheusQueryExemplarsRequest;

@ExtendWith(MockitoExtension.class)
public class QueryExemplarsFunctionTableScanOperatorTest {
  @Mock private PrometheusClient prometheusClient;

  @Test
  @SneakyThrows
  void testQueryResponseIterator() {

    PrometheusQueryExemplarsRequest prometheusQueryExemplarsRequest =
        new PrometheusQueryExemplarsRequest();
    prometheusQueryExemplarsRequest.setQuery(QUERY);
    prometheusQueryExemplarsRequest.setStartTime(STARTTIME);
    prometheusQueryExemplarsRequest.setEndTime(ENDTIME);

    QueryExemplarsFunctionTableScanOperator queryExemplarsFunctionTableScanOperator =
        new QueryExemplarsFunctionTableScanOperator(
            prometheusClient, prometheusQueryExemplarsRequest);

    when(prometheusClient.queryExemplars(any(), any(), any()))
        .thenReturn(new JSONArray(getJson("query_exemplars_result.json")));
    queryExemplarsFunctionTableScanOperator.open();
    Assertions.assertTrue(queryExemplarsFunctionTableScanOperator.hasNext());
    LinkedHashMap<String, ExprValue> seriesLabelsHashMap = new LinkedHashMap<>();
    seriesLabelsHashMap.put("instance", new ExprStringValue("localhost:8090"));
    seriesLabelsHashMap.put("__name__", new ExprStringValue("test_exemplar_metric_total"));
    seriesLabelsHashMap.put("service", new ExprStringValue("bar"));
    seriesLabelsHashMap.put("job", new ExprStringValue("prometheus"));
    LinkedHashMap<String, ExprValue> exemplarMap = new LinkedHashMap<>();
    exemplarMap.put(
        "labels",
        new ExprTupleValue(
            new LinkedHashMap<>() {
              {
                put("traceID", new ExprStringValue("EpTxMJ40fUus7aGY"));
              }
            }));
    exemplarMap.put("timestamp", new ExprTimestampValue(Instant.ofEpochMilli(1600096945479L)));
    exemplarMap.put("value", new ExprDoubleValue(6));
    List<ExprValue> exprValueList = new ArrayList<>();
    exprValueList.add(new ExprTupleValue(exemplarMap));
    ExprCollectionValue exemplars = new ExprCollectionValue(exprValueList);
    ExprTupleValue seriesLabels = new ExprTupleValue(seriesLabelsHashMap);
    ExprTupleValue firstRow =
        new ExprTupleValue(
            new LinkedHashMap<>() {
              {
                put("seriesLabels", seriesLabels);
                put("exemplars", exemplars);
              }
            });

    assertEquals(firstRow, queryExemplarsFunctionTableScanOperator.next());
  }

  @Test
  @SneakyThrows
  void testEmptyQueryWithNoMatrixKeyInResultJson() {
    PrometheusQueryExemplarsRequest prometheusQueryExemplarsRequest =
        new PrometheusQueryExemplarsRequest();
    prometheusQueryExemplarsRequest.setQuery(QUERY);
    prometheusQueryExemplarsRequest.setStartTime(STARTTIME);
    prometheusQueryExemplarsRequest.setEndTime(ENDTIME);

    QueryExemplarsFunctionTableScanOperator queryExemplarsFunctionTableScanOperator =
        new QueryExemplarsFunctionTableScanOperator(
            prometheusClient, prometheusQueryExemplarsRequest);

    when(prometheusClient.queryExemplars(any(), any(), any()))
        .thenReturn(new JSONArray(getJson("query_exemplars_empty_result.json")));
    queryExemplarsFunctionTableScanOperator.open();
    Assertions.assertFalse(queryExemplarsFunctionTableScanOperator.hasNext());
  }

  @Test
  @SneakyThrows
  void testQuerySchema() {

    PrometheusQueryExemplarsRequest prometheusQueryExemplarsRequest =
        new PrometheusQueryExemplarsRequest();
    prometheusQueryExemplarsRequest.setQuery(QUERY);
    prometheusQueryExemplarsRequest.setStartTime(STARTTIME);
    prometheusQueryExemplarsRequest.setEndTime(ENDTIME);

    QueryExemplarsFunctionTableScanOperator queryExemplarsFunctionTableScanOperator =
        new QueryExemplarsFunctionTableScanOperator(
            prometheusClient, prometheusQueryExemplarsRequest);

    when(prometheusClient.queryExemplars(any(), any(), any()))
        .thenReturn(new JSONArray(getJson("query_exemplars_result.json")));
    queryExemplarsFunctionTableScanOperator.open();
    Assertions.assertTrue(queryExemplarsFunctionTableScanOperator.hasNext());

    ArrayList<ExecutionEngine.Schema.Column> columns = new ArrayList<>();
    columns.add(
        new ExecutionEngine.Schema.Column("seriesLabels", "seriesLabels", ExprCoreType.STRUCT));
    columns.add(new ExecutionEngine.Schema.Column("exemplars", "exemplars", ExprCoreType.ARRAY));
    ExecutionEngine.Schema expectedSchema = new ExecutionEngine.Schema(columns);
    assertEquals(expectedSchema, queryExemplarsFunctionTableScanOperator.schema());
  }

  @Test
  @SneakyThrows
  void testEmptyQueryWithException() {

    PrometheusQueryExemplarsRequest prometheusQueryExemplarsRequest =
        new PrometheusQueryExemplarsRequest();
    prometheusQueryExemplarsRequest.setQuery(QUERY);
    prometheusQueryExemplarsRequest.setStartTime(STARTTIME);
    prometheusQueryExemplarsRequest.setEndTime(ENDTIME);

    QueryExemplarsFunctionTableScanOperator queryExemplarsFunctionTableScanOperator =
        new QueryExemplarsFunctionTableScanOperator(
            prometheusClient, prometheusQueryExemplarsRequest);
    when(prometheusClient.queryExemplars(any(), any(), any()))
        .thenThrow(new IOException("Error Message"));
    RuntimeException runtimeException =
        assertThrows(RuntimeException.class, queryExemplarsFunctionTableScanOperator::open);
    assertEquals(
        "Error fetching data from prometheus server: Error Message", runtimeException.getMessage());
  }

  @Test
  @SneakyThrows
  void testExplain() {

    PrometheusQueryExemplarsRequest prometheusQueryExemplarsRequest =
        new PrometheusQueryExemplarsRequest();
    prometheusQueryExemplarsRequest.setQuery(QUERY);
    prometheusQueryExemplarsRequest.setStartTime(STARTTIME);
    prometheusQueryExemplarsRequest.setEndTime(ENDTIME);

    QueryExemplarsFunctionTableScanOperator queryExemplarsFunctionTableScanOperator =
        new QueryExemplarsFunctionTableScanOperator(
            prometheusClient, prometheusQueryExemplarsRequest);
    Assertions.assertEquals(
        "query_exemplars(test_query, 1664767694133, 1664771294133)",
        queryExemplarsFunctionTableScanOperator.explain());
  }

  @Test
  @SneakyThrows
  void testClose() {
    PrometheusQueryExemplarsRequest prometheusQueryExemplarsRequest =
        new PrometheusQueryExemplarsRequest();
    prometheusQueryExemplarsRequest.setQuery(QUERY);
    prometheusQueryExemplarsRequest.setStartTime(STARTTIME);
    prometheusQueryExemplarsRequest.setEndTime(ENDTIME);

    QueryExemplarsFunctionTableScanOperator queryExemplarsFunctionTableScanOperator =
        new QueryExemplarsFunctionTableScanOperator(
            prometheusClient, prometheusQueryExemplarsRequest);
    queryExemplarsFunctionTableScanOperator.close();
  }
}
