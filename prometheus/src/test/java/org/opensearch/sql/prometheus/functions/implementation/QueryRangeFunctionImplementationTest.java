/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.functions.implementation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.functions.implementation.QueryRangeFunctionImplementation;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.prometheus.storage.PrometheusMetricTable;


@ExtendWith(MockitoExtension.class)
class QueryRangeFunctionImplementationTest {

  @Mock
  private PrometheusClient client;


  @Test
  void testValueOfAndTypeAndToString() {
    FunctionName functionName = new FunctionName("query_range");
    List<Expression> namedArgumentExpressionList
        = List.of(DSL.namedArgument("query", DSL.literal("http_latency")),
        DSL.namedArgument("starttime", DSL.literal(12345)),
        DSL.namedArgument("endtime", DSL.literal(12345)),
        DSL.namedArgument("step", DSL.literal(14)));
    QueryRangeFunctionImplementation queryRangeFunctionImplementation
        = new QueryRangeFunctionImplementation(functionName, namedArgumentExpressionList, client);
    UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
        () -> queryRangeFunctionImplementation.valueOf());
    assertEquals("Prometheus defined function [query_range] is only "
        + "supported in SOURCE clause with prometheus connector catalog", exception.getMessage());
    assertEquals("query_range(query=\"http_latency\", starttime=12345, endtime=12345, step=14)",
        queryRangeFunctionImplementation.toString());
    assertEquals(ExprCoreType.STRUCT, queryRangeFunctionImplementation.type());
  }

  @Test
  void testApplyArguments() {
    FunctionName functionName = new FunctionName("query_range");
    List<Expression> namedArgumentExpressionList
        = List.of(DSL.namedArgument("query", DSL.literal("http_latency")),
        DSL.namedArgument("starttime", DSL.literal(12345)),
        DSL.namedArgument("endtime", DSL.literal(1234)),
        DSL.namedArgument("step", DSL.literal(14)));
    QueryRangeFunctionImplementation queryRangeFunctionImplementation
        = new QueryRangeFunctionImplementation(functionName, namedArgumentExpressionList, client);
    PrometheusMetricTable prometheusMetricTable
        = (PrometheusMetricTable) queryRangeFunctionImplementation.applyArguments();
    assertNull(prometheusMetricTable.getMetricName());
    assertNotNull(prometheusMetricTable.getPrometheusQueryRequest());
    PrometheusQueryRequest prometheusQueryRequest
        = prometheusMetricTable.getPrometheusQueryRequest();
    assertEquals("http_latency", prometheusQueryRequest.getPromQl().toString());
    assertEquals(12345, prometheusQueryRequest.getStartTime());
    assertEquals(1234, prometheusQueryRequest.getEndTime());
    assertEquals("14", prometheusQueryRequest.getStep());
  }

  @Test
  void testApplyArgumentsException() {
    FunctionName functionName = new FunctionName("query_range");
    List<Expression> namedArgumentExpressionList
        = List.of(DSL.namedArgument("query", DSL.literal("http_latency")),
        DSL.namedArgument("starttime", DSL.literal(12345)),
        DSL.namedArgument("end_time", DSL.literal(1234)),
        DSL.namedArgument("step", DSL.literal(14)));
    QueryRangeFunctionImplementation queryRangeFunctionImplementation
        = new QueryRangeFunctionImplementation(functionName, namedArgumentExpressionList, client);
    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> queryRangeFunctionImplementation.applyArguments());
    assertEquals("Invalid Function Argument:end_time", exception.getMessage());
  }


}
