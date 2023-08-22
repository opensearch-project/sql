/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.functions.implementation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import org.opensearch.sql.prometheus.request.PrometheusQueryExemplarsRequest;
import org.opensearch.sql.prometheus.storage.QueryExemplarsTable;

@ExtendWith(MockitoExtension.class)
class QueryExemplarsFunctionImplementationTest {

  @Mock private PrometheusClient client;

  @Test
  void testValueOfAndTypeAndToString() {
    FunctionName functionName = new FunctionName("query_exemplars");
    List<Expression> namedArgumentExpressionList =
        List.of(
            DSL.namedArgument("query", DSL.literal("http_latency")),
            DSL.namedArgument("starttime", DSL.literal(12345)),
            DSL.namedArgument("endtime", DSL.literal(12345)));
    QueryExemplarFunctionImplementation queryExemplarFunctionImplementation =
        new QueryExemplarFunctionImplementation(functionName, namedArgumentExpressionList, client);
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> queryExemplarFunctionImplementation.valueOf());
    assertEquals(
        "Prometheus defined function [query_exemplars] is only "
            + "supported in SOURCE clause with prometheus connector catalog",
        exception.getMessage());
    assertEquals(
        "query_exemplars(query=\"http_latency\", starttime=12345, endtime=12345)",
        queryExemplarFunctionImplementation.toString());
    assertEquals(ExprCoreType.STRUCT, queryExemplarFunctionImplementation.type());
  }

  @Test
  void testApplyArguments() {
    FunctionName functionName = new FunctionName("query_exemplars");
    List<Expression> namedArgumentExpressionList =
        List.of(
            DSL.namedArgument("query", DSL.literal("http_latency")),
            DSL.namedArgument("starttime", DSL.literal(12345)),
            DSL.namedArgument("endtime", DSL.literal(1234)));
    QueryExemplarFunctionImplementation queryExemplarFunctionImplementation =
        new QueryExemplarFunctionImplementation(functionName, namedArgumentExpressionList, client);
    QueryExemplarsTable queryExemplarsTable =
        (QueryExemplarsTable) queryExemplarFunctionImplementation.applyArguments();
    assertNotNull(queryExemplarsTable.getExemplarsRequest());
    PrometheusQueryExemplarsRequest request = queryExemplarsTable.getExemplarsRequest();
    assertEquals("http_latency", request.getQuery());
    assertEquals(12345, request.getStartTime());
    assertEquals(1234, request.getEndTime());
  }

  @Test
  void testApplyArgumentsException() {
    FunctionName functionName = new FunctionName("query_exemplars");
    List<Expression> namedArgumentExpressionList =
        List.of(
            DSL.namedArgument("query", DSL.literal("http_latency")),
            DSL.namedArgument("starttime", DSL.literal(12345)),
            DSL.namedArgument("end_time", DSL.literal(1234)));
    QueryExemplarFunctionImplementation queryExemplarFunctionImplementation =
        new QueryExemplarFunctionImplementation(functionName, namedArgumentExpressionList, client);
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> queryExemplarFunctionImplementation.applyArguments());
    assertEquals("Invalid Function Argument:end_time", exception.getMessage());
  }
}
