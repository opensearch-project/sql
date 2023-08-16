/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.functions.resolver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.expression.function.TableFunctionImplementation;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.functions.implementation.QueryRangeFunctionImplementation;
import org.opensearch.sql.prometheus.functions.resolver.QueryRangeTableFunctionResolver;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.prometheus.storage.PrometheusMetricTable;

@ExtendWith(MockitoExtension.class)
class QueryRangeTableFunctionResolverTest {

  @Mock private PrometheusClient client;

  @Mock private FunctionProperties functionProperties;

  @Test
  void testResolve() {
    QueryRangeTableFunctionResolver queryRangeTableFunctionResolver =
        new QueryRangeTableFunctionResolver(client);
    FunctionName functionName = FunctionName.of("query_range");
    List<Expression> expressions =
        List.of(
            DSL.namedArgument("query", DSL.literal("http_latency")),
            DSL.namedArgument("starttime", DSL.literal(12345)),
            DSL.namedArgument("endtime", DSL.literal(12345)),
            DSL.namedArgument("step", DSL.literal(14)));
    FunctionSignature functionSignature =
        new FunctionSignature(
            functionName, expressions.stream().map(Expression::type).collect(Collectors.toList()));
    Pair<FunctionSignature, FunctionBuilder> resolution =
        queryRangeTableFunctionResolver.resolve(functionSignature);
    assertEquals(functionName, resolution.getKey().getFunctionName());
    assertEquals(functionName, queryRangeTableFunctionResolver.getFunctionName());
    assertEquals(List.of(STRING, LONG, LONG, STRING), resolution.getKey().getParamTypeList());
    FunctionBuilder functionBuilder = resolution.getValue();
    TableFunctionImplementation functionImplementation =
        (TableFunctionImplementation) functionBuilder.apply(functionProperties, expressions);
    assertTrue(functionImplementation instanceof QueryRangeFunctionImplementation);
    PrometheusMetricTable prometheusMetricTable =
        (PrometheusMetricTable) functionImplementation.applyArguments();
    assertNotNull(prometheusMetricTable.getPrometheusQueryRequest());
    PrometheusQueryRequest prometheusQueryRequest =
        prometheusMetricTable.getPrometheusQueryRequest();
    assertEquals("http_latency", prometheusQueryRequest.getPromQl());
    assertEquals(12345L, prometheusQueryRequest.getStartTime());
    assertEquals(12345L, prometheusQueryRequest.getEndTime());
    assertEquals("14", prometheusQueryRequest.getStep());
  }

  @Test
  void testArgumentsPassedByPosition() {
    QueryRangeTableFunctionResolver queryRangeTableFunctionResolver =
        new QueryRangeTableFunctionResolver(client);
    FunctionName functionName = FunctionName.of("query_range");
    List<Expression> expressions =
        List.of(
            DSL.namedArgument(null, DSL.literal("http_latency")),
            DSL.namedArgument(null, DSL.literal(12345)),
            DSL.namedArgument(null, DSL.literal(12345)),
            DSL.namedArgument(null, DSL.literal(14)));
    FunctionSignature functionSignature =
        new FunctionSignature(
            functionName, expressions.stream().map(Expression::type).collect(Collectors.toList()));

    Pair<FunctionSignature, FunctionBuilder> resolution =
        queryRangeTableFunctionResolver.resolve(functionSignature);

    assertEquals(functionName, resolution.getKey().getFunctionName());
    assertEquals(functionName, queryRangeTableFunctionResolver.getFunctionName());
    assertEquals(List.of(STRING, LONG, LONG, STRING), resolution.getKey().getParamTypeList());
    FunctionBuilder functionBuilder = resolution.getValue();
    TableFunctionImplementation functionImplementation =
        (TableFunctionImplementation) functionBuilder.apply(functionProperties, expressions);
    assertTrue(functionImplementation instanceof QueryRangeFunctionImplementation);
    PrometheusMetricTable prometheusMetricTable =
        (PrometheusMetricTable) functionImplementation.applyArguments();
    assertNotNull(prometheusMetricTable.getPrometheusQueryRequest());
    PrometheusQueryRequest prometheusQueryRequest =
        prometheusMetricTable.getPrometheusQueryRequest();
    assertEquals("http_latency", prometheusQueryRequest.getPromQl());
    assertEquals(12345L, prometheusQueryRequest.getStartTime());
    assertEquals(12345L, prometheusQueryRequest.getEndTime());
    assertEquals("14", prometheusQueryRequest.getStep());
  }

  @Test
  void testArgumentsPassedByNameWithDifferentOrder() {
    QueryRangeTableFunctionResolver queryRangeTableFunctionResolver =
        new QueryRangeTableFunctionResolver(client);
    FunctionName functionName = FunctionName.of("query_range");
    List<Expression> expressions =
        List.of(
            DSL.namedArgument("query", DSL.literal("http_latency")),
            DSL.namedArgument("endtime", DSL.literal(12345)),
            DSL.namedArgument("step", DSL.literal(14)),
            DSL.namedArgument("starttime", DSL.literal(12345)));
    FunctionSignature functionSignature =
        new FunctionSignature(
            functionName, expressions.stream().map(Expression::type).collect(Collectors.toList()));

    Pair<FunctionSignature, FunctionBuilder> resolution =
        queryRangeTableFunctionResolver.resolve(functionSignature);

    assertEquals(functionName, resolution.getKey().getFunctionName());
    assertEquals(functionName, queryRangeTableFunctionResolver.getFunctionName());
    assertEquals(List.of(STRING, LONG, LONG, STRING), resolution.getKey().getParamTypeList());
    FunctionBuilder functionBuilder = resolution.getValue();
    TableFunctionImplementation functionImplementation =
        (TableFunctionImplementation) functionBuilder.apply(functionProperties, expressions);
    assertTrue(functionImplementation instanceof QueryRangeFunctionImplementation);
    PrometheusMetricTable prometheusMetricTable =
        (PrometheusMetricTable) functionImplementation.applyArguments();
    assertNotNull(prometheusMetricTable.getPrometheusQueryRequest());
    PrometheusQueryRequest prometheusQueryRequest =
        prometheusMetricTable.getPrometheusQueryRequest();
    assertEquals("http_latency", prometheusQueryRequest.getPromQl());
    assertEquals(12345L, prometheusQueryRequest.getStartTime());
    assertEquals(12345L, prometheusQueryRequest.getEndTime());
    assertEquals("14", prometheusQueryRequest.getStep());
  }

  @Test
  void testMixedArgumentTypes() {
    QueryRangeTableFunctionResolver queryRangeTableFunctionResolver =
        new QueryRangeTableFunctionResolver(client);
    FunctionName functionName = FunctionName.of("query_range");
    List<Expression> expressions =
        List.of(
            DSL.namedArgument("query", DSL.literal("http_latency")),
            DSL.namedArgument(null, DSL.literal(12345)),
            DSL.namedArgument(null, DSL.literal(12345)),
            DSL.namedArgument(null, DSL.literal(14)));
    FunctionSignature functionSignature =
        new FunctionSignature(
            functionName, expressions.stream().map(Expression::type).collect(Collectors.toList()));
    Pair<FunctionSignature, FunctionBuilder> resolution =
        queryRangeTableFunctionResolver.resolve(functionSignature);

    assertEquals(functionName, resolution.getKey().getFunctionName());
    assertEquals(functionName, queryRangeTableFunctionResolver.getFunctionName());
    assertEquals(List.of(STRING, LONG, LONG, STRING), resolution.getKey().getParamTypeList());
    SemanticCheckException exception =
        assertThrows(
            SemanticCheckException.class,
            () -> resolution.getValue().apply(functionProperties, expressions));

    assertEquals("Arguments should be either passed by name or position", exception.getMessage());
  }

  @Test
  void testWrongArgumentsSizeWhenPassedByName() {
    QueryRangeTableFunctionResolver queryRangeTableFunctionResolver =
        new QueryRangeTableFunctionResolver(client);
    FunctionName functionName = FunctionName.of("query_range");
    List<Expression> expressions =
        List.of(
            DSL.namedArgument("query", DSL.literal("http_latency")),
            DSL.namedArgument("step", DSL.literal(12345)));
    FunctionSignature functionSignature =
        new FunctionSignature(
            functionName, expressions.stream().map(Expression::type).collect(Collectors.toList()));
    Pair<FunctionSignature, FunctionBuilder> resolution =
        queryRangeTableFunctionResolver.resolve(functionSignature);

    assertEquals(functionName, resolution.getKey().getFunctionName());
    assertEquals(functionName, queryRangeTableFunctionResolver.getFunctionName());
    assertEquals(List.of(STRING, LONG, LONG, STRING), resolution.getKey().getParamTypeList());
    SemanticCheckException exception =
        assertThrows(
            SemanticCheckException.class,
            () -> resolution.getValue().apply(functionProperties, expressions));

    assertEquals("Missing arguments:[endtime,starttime]", exception.getMessage());
  }

  @Test
  void testWrongArgumentsSizeWhenPassedByPosition() {
    QueryRangeTableFunctionResolver queryRangeTableFunctionResolver =
        new QueryRangeTableFunctionResolver(client);
    FunctionName functionName = FunctionName.of("query_range");
    List<Expression> expressions =
        List.of(
            DSL.namedArgument(null, DSL.literal("http_latency")),
            DSL.namedArgument(null, DSL.literal(12345)));
    FunctionSignature functionSignature =
        new FunctionSignature(
            functionName, expressions.stream().map(Expression::type).collect(Collectors.toList()));
    Pair<FunctionSignature, FunctionBuilder> resolution =
        queryRangeTableFunctionResolver.resolve(functionSignature);

    assertEquals(functionName, resolution.getKey().getFunctionName());
    assertEquals(functionName, queryRangeTableFunctionResolver.getFunctionName());
    assertEquals(List.of(STRING, LONG, LONG, STRING), resolution.getKey().getParamTypeList());
    SemanticCheckException exception =
        assertThrows(
            SemanticCheckException.class,
            () -> resolution.getValue().apply(functionProperties, expressions));

    assertEquals("Missing arguments:[endtime,step]", exception.getMessage());
  }
}
