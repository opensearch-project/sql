/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.functions.resolver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.expression.function.TableFunctionImplementation;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.functions.implementation.QueryExemplarFunctionImplementation;
import org.opensearch.sql.prometheus.request.PrometheusQueryExemplarsRequest;
import org.opensearch.sql.prometheus.storage.QueryExemplarsTable;

@ExtendWith(MockitoExtension.class)
class QueryExemplarsTableFunctionResolverTest {

  @Mock
  private PrometheusClient client;

  @Mock
  private FunctionProperties functionProperties;

  @Test
  void testResolve() {
    QueryExemplarsTableFunctionResolver queryExemplarsTableFunctionResolver
        = new QueryExemplarsTableFunctionResolver(client);
    FunctionName functionName = FunctionName.of("query_exemplars");
    List<Expression> expressions
        = List.of(DSL.namedArgument("query", DSL.literal("http_latency")),
        DSL.namedArgument("starttime", DSL.literal(12345)),
        DSL.namedArgument("endtime", DSL.literal(12345)));
    FunctionSignature functionSignature = new FunctionSignature(functionName, expressions
        .stream().map(Expression::type).collect(Collectors.toList()));
    Pair<FunctionSignature, FunctionBuilder> resolution
        = queryExemplarsTableFunctionResolver.resolve(functionSignature);
    assertEquals(functionName, resolution.getKey().getFunctionName());
    assertEquals(functionName, queryExemplarsTableFunctionResolver.getFunctionName());
    assertEquals(List.of(STRING, LONG, LONG), resolution.getKey().getParamTypeList());
    FunctionBuilder functionBuilder = resolution.getValue();
    TableFunctionImplementation functionImplementation
        = (TableFunctionImplementation) functionBuilder.apply(functionProperties, expressions);
    assertTrue(functionImplementation instanceof QueryExemplarFunctionImplementation);
    QueryExemplarsTable queryExemplarsTable
        = (QueryExemplarsTable) functionImplementation.applyArguments();
    assertNotNull(queryExemplarsTable.getExemplarsRequest());
    PrometheusQueryExemplarsRequest prometheusQueryExemplarsRequest =
        queryExemplarsTable.getExemplarsRequest();
    assertEquals("http_latency", prometheusQueryExemplarsRequest.getQuery());
    assertEquals(12345L, prometheusQueryExemplarsRequest.getStartTime());
    assertEquals(12345L, prometheusQueryExemplarsRequest.getEndTime());
  }

}

