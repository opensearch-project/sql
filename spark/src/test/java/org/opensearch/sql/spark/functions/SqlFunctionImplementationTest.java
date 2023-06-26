/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.functions;

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
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.functions.implementation.SqlFunctionImplementation;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.spark.storage.SparkMetricTable;

@ExtendWith(MockitoExtension.class)
public class SqlFunctionImplementationTest {
  @Mock
  private SparkClient client;

  @Test
  void testValueOfAndTypeToString() {
    FunctionName functionName = new FunctionName("sql");
    List<Expression> namedArgumentExpressionList
        = List.of(DSL.namedArgument("query", DSL.literal("select 1")));
    SqlFunctionImplementation sqlFunctionImplementation
        = new SqlFunctionImplementation(functionName, namedArgumentExpressionList, client);
    UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
        () -> sqlFunctionImplementation.valueOf());
    assertEquals("Spark defined function [sql] is only "
        + "supported in SOURCE clause with spark connector catalog", exception.getMessage());
    assertEquals("sql(query=\"select 1\")",
        sqlFunctionImplementation.toString());
    assertEquals(ExprCoreType.STRUCT, sqlFunctionImplementation.type());
  }


  @Test
  void testApplyArguments() {
    FunctionName functionName = new FunctionName("sql");
    List<Expression> namedArgumentExpressionList
        = List.of(DSL.namedArgument("query", DSL.literal("select 1")));
    SqlFunctionImplementation sqlFunctionImplementation
        = new SqlFunctionImplementation(functionName, namedArgumentExpressionList, client);
    SparkMetricTable sparkMetricTable
        = (SparkMetricTable) sqlFunctionImplementation.applyArguments();
    assertNotNull(sparkMetricTable.getSparkQueryRequest());
    SparkQueryRequest sparkQueryRequest
        = sparkMetricTable.getSparkQueryRequest();
    assertEquals("select 1", sparkQueryRequest.getSql());
  }

  @Test
  void testApplyArgumentsException() {
    FunctionName functionName = new FunctionName("sql");
    List<Expression> namedArgumentExpressionList
        = List.of(DSL.namedArgument("query", DSL.literal("select 1")),
        DSL.namedArgument("tmp", DSL.literal(12345)));
    SqlFunctionImplementation sqlFunctionImplementation
        = new SqlFunctionImplementation(functionName, namedArgumentExpressionList, client);
    ExpressionEvaluationException exception = assertThrows(ExpressionEvaluationException.class,
        () -> sqlFunctionImplementation.applyArguments());
    assertEquals("Invalid Function Argument:tmp", exception.getMessage());
  }

}