/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.spark.constants.TestConstants.QUERY;

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
import org.opensearch.sql.spark.functions.implementation.SparkSqlFunctionImplementation;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.spark.storage.SparkTable;

@ExtendWith(MockitoExtension.class)
public class SparkSqlFunctionImplementationTest {
  @Mock private SparkClient client;

  @Test
  void testValueOfAndTypeToString() {
    FunctionName functionName = new FunctionName("sql");
    List<Expression> namedArgumentExpressionList =
        List.of(DSL.namedArgument("query", DSL.literal(QUERY)));
    SparkSqlFunctionImplementation sparkSqlFunctionImplementation =
        new SparkSqlFunctionImplementation(functionName, namedArgumentExpressionList, client);
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class, () -> sparkSqlFunctionImplementation.valueOf());
    assertEquals(
        "Spark defined function [sql] is only "
            + "supported in SOURCE clause with spark connector catalog",
        exception.getMessage());
    assertEquals("sql(query=\"select 1\")", sparkSqlFunctionImplementation.toString());
    assertEquals(ExprCoreType.STRUCT, sparkSqlFunctionImplementation.type());
  }

  @Test
  void testApplyArguments() {
    FunctionName functionName = new FunctionName("sql");
    List<Expression> namedArgumentExpressionList =
        List.of(DSL.namedArgument("query", DSL.literal(QUERY)));
    SparkSqlFunctionImplementation sparkSqlFunctionImplementation =
        new SparkSqlFunctionImplementation(functionName, namedArgumentExpressionList, client);
    SparkTable sparkTable = (SparkTable) sparkSqlFunctionImplementation.applyArguments();
    assertNotNull(sparkTable.getSparkQueryRequest());
    SparkQueryRequest sparkQueryRequest = sparkTable.getSparkQueryRequest();
    assertEquals(QUERY, sparkQueryRequest.getSql());
  }

  @Test
  void testApplyArgumentsException() {
    FunctionName functionName = new FunctionName("sql");
    List<Expression> namedArgumentExpressionList =
        List.of(
            DSL.namedArgument("query", DSL.literal(QUERY)),
            DSL.namedArgument("tmp", DSL.literal(12345)));
    SparkSqlFunctionImplementation sparkSqlFunctionImplementation =
        new SparkSqlFunctionImplementation(functionName, namedArgumentExpressionList, client);
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> sparkSqlFunctionImplementation.applyArguments());
    assertEquals("Invalid Function Argument:tmp", exception.getMessage());
  }
}
