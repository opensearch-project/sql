/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.functions.implementation;

import static org.opensearch.sql.spark.functions.resolver.SqlTableFunctionResolver.QUERY;

import java.util.List;
import java.util.stream.Collectors;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.TableFunctionImplementation;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.spark.storage.SparkMetricTable;
import org.opensearch.sql.storage.Table;

/**
 * Spark SQL function implementation.
 */
public class SqlFunctionImplementation extends FunctionExpression
    implements TableFunctionImplementation {

  private final FunctionName functionName;
  private final List<Expression> arguments;
  private final SparkClient sparkClient;

  /**
   * Constructor for spark sql function.
   *
   * @param functionName name of the function
   * @param arguments    a list of expressions
   * @param sparkClient  spark client
   */
  public SqlFunctionImplementation(
      FunctionName functionName, List<Expression> arguments, SparkClient sparkClient) {
    super(functionName, arguments);
    this.functionName = functionName;
    this.arguments = arguments;
    this.sparkClient = sparkClient;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    throw new UnsupportedOperationException(String.format(
        "Spark defined function [%s] is only "
            + "supported in SOURCE clause with spark connector catalog", functionName));
  }

  @Override
  public ExprType type() {
    return ExprCoreType.STRUCT;
  }

  @Override
  public String toString() {
    List<String> args = arguments.stream()
        .map(arg -> String.format("%s=%s",
            ((NamedArgumentExpression) arg).getArgName(),
            ((NamedArgumentExpression) arg).getValue().toString()))
        .collect(Collectors.toList());
    return String.format("%s(%s)", functionName, String.join(", ", args));
  }

  @Override
  public Table applyArguments() {
    return new SparkMetricTable(sparkClient, buildQueryFromSqlFunction(arguments));
  }

  /**
   * This method builds a spark query request.
   *
   * @param arguments spark sql function arguments
   * @return          spark query request
   */
  private SparkQueryRequest buildQueryFromSqlFunction(List<Expression> arguments) {

    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    arguments.forEach(arg -> {
      String argName = ((NamedArgumentExpression) arg).getArgName();
      Expression argValue = ((NamedArgumentExpression) arg).getValue();
      ExprValue literalValue = argValue.valueOf();
      if (argName.equals(QUERY)) {
        sparkQueryRequest.setSql((String) literalValue.value());
      } else {
        throw new ExpressionEvaluationException(
            String.format("Invalid Function Argument:%s", argName));
      }
    });
    return sparkQueryRequest;
  }

}
