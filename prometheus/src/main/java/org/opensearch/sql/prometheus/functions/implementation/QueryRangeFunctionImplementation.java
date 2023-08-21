/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.functions.implementation;

import static org.opensearch.sql.prometheus.functions.resolver.QueryRangeTableFunctionResolver.ENDTIME;
import static org.opensearch.sql.prometheus.functions.resolver.QueryRangeTableFunctionResolver.QUERY;
import static org.opensearch.sql.prometheus.functions.resolver.QueryRangeTableFunctionResolver.STARTTIME;
import static org.opensearch.sql.prometheus.functions.resolver.QueryRangeTableFunctionResolver.STEP;

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
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.prometheus.storage.PrometheusMetricTable;
import org.opensearch.sql.storage.Table;

public class QueryRangeFunctionImplementation extends FunctionExpression
    implements TableFunctionImplementation {

  private final FunctionName functionName;
  private final List<Expression> arguments;
  private final PrometheusClient prometheusClient;

  /**
   * Required argument constructor.
   *
   * @param functionName name of the function
   * @param arguments a list of expressions
   */
  public QueryRangeFunctionImplementation(
      FunctionName functionName, List<Expression> arguments, PrometheusClient prometheusClient) {
    super(functionName, arguments);
    this.functionName = functionName;
    this.arguments = arguments;
    this.prometheusClient = prometheusClient;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    throw new UnsupportedOperationException(
        String.format(
            "Prometheus defined function [%s] is only "
                + "supported in SOURCE clause with prometheus connector catalog",
            functionName));
  }

  @Override
  public ExprType type() {
    return ExprCoreType.STRUCT;
  }

  @Override
  public String toString() {
    List<String> args =
        arguments.stream()
            .map(
                arg ->
                    String.format(
                        "%s=%s",
                        ((NamedArgumentExpression) arg).getArgName(),
                        ((NamedArgumentExpression) arg).getValue().toString()))
            .collect(Collectors.toList());
    return String.format("%s(%s)", functionName, String.join(", ", args));
  }

  @Override
  public Table applyArguments() {
    return new PrometheusMetricTable(prometheusClient, buildQueryFromQueryRangeFunction(arguments));
  }

  private PrometheusQueryRequest buildQueryFromQueryRangeFunction(List<Expression> arguments) {

    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    arguments.forEach(
        arg -> {
          String argName = ((NamedArgumentExpression) arg).getArgName();
          Expression argValue = ((NamedArgumentExpression) arg).getValue();
          ExprValue literalValue = argValue.valueOf();
          switch (argName) {
            case QUERY:
              prometheusQueryRequest.setPromQl((String) literalValue.value());
              break;
            case STARTTIME:
              prometheusQueryRequest.setStartTime(((Number) literalValue.value()).longValue());
              break;
            case ENDTIME:
              prometheusQueryRequest.setEndTime(((Number) literalValue.value()).longValue());
              break;
            case STEP:
              prometheusQueryRequest.setStep(literalValue.value().toString());
              break;
            default:
              throw new ExpressionEvaluationException(
                  String.format("Invalid Function Argument:%s", argName));
          }
        });
    return prometheusQueryRequest;
  }
}
