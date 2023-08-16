/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.functions.resolver;

import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.prometheus.utils.TableFunctionUtils.getNamedArgumentsOfTableFunction;
import static org.opensearch.sql.prometheus.utils.TableFunctionUtils.validatePrometheusTableFunctionArguments;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.functions.implementation.QueryRangeFunctionImplementation;

@RequiredArgsConstructor
public class QueryRangeTableFunctionResolver implements FunctionResolver {

  private final PrometheusClient prometheusClient;
  public static final String QUERY_RANGE = "query_range";
  public static final String QUERY = "query";
  public static final String STARTTIME = "starttime";
  public static final String ENDTIME = "endtime";
  public static final String STEP = "step";

  @Override
  public Pair<FunctionSignature, FunctionBuilder> resolve(FunctionSignature unresolvedSignature) {
    FunctionName functionName = FunctionName.of(QUERY_RANGE);
    FunctionSignature functionSignature =
        new FunctionSignature(functionName, List.of(STRING, LONG, LONG, STRING));
    final List<String> argumentNames = List.of(QUERY, STARTTIME, ENDTIME, STEP);
    FunctionBuilder functionBuilder = (functionProperties, arguments) -> {
      validatePrometheusTableFunctionArguments(arguments, argumentNames);
      List<Expression> namedArguments = getNamedArgumentsOfTableFunction(arguments, argumentNames);
      return new QueryRangeFunctionImplementation(functionName, namedArguments, prometheusClient);
    };
    return Pair.of(functionSignature, functionBuilder);
  }

  @Override
  public FunctionName getFunctionName() {
    return FunctionName.of(QUERY_RANGE);
  }

}
