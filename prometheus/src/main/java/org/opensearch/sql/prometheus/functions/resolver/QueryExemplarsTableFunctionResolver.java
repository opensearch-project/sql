/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
import org.opensearch.sql.prometheus.functions.implementation.QueryExemplarFunctionImplementation;

/**
 * This class is for query_exemplars table function resolver {@link FunctionResolver}.
 * It takes care of validating function arguments and also creating
 * required {@link org.opensearch.sql.expression.function.TableFunctionImplementation} Class.
 */
@RequiredArgsConstructor
public class QueryExemplarsTableFunctionResolver implements FunctionResolver {

  private final PrometheusClient prometheusClient;
  public static final String QUERY_EXEMPLARS = "query_exemplars";

  public static final String QUERY = "query";
  public static final String STARTTIME = "starttime";
  public static final String ENDTIME = "endtime";

  @Override
  public Pair<FunctionSignature, FunctionBuilder> resolve(FunctionSignature unresolvedSignature) {
    final FunctionName functionName = FunctionName.of(QUERY_EXEMPLARS);
    FunctionSignature functionSignature =
        new FunctionSignature(FunctionName.of(QUERY_EXEMPLARS), List.of(STRING, LONG, LONG));
    FunctionBuilder functionBuilder =  (functionProperties, arguments) -> {
      final List<String> argumentNames = List.of(QUERY, STARTTIME, ENDTIME);
      validatePrometheusTableFunctionArguments(arguments, argumentNames);
      List<Expression> namedArguments = getNamedArgumentsOfTableFunction(arguments, argumentNames);
      return new QueryExemplarFunctionImplementation(functionName,
          namedArguments, prometheusClient);
    };
    return Pair.of(functionSignature, functionBuilder);
  }

  @Override
  public FunctionName getFunctionName() {
    return FunctionName.of(QUERY_EXEMPLARS);
  }
}
