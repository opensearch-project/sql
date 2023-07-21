/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.functions.resolver;

import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedArgumentExpression;
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
        new FunctionSignature(functionName, List.of(STRING, LONG, LONG, LONG));
    final List<String> argumentNames = List.of(QUERY, STARTTIME, ENDTIME, STEP);

    FunctionBuilder functionBuilder = (functionProperties, arguments) -> {
      Boolean argumentsPassedByName = arguments.stream()
          .noneMatch(arg -> StringUtils.isEmpty(((NamedArgumentExpression) arg).getArgName()));
      Boolean argumentsPassedByPosition = arguments.stream()
          .allMatch(arg -> StringUtils.isEmpty(((NamedArgumentExpression) arg).getArgName()));
      if (!(argumentsPassedByName || argumentsPassedByPosition)) {
        throw new SemanticCheckException("Arguments should be either passed by name or position");
      }

      if (arguments.size() != argumentNames.size()) {
        throw new SemanticCheckException(
            generateErrorMessageForMissingArguments(argumentsPassedByPosition, arguments,
                argumentNames));
      }

      if (argumentsPassedByPosition) {
        List<Expression> namedArguments = new ArrayList<>();
        for (int i = 0; i < arguments.size(); i++) {
          namedArguments.add(new NamedArgumentExpression(argumentNames.get(i),
              ((NamedArgumentExpression) arguments.get(i)).getValue()));
        }
        return new QueryRangeFunctionImplementation(functionName, namedArguments, prometheusClient);
      }
      return new QueryRangeFunctionImplementation(functionName, arguments, prometheusClient);
    };
    return Pair.of(functionSignature, functionBuilder);
  }

  private String generateErrorMessageForMissingArguments(Boolean argumentsPassedByPosition,
                                                         List<Expression> arguments,
                                                         List<String> argumentNames) {
    if (argumentsPassedByPosition) {
      return String.format("Missing arguments:[%s]",
          String.join(",", argumentNames.subList(arguments.size(), argumentNames.size())));
    } else {
      Set<String> requiredArguments = new HashSet<>(argumentNames);
      Set<String> providedArguments =
          arguments.stream().map(expression -> ((NamedArgumentExpression) expression).getArgName())
              .collect(Collectors.toSet());
      requiredArguments.removeAll(providedArguments);
      return String.format("Missing arguments:[%s]", String.join(",", requiredArguments));
    }
  }

  @Override
  public FunctionName getFunctionName() {
    return FunctionName.of(QUERY_RANGE);
  }

}
