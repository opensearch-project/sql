/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.spark.functions.resolver;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.ArrayList;
import java.util.List;
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
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.functions.implementation.SparkSqlFunctionImplementation;

/**
 * Function resolver for sql function of spark connector.
 */
@RequiredArgsConstructor
public class SparkSqlTableFunctionResolver implements FunctionResolver {
  private final SparkClient sparkClient;

  public static final String SQL = "sql";
  public static final String QUERY = "query";

  @Override
  public Pair<FunctionSignature, FunctionBuilder> resolve(FunctionSignature unresolvedSignature) {
    FunctionName functionName = FunctionName.of(SQL);
    FunctionSignature functionSignature =
        new FunctionSignature(functionName, List.of(STRING));
    final List<String> argumentNames = List.of(QUERY);

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
            String.format("Missing arguments:[%s]",
                String.join(",", argumentNames.subList(arguments.size(), argumentNames.size()))));
      }

      if (argumentsPassedByPosition) {
        List<Expression> namedArguments = new ArrayList<>();
        for (int i = 0; i < arguments.size(); i++) {
          namedArguments.add(new NamedArgumentExpression(argumentNames.get(i),
              ((NamedArgumentExpression) arguments.get(i)).getValue()));
        }
        return new SparkSqlFunctionImplementation(functionName, namedArguments, sparkClient);
      }
      return new SparkSqlFunctionImplementation(functionName, arguments, sparkClient);
    };
    return Pair.of(functionSignature, functionBuilder);
  }

  @Override
  public FunctionName getFunctionName() {
    return FunctionName.of(SQL);
  }
}
