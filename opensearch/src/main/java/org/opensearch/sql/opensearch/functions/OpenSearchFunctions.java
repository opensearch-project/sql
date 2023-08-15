/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.functions;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.expression.function.FunctionSignature;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;

@UtilityClass
public class OpenSearchFunctions {
  public Collection<FunctionResolver> getResolvers() {
    return List.of(
        match_bool_prefix(),
        multi_match(BuiltinFunctionName.MULTI_MATCH),
        multi_match(BuiltinFunctionName.MULTIMATCH),
        multi_match(BuiltinFunctionName.MULTIMATCHQUERY),
        match(BuiltinFunctionName.MATCH),
        match(BuiltinFunctionName.MATCHQUERY),
        match(BuiltinFunctionName.MATCH_QUERY),
        simple_query_string(),
        query(),
        query_string(),

        // Register MATCHPHRASE as MATCH_PHRASE as well for backwards
        // compatibility.
        match_phrase(BuiltinFunctionName.MATCH_PHRASE),
        match_phrase(BuiltinFunctionName.MATCHPHRASE),
        match_phrase(BuiltinFunctionName.MATCHPHRASEQUERY),
        match_phrase_prefix(),
        wildcard_query(BuiltinFunctionName.WILDCARD_QUERY),
        wildcard_query(BuiltinFunctionName.WILDCARDQUERY),
        score(BuiltinFunctionName.SCORE),
        score(BuiltinFunctionName.SCOREQUERY),
        score(BuiltinFunctionName.SCORE_QUERY),
        nested()
        );
  }

  private static FunctionResolver match_bool_prefix() {
    FunctionName name = BuiltinFunctionName.MATCH_BOOL_PREFIX.getName();
    return new RelevanceFunctionResolver(name);
  }

  private static FunctionResolver match(BuiltinFunctionName match) {
    FunctionName funcName = match.getName();
    return new RelevanceFunctionResolver(funcName);
  }

  private static FunctionResolver match_phrase_prefix() {
    FunctionName funcName = BuiltinFunctionName.MATCH_PHRASE_PREFIX.getName();
    return new RelevanceFunctionResolver(funcName);
  }

  private static FunctionResolver match_phrase(BuiltinFunctionName matchPhrase) {
    FunctionName funcName = matchPhrase.getName();
    return new RelevanceFunctionResolver(funcName);
  }

  private static FunctionResolver multi_match(BuiltinFunctionName multiMatchName) {
    return new RelevanceFunctionResolver(multiMatchName.getName());
  }

  private static FunctionResolver simple_query_string() {
    FunctionName funcName = BuiltinFunctionName.SIMPLE_QUERY_STRING.getName();
    return new RelevanceFunctionResolver(funcName);
  }

  private static FunctionResolver query() {
    FunctionName funcName = BuiltinFunctionName.QUERY.getName();
    return new RelevanceFunctionResolver(funcName);
  }

  private static FunctionResolver query_string() {
    FunctionName funcName = BuiltinFunctionName.QUERY_STRING.getName();
    return new RelevanceFunctionResolver(funcName);
  }

  private static FunctionResolver wildcard_query(BuiltinFunctionName wildcardQuery) {
    FunctionName funcName = wildcardQuery.getName();
    return new RelevanceFunctionResolver(funcName);
  }

  private static FunctionResolver nested() {
    return new FunctionResolver() {
      @Override
      public Pair<FunctionSignature, FunctionBuilder> resolve(
          FunctionSignature unresolvedSignature) {
        return Pair.of(unresolvedSignature,
            (functionProperties, arguments) ->
            new FunctionExpression(BuiltinFunctionName.NESTED.getName(), arguments) {
              @Override
              public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
                return valueEnv.resolve(getArguments().get(0));
              }

              @Override
              public ExprType type() {
                return getArguments().get(0).type();
              }
            });
      }

      @Override
      public FunctionName getFunctionName() {
        return BuiltinFunctionName.NESTED.getName();
      }
    };
  }




  private static FunctionResolver score(BuiltinFunctionName score) {
    FunctionName funcName = score.getName();
    return new RelevanceFunctionResolver(funcName);
  }

  public static class OpenSearchFunction extends FunctionExpression {
    private final FunctionName functionName;
    private final List<Expression> arguments;

    @Getter
    @Setter
    private boolean isScoreTracked;

    /**
     * Required argument constructor.
     * @param functionName name of the function
     * @param arguments a list of expressions
     */
    public OpenSearchFunction(FunctionName functionName, List<Expression> arguments) {
      super(functionName, arguments);
      this.functionName = functionName;
      this.arguments = arguments;
      this.isScoreTracked = false;
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
      throw new UnsupportedOperationException(String.format(
          "OpenSearch defined function [%s] is only supported in WHERE and HAVING clause.",
          functionName));
    }

    @Override
    public ExprType type() {
      return BOOLEAN;
    }

    @Override
    public String toString() {
      List<String> args = arguments.stream()
          .map(arg -> String.format("%s=%s", ((NamedArgumentExpression) arg)
              .getArgName(), ((NamedArgumentExpression) arg).getValue().toString()))
          .collect(Collectors.toList());
      return String.format("%s(%s)", functionName, String.join(", ", args));
    }
  }
}
