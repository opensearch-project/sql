/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;

import java.util.List;
import java.util.stream.Collectors;
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

@UtilityClass
public class OpenSearchFunctions {
  /** Add functions specific to OpenSearch to repository. */
  public void register(BuiltinFunctionRepository repository) {
    repository.register(match_bool_prefix());
    repository.register(multi_match(BuiltinFunctionName.MULTI_MATCH));
    repository.register(multi_match(BuiltinFunctionName.MULTIMATCH));
    repository.register(multi_match(BuiltinFunctionName.MULTIMATCHQUERY));
    repository.register(match(BuiltinFunctionName.MATCH));
    repository.register(match(BuiltinFunctionName.MATCHQUERY));
    repository.register(match(BuiltinFunctionName.MATCH_QUERY));
    repository.register(simple_query_string());
    repository.register(query());
    repository.register(query_string());

    // Register MATCHPHRASE as MATCH_PHRASE as well for backwards
    // compatibility.
    repository.register(match_phrase(BuiltinFunctionName.MATCH_PHRASE));
    repository.register(match_phrase(BuiltinFunctionName.MATCHPHRASE));
    repository.register(match_phrase(BuiltinFunctionName.MATCHPHRASEQUERY));
    repository.register(match_phrase_prefix());
    repository.register(wildcard_query(BuiltinFunctionName.WILDCARD_QUERY));
    repository.register(wildcard_query(BuiltinFunctionName.WILDCARDQUERY));
    repository.register(score(BuiltinFunctionName.SCORE));
    repository.register(score(BuiltinFunctionName.SCOREQUERY));
    repository.register(score(BuiltinFunctionName.SCORE_QUERY));
    // Functions supported in SELECT clause
    repository.register(nested());
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
        return Pair.of(
            unresolvedSignature,
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
    private final ExprType returnType;

    @Getter @Setter private boolean isScoreTracked;

    public OpenSearchFunction(FunctionName functionName, List<Expression> arguments, ExprType returnType) {
      super(functionName, arguments);
      this.functionName = functionName;
      this.arguments = arguments;
      this.returnType = returnType;
      this.isScoreTracked = false;
    }

    /**
     * Required argument constructor.
     *
     * @param functionName name of the function
     * @param arguments a list of expressions
     */
    public OpenSearchFunction(FunctionName functionName, List<Expression> arguments) {
      this(functionName, arguments, BOOLEAN);
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
      throw new UnsupportedOperationException(
          String.format(
              "OpenSearch defined function [%s] is only supported in WHERE clause, HAVING clause and Eval operation.",
              functionName));
    }

    @Override
    public ExprType type() {
      return returnType;
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
  }
}
