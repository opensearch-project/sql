/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;

@UtilityClass
public class OpenSearchFunctions {

  public static final int MATCH_MAX_NUM_PARAMETERS = 12;
  public static final int MATCH_PHRASE_MAX_NUM_PARAMETERS = 3;
  public static final int MIN_NUM_PARAMETERS = 2;
  private static final int MATCH_PHRASE_PREFIX_MAX_NUM_PARAMETERS = 0;

  /**
   * Add functions specific to OpenSearch to repository.
   */
  public void register(BuiltinFunctionRepository repository) {
    repository.register(match());
    // Register MATCHPHRASE as MATCH_PHRASE as well for backwards
    // compatibility.
    repository.register(match_phrase(BuiltinFunctionName.MATCH_PHRASE));
    repository.register(match_phrase(BuiltinFunctionName.MATCHPHRASE));
    repository.register(match_phrase_prefix());
  }

  private static FunctionResolver match() {
    FunctionName funcName = BuiltinFunctionName.MATCH.getName();
    return getRelevanceFunctionResolver(funcName, MATCH_MAX_NUM_PARAMETERS);
  }

  private static FunctionResolver match_phrase_prefix() {
    FunctionName funcName = BuiltinFunctionName.MATCH_PHRASE_PREFIX.getName();
    return getRelevanceFunctionResolver(funcName, MATCH_PHRASE_PREFIX_MAX_NUM_PARAMETERS);
  }
  
  private static FunctionResolver match_phrase(BuiltinFunctionName matchPhrase) {
    FunctionName funcName = matchPhrase.getName();
    return getRelevanceFunctionResolver(funcName, MATCH_PHRASE_MAX_NUM_PARAMETERS);
  }

  private static FunctionResolver getRelevanceFunctionResolver(
      FunctionName funcName, int maxNumParameters) {
    return new FunctionResolver(funcName,
      getRelevanceFunctionSignatureMap(funcName, maxNumParameters));
  }

  private static Map<FunctionSignature, FunctionBuilder> getRelevanceFunctionSignatureMap(
      FunctionName funcName, int numOptionalParameters) {
    FunctionBuilder buildFunction = args -> new OpenSearchFunction(funcName, args);
    var signatureMapBuilder = ImmutableMap.<FunctionSignature, FunctionBuilder>builder();
    for (int numParameters = MIN_NUM_PARAMETERS;
         numParameters <= MIN_NUM_PARAMETERS + numOptionalParameters;
         numParameters++) {
      List<ExprType> args = Collections.nCopies(numParameters, STRING);
      signatureMapBuilder.put(new FunctionSignature(funcName, args), buildFunction);
    }
    return  signatureMapBuilder.build();
  }

  private static class OpenSearchFunction extends FunctionExpression {
    private final FunctionName functionName;
    private final List<Expression> arguments;

    public OpenSearchFunction(FunctionName functionName, List<Expression> arguments) {
      super(functionName, arguments);
      this.functionName = functionName;
      this.arguments = arguments;
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
      throw new UnsupportedOperationException(String.format(
          "OpenSearch defined function [%s] is only supported in WHERE and HAVING clause.",
          functionName));
    }

    @Override
    public ExprType type() {
      return ExprCoreType.BOOLEAN;
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
