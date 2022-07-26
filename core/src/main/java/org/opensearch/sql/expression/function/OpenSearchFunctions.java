/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.analysis.HighlightExpression;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.common.utils.StringUtils;

@UtilityClass
public class OpenSearchFunctions {

  public static final int MATCH_MAX_NUM_PARAMETERS = 14;
  public static final int MATCH_BOOL_PREFIX_MAX_NUM_PARAMETERS = 9;
  public static final int MATCH_PHRASE_MAX_NUM_PARAMETERS = 5;
  public static final int MIN_NUM_PARAMETERS = 2;
  public static final int MULTI_MATCH_MAX_NUM_PARAMETERS = 17;
  public static final int SIMPLE_QUERY_STRING_MAX_NUM_PARAMETERS = 14;
  public static final int QUERY_STRING_MAX_NUM_PARAMETERS = 25;
  public static final int MATCH_PHRASE_PREFIX_MAX_NUM_PARAMETERS = 7;

  /**
   * Add functions specific to OpenSearch to repository.
   */
  public void register(BuiltinFunctionRepository repository) {
    repository.register(match_bool_prefix());
    repository.register(match());
    repository.register(multi_match());
    repository.register(simple_query_string());
    repository.register(query_string());
    // Register MATCHPHRASE as MATCH_PHRASE as well for backwards
    // compatibility.
    repository.register(match_phrase(BuiltinFunctionName.MATCH_PHRASE));
    repository.register(match_phrase(BuiltinFunctionName.MATCHPHRASE));
    repository.register(match_phrase_prefix());
    repository.register(highlight());
  }

  private static FunctionResolver highlight() {
    FunctionName functionName = BuiltinFunctionName.HIGHLIGHT.getName();
    FunctionSignature functionSignature = new FunctionSignature(functionName, List.of(STRING));
    FunctionBuilder functionBuilder = arguments -> new HighlightExpression(arguments.get(0));
    return new FunctionResolver(functionName, ImmutableMap.of(functionSignature, functionBuilder));
  }

  private static FunctionResolver match_bool_prefix() {
    FunctionName name = BuiltinFunctionName.MATCH_BOOL_PREFIX.getName();
    return getRelevanceFunctionResolver(name, MATCH_BOOL_PREFIX_MAX_NUM_PARAMETERS, STRING);
  }

  private static FunctionResolver match() {
    FunctionName funcName = BuiltinFunctionName.MATCH.getName();
    return getRelevanceFunctionResolver(funcName, MATCH_MAX_NUM_PARAMETERS, STRING);
  }

  private static FunctionResolver match_phrase_prefix() {
    FunctionName funcName = BuiltinFunctionName.MATCH_PHRASE_PREFIX.getName();
    return getRelevanceFunctionResolver(funcName, MATCH_PHRASE_PREFIX_MAX_NUM_PARAMETERS, STRING);
  }

  private static FunctionResolver match_phrase(BuiltinFunctionName matchPhrase) {
    FunctionName funcName = matchPhrase.getName();
    return getRelevanceFunctionResolver(funcName, MATCH_PHRASE_MAX_NUM_PARAMETERS, STRING);
  }

  private static FunctionResolver multi_match() {
    FunctionName funcName = BuiltinFunctionName.MULTI_MATCH.getName();
    return getRelevanceFunctionResolver(funcName, MULTI_MATCH_MAX_NUM_PARAMETERS, STRUCT);
  }

  private static FunctionResolver simple_query_string() {
    FunctionName funcName = BuiltinFunctionName.SIMPLE_QUERY_STRING.getName();
    return getRelevanceFunctionResolver(funcName, SIMPLE_QUERY_STRING_MAX_NUM_PARAMETERS, STRUCT);
  }

  private static FunctionResolver query_string() {
    FunctionName funcName = BuiltinFunctionName.QUERY_STRING.getName();
    return getRelevanceFunctionResolver(funcName, QUERY_STRING_MAX_NUM_PARAMETERS, STRUCT);
  }

  private static FunctionResolver getRelevanceFunctionResolver(
      FunctionName funcName, int maxNumParameters, ExprCoreType firstArgType) {
    return new FunctionResolver(funcName,
      getRelevanceFunctionSignatureMap(funcName, maxNumParameters, firstArgType));
  }

  private static Map<FunctionSignature, FunctionBuilder> getRelevanceFunctionSignatureMap(
      FunctionName funcName, int maxNumParameters, ExprCoreType firstArgType) {
    FunctionBuilder buildFunction = args -> new OpenSearchFunction(funcName, args);
    var signatureMapBuilder = ImmutableMap.<FunctionSignature, FunctionBuilder>builder();
    for (int numParameters = MIN_NUM_PARAMETERS;
         numParameters <= maxNumParameters; numParameters++) {
      List<ExprType> args = new ArrayList<>(Collections.nCopies(numParameters - 1, STRING));
      args.add(0, firstArgType);
      signatureMapBuilder.put(new FunctionSignature(funcName, args), buildFunction);
    }
    return signatureMapBuilder.build();
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
