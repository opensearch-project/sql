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
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;

@UtilityClass
public class OpenSearchFunctions {
  public void register(BuiltinFunctionRepository repository) {
    repository.register(match());
    repository.register(simple_query_string());
  }

  private static FunctionResolver match() {
    FunctionName funcName = BuiltinFunctionName.MATCH.getName();
    // At most field, query, and all optional parameters
    final int matchMaxNumParameters = 14;
    return getRelevanceFunctionResolver(funcName, matchMaxNumParameters, STRING);
  }

  private static FunctionResolver simple_query_string() {
    FunctionName funcName = BuiltinFunctionName.SIMPLE_QUERY_STRING.getName();
    // At most field, query, and all optional parameters
    final int simpleQueryStringMaxNumParameters = 12;
    return getRelevanceFunctionResolver(funcName, simpleQueryStringMaxNumParameters, STRUCT);
  }

  private static FunctionResolver getRelevanceFunctionResolver(
      FunctionName funcName, int maxNumParameters, ExprCoreType firstArgType) {
    return new FunctionResolver(funcName,
      getRelevanceFunctionSignatureMap(funcName, maxNumParameters, firstArgType));
  }

  private static Map<FunctionSignature, FunctionBuilder> getRelevanceFunctionSignatureMap(
      FunctionName funcName, int maxNumParameters, ExprCoreType firstArgType) {
    final int minNumParameters = 2;
    FunctionBuilder buildFunction = args -> new OpenSearchFunction(funcName, args);
    var signatureMapBuilder = ImmutableMap.<FunctionSignature, FunctionBuilder>builder();
    for (int numParameters = minNumParameters; numParameters <= maxNumParameters; numParameters++) {
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
