/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.functions;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.expression.function.OpenSearchFunction;

@RequiredArgsConstructor
public class RelevanceFunctionResolver implements FunctionResolver {
  @Getter private final FunctionName functionName;

  @Override
  public Pair<FunctionSignature, FunctionBuilder> resolve(FunctionSignature unresolvedSignature) {
    if (!unresolvedSignature.getFunctionName().equals(functionName)) {
      throw new SemanticCheckException(
          String.format(
              "Expected '%s' but got '%s'",
              functionName.getFunctionName(),
              unresolvedSignature.getFunctionName().getFunctionName()));
    }
    List<ExprType> paramTypes = unresolvedSignature.getParamTypeList();
    // Check if all but the first parameter are of type STRING.
    for (int i = 1; i < paramTypes.size(); i++) {
      ExprType paramType = paramTypes.get(i);
      if (!ExprCoreType.STRING.equals(paramType)) {
        throw new SemanticCheckException(
            getWrongParameterErrorMessage(i, paramType, ExprCoreType.STRING));
      }
    }

    FunctionBuilder buildFunction =
        (functionProperties, args) -> new OpenSearchFunction(functionName, args);
    return Pair.of(unresolvedSignature, buildFunction);
  }

  /**
   * Returns a helpful error message when expected parameter type does not match the specified
   * parameter type.
   *
   * @param i 0-based index of the parameter in a function signature.
   * @param paramType the type of the ith parameter at run-time.
   * @param expectedType the expected type of the ith parameter
   * @return A user-friendly error message that informs of the type difference.
   */
  private String getWrongParameterErrorMessage(int i, ExprType paramType, ExprType expectedType) {
    return String.format(
        "Expected type %s instead of %s for parameter #%d",
        expectedType.typeName(), paramType.typeName(), i + 1);
  }
}
