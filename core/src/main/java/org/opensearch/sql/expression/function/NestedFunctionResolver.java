/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.nested.NestedFunction;

@RequiredArgsConstructor
public class NestedFunctionResolver
    implements FunctionResolver {

  @Getter
  private final FunctionName functionName;

  @Override
  public Pair<FunctionSignature, FunctionBuilder> resolve(FunctionSignature unresolvedSignature) {
    if (!unresolvedSignature.getFunctionName().equals(functionName)) {
      throw new SemanticCheckException(String.format("Expected '%s' but got '%s'",
          functionName.getFunctionName(), unresolvedSignature.getFunctionName().getFunctionName()));
    }

    FunctionBuilder buildFunction = (functionProperties, args)
        -> new NestedFunction(functionName, args);
    return Pair.of(unresolvedSignature, buildFunction);
  }
}
