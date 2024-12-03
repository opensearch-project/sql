/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.AbstractMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.exception.ExpressionEvaluationException;

/**
 * The Function Resolver hold the overload {@link FunctionBuilder} implementation. is composed by
 * {@link FunctionName} which identified the function name and a map of {@link FunctionSignature}
 * and {@link FunctionBuilder} to represent the overloaded implementation
 */
@Builder
@RequiredArgsConstructor
public class DefaultFunctionResolver implements FunctionResolver {
  @Getter private final FunctionName functionName;

  @Singular("functionBundle")
  private final Map<FunctionSignature, FunctionBuilder> functionBundle;

  /**
   * Resolve the {@link FunctionBuilder} by using input {@link FunctionSignature}. If the {@link
   * FunctionBuilder} exactly match the input {@link FunctionSignature}, return it. If applying the
   * widening rule, found the most match one, return it. If nothing found, throw {@link
   * ExpressionEvaluationException}
   *
   * @return function signature and its builder
   */
  @Override
  public Pair<FunctionSignature, FunctionBuilder> resolve(FunctionSignature unresolvedSignature) {
    PriorityQueue<Map.Entry<Integer, FunctionSignature>> functionMatchQueue =
        new PriorityQueue<>(Map.Entry.comparingByKey());

    for (FunctionSignature functionSignature : functionBundle.keySet()) {
      functionMatchQueue.add(
          new AbstractMap.SimpleEntry<>(
              unresolvedSignature.match(functionSignature), functionSignature));
    }
    Map.Entry<Integer, FunctionSignature> bestMatchEntry = functionMatchQueue.peek();
    if (FunctionSignature.isVarArgFunction(bestMatchEntry.getValue().getParamTypeList())
        && (unresolvedSignature.getParamTypeList().isEmpty()
            || unresolvedSignature.getParamTypeList().size() > 9)) {
      throw new ExpressionEvaluationException(
          String.format(
              "%s function expected 1-9 arguments, but got %d",
              functionName, unresolvedSignature.getParamTypeList().size()));
    }
    if (FunctionSignature.NOT_MATCH.equals(bestMatchEntry.getKey())
        && !FunctionSignature.isVarArgFunction(bestMatchEntry.getValue().getParamTypeList())) {
      throw new ExpressionEvaluationException(
          String.format(
              "%s function expected %s, but got %s",
              functionName,
              formatFunctions(functionBundle.keySet()),
              unresolvedSignature.formatTypes()));
    } else {
      FunctionSignature resolvedSignature = bestMatchEntry.getValue();
      return Pair.of(resolvedSignature, functionBundle.get(resolvedSignature));
    }
  }

  private String formatFunctions(Set<FunctionSignature> functionSignatures) {
    return functionSignatures.stream()
        .map(FunctionSignature::formatTypes)
        .collect(Collectors.joining(",", "{", "}"));
  }
}
