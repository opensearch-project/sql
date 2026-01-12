/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/** Repository for discovering and loading PPL functions as {@link UnifiedFunction} instances. */
@RequiredArgsConstructor
public class UnifiedFunctionRepository {

  /** Unified query context containing CalcitePlanContext for creating Rex expressions. */
  private final UnifiedQueryContext context;

  /**
   * Loads all PPL functions from {@link PPLBuiltinOperators} as descriptors.
   *
   * @return list of function descriptors
   */
  public List<UnifiedFunctionDescriptor> loadFunctions() {
    RexBuilder rexBuilder = context.getPlanContext().rexBuilder;
    return PPLBuiltinOperators.instance().getOperatorList().stream()
        .filter(SqlUserDefinedFunction.class::isInstance)
        .map(
            operator -> {
              String functionName = operator.getName();
              UnifiedFunctionBuilder builder =
                  inputTypeNames ->
                      UnifiedFunctionCalciteAdapter.create(
                          rexBuilder, functionName, inputTypeNames);
              return new UnifiedFunctionDescriptor(functionName, builder);
            })
        .collect(Collectors.toList());
  }

  /**
   * Loads a specific PPL function by name.
   *
   * @param functionName the name of the function to load (case-insensitive)
   * @return optional function descriptor, empty if not found
   */
  public Optional<UnifiedFunctionDescriptor> loadFunction(String functionName) {
    return loadFunctions().stream()
        .filter(desc -> desc.getFunctionName().equalsIgnoreCase(functionName))
        .findFirst();
  }

  /** Function descriptor with name and builder for creating {@link UnifiedFunction} instances. */
  @Value
  public static class UnifiedFunctionDescriptor {
    /** The name of the function in upper case. */
    String functionName;

    /** Builder for creating {@link UnifiedFunction} instances with specific input types. */
    UnifiedFunctionBuilder builder;
  }

  /** Builder for creating {@link UnifiedFunction} instances with specific input types. */
  @FunctionalInterface
  public interface UnifiedFunctionBuilder extends Serializable {

    /**
     * Builds a {@link UnifiedFunction} instance for the specified input types.
     *
     * @param inputTypes SQL type names for function arguments (e.g., ["VARCHAR", "INTEGER"])
     * @return a UnifiedFunction instance configured for the specified input types
     */
    UnifiedFunction build(List<String> inputTypes);
  }
}
