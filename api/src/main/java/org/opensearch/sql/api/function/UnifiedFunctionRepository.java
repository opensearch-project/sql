/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function;

import java.util.ArrayList;
import java.util.List;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.function.calcite.UnifiedFunctionCalciteAdapter;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/**
 * Repository for discovering and loading PPL functions as {@link UnifiedFunction} instances.
 *
 * <p>Inspects {@link PPLBuiltinOperators} and creates function descriptors for external execution
 * engines. Engines use the builder to construct functions with specific input types.
 */
public class UnifiedFunctionRepository {

  /** RexBuilder from the unified query context for creating Rex expressions. */
  private final RexBuilder rexBuilder;

  /** Cached list of all operators from PPLBuiltinOperators. */
  private final List<SqlOperator> allOperators;

  /**
   * Constructs a UnifiedFunctionRepository with a unified query context.
   *
   * @param context the unified query context containing CalcitePlanContext
   */
  public UnifiedFunctionRepository(UnifiedQueryContext context) {
    this.rexBuilder = context.getPlanContext().rexBuilder;
    this.allOperators = PPLBuiltinOperators.instance().getOperatorList();
  }

  /** Function descriptor with name and builder for creating {@link UnifiedFunction} instances. */
  @Value
  public static class FunctionDescriptor {
    @NonNull String functionName;
    @NonNull UnifiedFunctionBuilder builder;
  }

  /**
   * Loads all PPL functions from {@link PPLBuiltinOperators} as descriptors.
   *
   * @return list of function descriptors
   * @throws RuntimeException if function discovery fails
   */
  public List<FunctionDescriptor> loadFunctions() {
    List<FunctionDescriptor> descriptors = new ArrayList<>();

    try {
      for (SqlOperator operator : allOperators) {
        if (operator instanceof SqlUserDefinedFunction) {
          String functionName = operator.getName();
          UnifiedFunctionBuilder builder =
              inputTypeNames ->
                  UnifiedFunctionCalciteAdapter.create(functionName, rexBuilder, inputTypeNames);
          descriptors.add(new FunctionDescriptor(functionName, builder));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to load functions from PPLBuiltinOperators", e);
    }

    return descriptors;
  }

  /**
   * Loads a specific PPL function by name.
   *
   * @param functionName the name of the function to load (case-insensitive)
   * @return function descriptor for the specified function
   * @throws IllegalArgumentException if the function is not found
   */
  public FunctionDescriptor loadFunction(String functionName) {
    try {
      for (SqlOperator operator : allOperators) {
        if (operator instanceof SqlUserDefinedFunction
            && operator.getName().equalsIgnoreCase(functionName)) {
          String name = operator.getName();
          UnifiedFunctionBuilder builder =
              inputTypeNames ->
                  UnifiedFunctionCalciteAdapter.create(name, rexBuilder, inputTypeNames);
          return new FunctionDescriptor(name, builder);
        }
      }

      throw new IllegalArgumentException("Function not found: " + functionName);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to load function: " + functionName, e);
    }
  }
}
