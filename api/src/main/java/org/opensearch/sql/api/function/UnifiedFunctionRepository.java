/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function;

import java.util.ArrayList;
import java.util.List;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.function.calcite.CalciteTypeConverter;
import org.opensearch.sql.api.function.calcite.UnifiedFunctionCalciteAdapter;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/**
 * Repository for discovering and loading PPL functions as {@link UnifiedFunction} instances.
 *
 * <p>Inspects {@link PPLBuiltinOperators} and creates function descriptors for external execution
 * engines (Spark, Flink, etc.). Engines use the builder to construct functions with specific input
 * types, then convert to engine-specific representations.
 *
 * <p>Example for Spark integration:
 *
 * <pre>{@code
 * UnifiedFunctionRepository repository = new UnifiedFunctionRepository(context);
 * List<FunctionDescriptor> functions = repository.loadFunctions();
 * for (FunctionDescriptor desc : functions) {
 *   // Build function with specific input types
 *   UnifiedFunction func = desc.getBuilder().build(List.of("VARCHAR"));
 *
 *   // Convert to Spark-specific types
 *   FunctionIdentifier id = new FunctionIdentifier(desc.getFunctionName());
 *   ExpressionInfo info = new ExpressionInfo(
 *     func.getClass().getName(),
 *     desc.getFunctionName(),
 *     "usage info"
 *   );
 *   FunctionBuilder sparkBuilder = (exprs) -> convertToSparkExpression(func, exprs);
 *
 *   // Register with Spark
 *   sparkSession.sessionState.functionRegistry.registerFunction(id, info, sparkBuilder);
 * }
 * }</pre>
 */
@Slf4j
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

  /**
   * Function descriptor with name and builder for creating {@link UnifiedFunction} instances.
   *
   * <p>Execution engines use the function name and builder to construct engine-specific
   * representations (e.g., Spark's FunctionIdentifier, ExpressionInfo, FunctionBuilder).
   */
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
          descriptors.add(createDescriptor(operator));
        }
      }

      log.info("Loaded {} unified functions from PPLBuiltinOperators", descriptors.size());
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
          return createDescriptor(operator);
        }
      }

      throw new IllegalArgumentException("Function not found: " + functionName);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to load function: " + functionName, e);
    }
  }

  private FunctionDescriptor createDescriptor(SqlOperator operator) {
    String functionName = operator.getName();
    UnifiedFunctionBuilder builder = createFunctionBuilder(functionName);

    log.debug("Registered function: {}", functionName);
    return new FunctionDescriptor(functionName, builder);
  }

  private UnifiedFunctionBuilder createFunctionBuilder(String functionName) {
    return inputTypeNames -> {
      List<RexNode> rexNodes = new ArrayList<>();
      for (int i = 0; i < inputTypeNames.size(); i++) {
        var relDataType =
            CalciteTypeConverter.toCalciteType(inputTypeNames.get(i), rexBuilder.getTypeFactory());
        rexNodes.add(rexBuilder.makeInputRef(relDataType, i));
      }

      return UnifiedFunctionCalciteAdapter.create(functionName, rexBuilder, rexNodes);
    };
  }
}
