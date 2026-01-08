/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.sql.api.function.calcite.CalciteTypeConverter;
import org.opensearch.sql.api.function.calcite.UnifiedFunctionCalciteAdapter;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/**
 * Repository for discovering and loading PPL functions as {@link UnifiedFunction} instances.
 *
 * <p>Inspects {@link PPLBuiltinOperators} and creates function descriptors for external execution
 * engines (Spark, Flink, etc.).
 *
 * <p>Example:
 *
 * <pre>{@code
 * UnifiedQueryContext context = UnifiedQueryContext.builder()
 *     .language(QueryType.PPL)
 *     .catalog("opensearch", schema)
 *     .build();
 * UnifiedFunctionRepository repository = new UnifiedFunctionRepository(context);
 * List<FunctionDescriptor> functions = repository.loadFunctions();
 * for (FunctionDescriptor desc : functions) {
 *   UnifiedFunction func = desc.getBuilder().apply(List.of("VARCHAR"));
 *   externalEngine.registerFunction(desc.getIdentifier(), func);
 * }
 * }</pre>
 */
@Slf4j
public class UnifiedFunctionRepository {

  /** RexBuilder from the unified query context for creating Rex expressions. */
  private final RexBuilder rexBuilder;

  /** Type factory from the unified query context for type conversions. */
  private final RelDataTypeFactory typeFactory;

  /**
   * Constructs a UnifiedFunctionRepository with a unified query context.
   *
   * @param context the unified query context containing CalcitePlanContext
   */
  public UnifiedFunctionRepository(org.opensearch.sql.api.UnifiedQueryContext context) {
    this.rexBuilder = context.getPlanContext().rexBuilder;
    this.typeFactory = rexBuilder.getTypeFactory();
  }

  /**
   * Function descriptor with metadata and builder for creating {@link UnifiedFunction} instances.
   */
  @Value
  public static class FunctionDescriptor {
    @NonNull String identifier;
    @NonNull String expressionInfo;
    @NonNull Function<List<String>, UnifiedFunction> builder;
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
      List<SqlOperator> allOperators = PPLBuiltinOperators.instance().getOperatorList();

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
    String normalizedName = functionName.toLowerCase();

    try {
      List<SqlOperator> allOperators = PPLBuiltinOperators.instance().getOperatorList();

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
    String identifier = functionName.toLowerCase();
    String expressionInfo = buildExpressionInfo(operator, functionName);
    Function<List<String>, UnifiedFunction> builder = createFunctionBuilder(functionName);

    log.debug("Registered function: {}", identifier);
    return new FunctionDescriptor(identifier, expressionInfo, builder);
  }

  private static String buildExpressionInfo(SqlOperator operator, String functionName) {
    String operands =
        operator.getOperandTypeChecker() != null
            ? operator.getOperandTypeChecker().getAllowedSignatures(operator, functionName)
            : "...";
    String returnType = operator.getReturnTypeInference() != null ? "DYNAMIC" : "UNKNOWN";
    return String.format("%s(%s) -> %s", functionName, operands, returnType);
  }

  private Function<List<String>, UnifiedFunction> createFunctionBuilder(String functionName) {
    return inputTypeNames -> {
      List<RexNode> rexNodes = new ArrayList<>();
      for (int i = 0; i < inputTypeNames.size(); i++) {
        var relDataType = CalciteTypeConverter.toCalciteType(inputTypeNames.get(i), typeFactory);
        rexNodes.add(rexBuilder.makeInputRef(relDataType, i));
      }

      return UnifiedFunctionCalciteAdapter.create(functionName, rexBuilder, rexNodes);
    };
  }
}
