/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function.calcite;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import org.apache.calcite.DataContext;
import org.apache.calcite.DataContexts;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutable;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.api.function.UnifiedFunction;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/**
 * Adapter that implements {@link UnifiedFunction} using Calcite's {@link RexExecutorImpl}.
 *
 * <p>Compiles and executes RexNode expressions. Handles serialization by storing generated Java
 * code and recreating {@link RexExecutable} on deserialization.
 */
@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class UnifiedFunctionCalciteAdapter implements UnifiedFunction {

  @Serial private static final long serialVersionUID = 1L;

  @EqualsAndHashCode.Include private final String functionName;
  private transient RexExecutable rexExecutor;
  private String serializedCode;
  @EqualsAndHashCode.Include private final String returnTypeName;
  @EqualsAndHashCode.Include private final List<String> inputTypeNames;
  @EqualsAndHashCode.Include private final boolean isNullable;

  private UnifiedFunctionCalciteAdapter(
      @NonNull String functionName,
      @NonNull RexExecutable rexExecutor,
      @NonNull String serializedCode,
      @NonNull String returnTypeName,
      @NonNull List<String> inputTypeNames,
      boolean isNullable) {
    this.functionName = functionName;
    this.rexExecutor = rexExecutor;
    this.serializedCode = serializedCode;
    this.returnTypeName = returnTypeName;
    this.inputTypeNames = inputTypeNames;
    this.isNullable = isNullable;
  }

  /**
   * Creates adapter for a PPL function using Calcite's RexExecutorImpl.
   *
   * @param functionName PPL function name (e.g., "UPPER", "CONCAT", "ABS")
   * @param rexBuilder RexBuilder for creating expressions
   * @param rexNodes function arguments as RexNodes
   * @return configured adapter instance
   */
  public static UnifiedFunctionCalciteAdapter create(
      String functionName, RexBuilder rexBuilder, List<RexNode> rexNodes) {
    Objects.requireNonNull(functionName, "functionName must not be null");
    Objects.requireNonNull(rexBuilder, "rexBuilder must not be null");
    Objects.requireNonNull(rexNodes, "rexNodes must not be null");

    // Resolve the PPL function with actual argument types
    RexNode rexNode =
        PPLFuncImpTable.INSTANCE.resolve(
            rexBuilder, functionName, rexNodes.toArray(new RexNode[0]));

    // Build input row type from the original rexNodes (not the resolved function)
    // This represents the structure of input data that will be provided at evaluation time
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    List<org.apache.calcite.rel.type.RelDataType> inputTypes = new ArrayList<>();
    List<String> inputNames = new ArrayList<>();
    for (int i = 0; i < rexNodes.size(); i++) {
      inputTypes.add(rexNodes.get(i).getType());
      inputNames.add("_" + i);
    }
    org.apache.calcite.rel.type.RelDataType inputRowType =
        typeFactory.createStructType(inputTypes, inputNames);

    // Use Calcite's built-in RexExecutorImpl to compile the expression
    RexExecutable result =
        RexExecutorImpl.getExecutable(rexBuilder, List.of(rexNode), inputRowType);

    // Extract input types from rexNodes and convert to SQL type name strings
    List<String> inputTypeNames = new ArrayList<>();
    for (RexNode node : rexNodes) {
      inputTypeNames.add(CalciteTypeConverter.relDataTypeToSqlTypeName(node.getType()));
    }
    String returnTypeName = CalciteTypeConverter.relDataTypeToSqlTypeName(rexNode.getType());
    boolean isNullable = rexNode.getType().isNullable();

    return new UnifiedFunctionCalciteAdapter(
        functionName, result, result.getSource(), returnTypeName, inputTypeNames, isNullable);
  }

  @Override
  public String getFunctionName() {
    return functionName;
  }

  @Override
  public List<String> getInputTypes() {
    return new ArrayList<>(inputTypeNames);
  }

  @Override
  public String getReturnType() {
    return returnTypeName;
  }

  @Override
  public boolean isNullable() {
    return isNullable;
  }

  @Override
  public Object eval(List<Object> inputs) {
    Objects.requireNonNull(inputs, "inputs must not be null");

    // Validate input count
    if (inputs.size() != inputTypeNames.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Function '%s' expects %d arguments but got %d",
              functionName, inputTypeNames.size(), inputs.size()));
    }

    // Create DataContext with input values as an array
    // RexExecutorImpl's default InputGetter expects "inputRecord" to be an Object[]
    Object[] inputArray = inputs.toArray();
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put("inputRecord", inputArray);

    DataContext dataContext = DataContexts.of(fieldMap);
    rexExecutor.setDataContext(dataContext);

    // Execute the function
    try {
      Object[] results = rexExecutor.execute();
      // The result is an array with one element (the function result)
      return (results == null || results.length == 0) ? null : results[0];
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to evaluate function '%s': %s", functionName, e.getMessage()), e);
    }
  }

  @Serial
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

  @Serial
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.rexExecutor = new RexExecutable(serializedCode, "deserialized Rex code");
  }
}
