/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function.calcite;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.calcite.DataContext;
import org.apache.calcite.DataContexts;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutable;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
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

  private static final long serialVersionUID = 1L;

  /**
   * Key used by RexExecutorImpl's InputGetter to retrieve input values from DataContext. This is a
   * Calcite internal convention.
   */
  private static final String INPUT_RECORD_KEY = "inputRecord";

  @EqualsAndHashCode.Include private final String functionName;
  @EqualsAndHashCode.Include private final String returnTypeName;
  @EqualsAndHashCode.Include private final List<String> inputTypeNames;

  private transient RexExecutable rexExecutor;
  private String serializedCode;

  private UnifiedFunctionCalciteAdapter(
      String functionName,
      RexExecutable rexExecutor,
      String returnTypeName,
      List<String> inputTypeNames) {
    this.functionName = functionName;
    this.rexExecutor = rexExecutor;
    this.returnTypeName = returnTypeName;
    this.inputTypeNames = inputTypeNames;
  }

  /**
   * Creates adapter for a PPL function using Calcite's RexExecutorImpl.
   *
   * @param functionName PPL function name (e.g., "UPPER", "CONCAT", "ABS")
   * @param rexBuilder RexBuilder for creating expressions
   * @param inputTypeNames function argument types as SQL type names (e.g., "VARCHAR", "INTEGER")
   * @return configured adapter instance
   */
  public static UnifiedFunctionCalciteAdapter create(
      String functionName, RexBuilder rexBuilder, List<String> inputTypeNames) {
    Objects.requireNonNull(functionName, "functionName must not be null");
    Objects.requireNonNull(rexBuilder, "rexBuilder must not be null");
    Objects.requireNonNull(inputTypeNames, "inputTypeNames must not be null");

    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

    // Convert input type names to RexNode input references
    List<RexNode> inputRefs = buildInputReferences(inputTypeNames, typeFactory, rexBuilder);

    // Resolve the PPL function with actual argument types
    RexNode resolvedFunction =
        PPLFuncImpTable.INSTANCE.resolve(
            rexBuilder, functionName, inputRefs.toArray(new RexNode[0]));

    // Build input row type for the executor
    RelDataType inputRowType = buildInputRowType(inputRefs, typeFactory);

    // Compile the expression using Calcite's RexExecutorImpl
    RexExecutable executable =
        RexExecutorImpl.getExecutable(rexBuilder, List.of(resolvedFunction), inputRowType);

    String returnTypeName = resolvedFunction.getType().getSqlTypeName().toString();

    return new UnifiedFunctionCalciteAdapter(
        functionName, executable, returnTypeName, inputTypeNames);
  }

  private static List<RexNode> buildInputReferences(
      List<String> inputTypeNames, RelDataTypeFactory typeFactory, RexBuilder rexBuilder) {
    List<RexNode> inputRefs = new ArrayList<>(inputTypeNames.size());
    for (int i = 0; i < inputTypeNames.size(); i++) {
      SqlTypeName sqlTypeName = SqlTypeName.valueOf(inputTypeNames.get(i));
      RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
      inputRefs.add(rexBuilder.makeInputRef(relDataType, i));
    }
    return inputRefs;
  }

  private static RelDataType buildInputRowType(
      List<RexNode> inputRefs, RelDataTypeFactory typeFactory) {
    List<RelDataType> inputTypes = new ArrayList<>(inputRefs.size());
    List<String> inputNames = new ArrayList<>(inputRefs.size());
    for (int i = 0; i < inputRefs.size(); i++) {
      inputTypes.add(inputRefs.get(i).getType());
      inputNames.add("_" + i);
    }
    return typeFactory.createStructType(inputTypes, inputNames);
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
  public Object eval(List<Object> inputs) {
    Objects.requireNonNull(inputs, "inputs must not be null");

    if (inputs.size() != inputTypeNames.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Function '%s' expects %d arguments but got %d",
              functionName, inputTypeNames.size(), inputs.size()));
    }

    // Create DataContext with input values
    // RexExecutorImpl's InputGetter expects the key "inputRecord" with an Object[] value
    DataContext dataContext = DataContexts.of(Map.of(INPUT_RECORD_KEY, inputs.toArray()));
    rexExecutor.setDataContext(dataContext);

    try {
      Object[] results = rexExecutor.execute();
      return (results == null || results.length == 0) ? null : results[0];
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to evaluate function '%s': %s", functionName, e.getMessage()), e);
    }
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    // Serialize the generated code from RexExecutable
    serializedCode = rexExecutor.getSource();
    out.defaultWriteObject();
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.rexExecutor = new RexExecutable(serializedCode, "deserialized Rex code");
  }
}
