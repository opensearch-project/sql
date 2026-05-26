/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function;

import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
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
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/** Adapter that implements {@link UnifiedFunction} using Calcite's {@link RexExecutorImpl}. */
@ToString
@EqualsAndHashCode(exclude = "compiledCode")
@RequiredArgsConstructor
public class UnifiedFunctionCalciteAdapter implements UnifiedFunction {

  private static final long serialVersionUID = 1L;

  /**
   * Key used by RexExecutorImpl's InputGetter to retrieve input values from DataContext. This is a
   * Calcite internal convention.
   */
  private static final String INPUT_RECORD_KEY = "inputRecord";

  /** Unified function name. */
  @Getter private final String functionName;

  /** Unified type name of the return value. */
  @Getter private final String returnType;

  /** Unified type names of the input arguments. */
  @Getter private final List<String> inputTypes;

  /**
   * Compiled Java source for evaluating the function.
   *
   * <p>The generated code reads inputs from the {@code "inputRecord"} entry in {@link DataContext}.
   * Arguments are mapped to field variables named {@code "_0"}, {@code "_1"}, etc.
   *
   * <pre>{@code
   * // For UPPER(input) function:
   * Object[] inputRecord = (Object[]) dataContext.get("inputRecord");
   * String _0 = (String) inputRecord[0];
   * return _0 == null ? null : _0.toUpperCase();
   * }</pre>
   */
  private final String compiledCode;

  @Override
  public Object eval(List<Object> inputs) {
    RexExecutable rexExecutor = new RexExecutable(compiledCode, functionName);
    DataContext dataContext = DataContexts.of(Map.of(INPUT_RECORD_KEY, inputs.toArray()));
    rexExecutor.setDataContext(dataContext);

    Object[] results = rexExecutor.execute();
    return (results == null || results.length == 0) ? null : results[0];
  }

  /**
   * Creates Calcite RexNode adapter for a unified function.
   *
   * <p>Note: this method pre-compiles the resolved function expression and stores the generated
   * source code as a string. This avoids serializing {@link RexNode} instances and simplifies
   * distribution across execution engines. If performance or security concerns arise, we can change
   * this internal implementation.
   *
   * @param rexBuilder RexBuilder for creating expressions
   * @param functionName function name
   * @param inputTypes function argument types
   * @return configured adapter instance
   */
  public static UnifiedFunctionCalciteAdapter create(
      RexBuilder rexBuilder, String functionName, List<String> inputTypes) {
    RexNode[] inputRefs = makeInputRefs(rexBuilder, inputTypes);
    RexNode resolved = PPLFuncImpTable.INSTANCE.resolve(rexBuilder, functionName, inputRefs);
    RelDataType inputRowType = buildInputRowType(rexBuilder, inputTypes);
    RexExecutable executable =
        RexExecutorImpl.getExecutable(rexBuilder, List.of(resolved), inputRowType);
    String returnType = resolved.getType().getSqlTypeName().getName();

    return new UnifiedFunctionCalciteAdapter(
        functionName, returnType, List.copyOf(inputTypes), executable.getSource());
  }

  private static RelDataType buildInputRowType(RexBuilder rexBuilder, List<String> inputTypes) {
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (int i = 0; i < inputTypes.size(); i++) {
      RelDataType relType = typeFactory.createSqlType(SqlTypeName.valueOf(inputTypes.get(i)));
      builder.add("_" + i, relType);
    }
    return builder.build();
  }

  private static RexNode[] makeInputRefs(RexBuilder rexBuilder, List<String> inputTypes) {
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    RexNode[] inputRefs = new RexNode[inputTypes.size()];
    for (int i = 0; i < inputTypes.size(); i++) {
      RelDataType relType = typeFactory.createSqlType(SqlTypeName.valueOf(inputTypes.get(i)));
      inputRefs[i] = rexBuilder.makeInputRef(relType, i);
    }
    return inputRefs;
  }
}
