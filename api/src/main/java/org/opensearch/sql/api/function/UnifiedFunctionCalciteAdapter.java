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

  /** PPL function name (e.g., "UPPER", "CONCAT", "ABS"). */
  @Getter private final String functionName;

  /** SQL type name of the return value (e.g., "VARCHAR", "INTEGER"). */
  @Getter private final String returnType;

  /** SQL type names of the input arguments. */
  @Getter private final List<String> inputTypes;

  /** Compiled Janino code for the function expression. */
  private final String compiledCode;

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
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    RelDataTypeFactory.Builder rowTypeBuilder = typeFactory.builder();
    RexNode[] inputRefs = new RexNode[inputTypeNames.size()];
    for (int i = 0; i < inputTypeNames.size(); i++) {
      SqlTypeName sqlType = SqlTypeName.valueOf(inputTypeNames.get(i));
      RelDataType relType = typeFactory.createSqlType(sqlType);
      rowTypeBuilder.add("_" + i, relType);
      inputRefs[i] = rexBuilder.makeInputRef(relType, i);
    }

    RelDataType inputRowType = rowTypeBuilder.build();
    RexNode resolved = PPLFuncImpTable.INSTANCE.resolve(rexBuilder, functionName, inputRefs);
    RexExecutable executable =
        RexExecutorImpl.getExecutable(rexBuilder, List.of(resolved), inputRowType);
    String returnTypeName = resolved.getType().getSqlTypeName().getName();
    return new UnifiedFunctionCalciteAdapter(
        functionName, returnTypeName, List.copyOf(inputTypeNames), executable.getSource());
  }

  @Override
  public Object eval(List<Object> inputs) {
    RexExecutable rexExecutor = new RexExecutable(compiledCode, functionName);
    DataContext dataContext = DataContexts.of(Map.of(INPUT_RECORD_KEY, inputs.toArray()));
    rexExecutor.setDataContext(dataContext);

    Object[] results = rexExecutor.execute();
    return (results == null || results.length == 0) ? null : results[0];
  }
}
