/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

/**
 * Safety net for #5660: every registered function that compiles with INTEGER arguments must also
 * compile when those arguments are BIGINT.
 *
 * <p>PPL widens integer arithmetic to BIGINT (#5603), so {@code f(x, 1 + 1)} passes a BIGINT where
 * the signature declares INTEGER. If the runtime method takes a Java {@code int}, code generation
 * fails unless the argument is narrowed. This test walks the whole function registry, so new
 * functions are covered automatically: for each signature with an INTEGER position it compiles the
 * resolved call with INTEGER operands (as a baseline) and again with BIGINT operands, and fails if
 * only the BIGINT version cannot be compiled. Signatures that cannot be compiled in isolation even
 * with INTEGER operands are skipped.
 */
class BigintArgumentCodegenTest {

  private final RexBuilder builder = new RexBuilder(TYPE_FACTORY);

  @Test
  void functionsAcceptingIntegerAlsoCompileWithBigint() throws Exception {
    TreeSet<String> failures = new TreeSet<>();
    for (Map.Entry<BuiltinFunctionName, List<Map.Entry<CalciteFuncSignature, ?>>> entry :
        registry().entrySet()) {
      for (Map.Entry<CalciteFuncSignature, ?> implementation : entry.getValue()) {
        PPLTypeChecker typeChecker = implementation.getKey().typeChecker();
        for (List<RelDataType> signature : parameterTypes(typeChecker)) {
          if (signature.stream().noneMatch(t -> t.getSqlTypeName() == SqlTypeName.INTEGER)) {
            continue;
          }
          for (boolean nullable : new boolean[] {false, true}) {
            List<RelDataType> intTypes = withNullability(signature, nullable);
            if (!compiles(entry.getKey(), intTypes)) {
              continue;
            }
            List<RelDataType> bigintTypes = integerToBigint(intTypes);
            String error = compileError(entry.getKey(), bigintTypes);
            if (error != null) {
              failures.add(entry.getKey() + " " + bigintTypes + ": " + error);
            }
          }
        }
      }
    }
    assertTrue(
        failures.isEmpty(),
        "Functions that compile with INTEGER but not BIGINT arguments (#5660): " + failures);
  }

  private boolean compiles(BuiltinFunctionName function, List<RelDataType> types) {
    return compileError(function, types) == null;
  }

  /** Resolves {@code function} over input refs of {@code types} and compiles it. */
  private String compileError(BuiltinFunctionName function, List<RelDataType> types) {
    try {
      List<RexNode> args = new ArrayList<>();
      for (int i = 0; i < types.size(); i++) {
        args.add(builder.makeInputRef(types.get(i), i));
      }
      RexNode call =
          PPLFuncImpTable.INSTANCE.resolve(builder, function, args.toArray(new RexNode[0]));
      RelDataType rowType =
          TYPE_FACTORY.createStructType(
              types, IntStream.range(0, types.size()).mapToObj(i -> "f" + i).toList());
      RexExecutorImpl.getExecutable(builder, List.of(call), rowType);
      return null;
    } catch (Throwable e) {
      String message = String.valueOf(e.getMessage());
      return message.substring(0, Math.min(200, message.length()));
    }
  }

  private static List<List<RelDataType>> parameterTypes(PPLTypeChecker typeChecker) {
    if (typeChecker == null) {
      return List.of();
    }
    try {
      return typeChecker.getParameterTypes();
    } catch (RuntimeException e) {
      return List.of();
    }
  }

  private static List<RelDataType> withNullability(List<RelDataType> types, boolean nullable) {
    return types.stream().map(t -> TYPE_FACTORY.createTypeWithNullability(t, nullable)).toList();
  }

  private static List<RelDataType> integerToBigint(List<RelDataType> types) {
    return types.stream()
        .map(
            t ->
                t.getSqlTypeName() == SqlTypeName.INTEGER
                    ? TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT, t.isNullable())
                    : t)
        .toList();
  }

  @SuppressWarnings("unchecked")
  private static Map<BuiltinFunctionName, List<Map.Entry<CalciteFuncSignature, ?>>> registry()
      throws ReflectiveOperationException {
    Field field = PPLFuncImpTable.class.getDeclaredField("functionRegistry");
    field.setAccessible(true);
    return (Map<BuiltinFunctionName, List<Map.Entry<CalciteFuncSignature, ?>>>)
        field.get(PPLFuncImpTable.INSTANCE);
  }
}
