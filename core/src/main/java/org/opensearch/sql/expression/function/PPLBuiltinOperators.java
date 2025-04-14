/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexImpTable.RexCallImplementor;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.util.BuiltInMethod;
import org.opensearch.sql.calcite.udf.mathUDF.CRC32FunctionImpl;
import org.opensearch.sql.calcite.udf.mathUDF.ConvFunctionImpl;
import org.opensearch.sql.calcite.udf.mathUDF.DivideFunctionImpl;
import org.opensearch.sql.calcite.udf.mathUDF.EulerFunctionImpl;
import org.opensearch.sql.calcite.udf.mathUDF.ModFunctionImpl;
import org.opensearch.sql.calcite.udf.mathUDF.SqrtFunctionImpl;
import org.opensearch.sql.calcite.udf.textUDF.LocateFunctionImpl;
import org.opensearch.sql.calcite.udf.textUDF.ReplaceFunctionImpl;

/** Defines functions and operators that are implemented only by PPL */
public class PPLBuiltinOperators extends ReflectiveSqlOperatorTable {

  // Math functions
  public static final SqlOperator SPAN = new SpanFunctionImpl().toUDF("SPAN");
  public static final SqlOperator E = new EulerFunctionImpl().toUDF("E");
  public static final SqlOperator SQRT = new SqrtFunctionImpl().toUDF("SQRT");
  public static final SqlOperator CONV = new ConvFunctionImpl().toUDF("CONVERT");
  public static final SqlOperator MOD = new ModFunctionImpl().toUDF("MOD");
  public static final SqlOperator CRC32 = new CRC32FunctionImpl().toUDF("CRC32");
  public static final SqlOperator DIVIDE = new DivideFunctionImpl().toUDF("DIVIDE");

  // Text function
  public static final SqlOperator LOCATE = new LocateFunctionImpl().toUDF("LOCATE");
  public static final SqlOperator REPLACE = new ReplaceFunctionImpl().toUDF("REPLACE");

  /**
   * Invoking an implementor registered in {@link RexImpTable}, need to use reflection since they're
   * all private Use method directly in {@link BuiltInMethod} if possible, most operators'
   * implementor could be substituted by a single method.
   */
  private static Expression invokeCalciteImplementor(
      RexToLixTranslator translator, RexCall call, SqlOperator operator, Expression field)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    RexCallImplementor rexCallImplementor = RexImpTable.INSTANCE.get(operator);
    Method method =
        rexCallImplementor
            .getClass()
            .getDeclaredMethod(
                "implementSafe", RexToLixTranslator.class, RexCall.class, List.class);
    method.setAccessible(true);
    return (Expression) method.invoke(rexCallImplementor, translator, call, List.of(field));
  }
}
