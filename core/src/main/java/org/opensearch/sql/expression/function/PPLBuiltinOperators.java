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
import org.opensearch.sql.expression.function.CollectionUDF.ArrayFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.ExistsFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.FilterFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.ForallFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.ReduceFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.TransformFunctionImpl;

/** Defines functions and operators that are implemented only by PPL */
public class PPLBuiltinOperators extends ReflectiveSqlOperatorTable {

  public static final SqlOperator SPAN = new SpanFunctionImpl().toUDF("SPAN");

  public static final SqlOperator FORALL = new ForallFunctionImpl().toUDF("forall");
  public static final SqlOperator EXISTS = new ExistsFunctionImpl().toUDF("exists");
  public static final SqlOperator ARRAY = new ArrayFunctionImpl().toUDF("array");
  public static final SqlOperator FILTER = new FilterFunctionImpl().toUDF("filter");
  public static final SqlOperator TRANSFORM = new TransformFunctionImpl().toUDF("transform");
  public static final SqlOperator REDUCE = new ReduceFunctionImpl().toUDF("reduce");

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
