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
import org.opensearch.sql.expression.function.jsonUDF.JsonArrayFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonArrayLengthFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonExtractFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonKeysFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonObjectFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonValidFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.ToJsonStringFunctionImpl;

/** Defines functions and operators that are implemented only by PPL */
public class PPLBuiltinOperators extends ReflectiveSqlOperatorTable {

  public static final SqlOperator SPAN = new SpanFunctionImpl().toUDF("SPAN");
  public static final SqlOperator JSON = new JsonFunctionImpl().toUDF("JSON");
  public static final SqlOperator JSON_OBJECT = new JsonObjectFunctionImpl().toUDF("JSON_OBJECT");
  public static final SqlOperator JSON_ARRAY = new JsonArrayFunctionImpl().toUDF("JSON_ARRAY");
  public static final SqlOperator TO_JSON_STRING = new ToJsonStringFunctionImpl().toUDF("TO_JSON_STRING");
  public static final SqlOperator JSON_ARRAY_LENGTH = new JsonArrayLengthFunctionImpl().toUDF("JSON_ARRAY_LENGTH");
  public static final SqlOperator JSON_EXTRACT = new JsonExtractFunctionImpl().toUDF("JSON_EXTRACT");
  public static final SqlOperator JSON_KEYS = new JsonKeysFunctionImpl().toUDF("JSON_KEYS");
  public static final SqlOperator JSON_VALID = new JsonValidFunctionImpl().toUDF("JSON_VALID");

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
