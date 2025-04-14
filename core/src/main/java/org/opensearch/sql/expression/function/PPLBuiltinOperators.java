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
import org.opensearch.sql.expression.function.datetimeUDF.DateImpl;
import org.opensearch.sql.expression.function.datetimeUDF.TimeImpl;
import org.opensearch.sql.expression.function.datetimeUDF.TimestampImpl;
import org.opensearch.sql.expression.function.datetimeUDF.TodaysImpl;
import org.opensearch.sql.expression.function.datetimeUDF.TosecondsImpl;
import org.opensearch.sql.expression.function.datetimeUDF.UTCDateImpl;
import org.opensearch.sql.expression.function.datetimeUDF.UTCTimeImpl;
import org.opensearch.sql.expression.function.datetimeUDF.UTCTimestampImpl;
import org.opensearch.sql.expression.function.datetimeUDF.UnixTimestampImpl;
import org.opensearch.sql.expression.function.datetimeUDF.WeekdayImpl;
import org.opensearch.sql.expression.function.datetimeUDF.YearImpl;
import org.opensearch.sql.expression.function.datetimeUDF.YearweekImpl;

/** Defines functions and operators that are implemented only by PPL */
public class PPLBuiltinOperators extends ReflectiveSqlOperatorTable {

  public static final SqlOperator SPAN = new SpanFunctionImpl().toUDF("SPAN");
  public static final SqlOperator TIMESTAMP = new TimestampImpl().toUDF("TIMESTAMP");
  public static final SqlOperator UTC_TIME = new UTCTimeImpl().toUDF("UTC_TIME");
  public static final SqlOperator UTC_TIMESTAMP = new UTCTimestampImpl().toUDF("UTC_TIMESTAMP");
  public static final SqlOperator UTC_DATE = new UTCDateImpl().toUDF("UTC_DATE");
  public static final SqlOperator DATE = new DateImpl().toUDF("DATE");
  public static final SqlOperator TIME = new TimeImpl().toUDF("TIME");
  public static final SqlOperator YEAR = new YearImpl().toUDF("YEAR");
  public static final SqlOperator YEARWEEK = new YearweekImpl().toUDF("YEARWEEK");
  public static final SqlOperator WEEKDAY = new WeekdayImpl().toUDF("WEEKDAY");
  public static final SqlOperator UNIX_TIMESTAMP = new UnixTimestampImpl().toUDF("UNIX_TIMESTAMP");
  public static final SqlOperator TO_SECONDS = new TosecondsImpl().toUDF("TO_SECONDS");
  public static final SqlOperator TO_DAYS = new TodaysImpl().toUDF("TO_DAYS");

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
