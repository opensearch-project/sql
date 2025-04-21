/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexImpTable.RexCallImplementor;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.util.BuiltInMethod;
import org.opensearch.sql.calcite.udf.conditionUDF.IfImpl;
import org.opensearch.sql.calcite.udf.conditionUDF.IfNullImpl;
import org.opensearch.sql.calcite.udf.conditionUDF.NullIfImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.ConvertTZFunctionImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.CurrentFunctionImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.DateAddSubFunctionImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.DatePartFunctionImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.DatetimeFunctionImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.DiffFunctionImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.ExtractFunctionImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.FormatFunctionImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.FromUnixTimeFunctionImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.LastDayFunctionImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.MinuteOfDayFunctionImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.PeriodNameFunctionImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.SecToTimeFunctionImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.SysdateFunctionImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimeAddSubFunctionImpl;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimestampAddFunctionImpl;
import org.opensearch.sql.calcite.udf.mathUDF.CRC32FunctionImpl;
import org.opensearch.sql.calcite.udf.mathUDF.ConvFunctionImpl;
import org.opensearch.sql.calcite.udf.mathUDF.DivideFunctionImpl;
import org.opensearch.sql.calcite.udf.mathUDF.EulerFunctionImpl;
import org.opensearch.sql.calcite.udf.mathUDF.ModFunctionImpl;
import org.opensearch.sql.calcite.udf.mathUDF.SqrtFunctionImpl;
import org.opensearch.sql.calcite.udf.textUDF.LocateFunctionImpl;
import org.opensearch.sql.calcite.udf.textUDF.ReplaceFunctionImpl;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.datetimeUDF.DateImpl;
import org.opensearch.sql.expression.function.datetimeUDF.TimeImpl;
import org.opensearch.sql.expression.function.datetimeUDF.TimestampImpl;
import org.opensearch.sql.expression.function.datetimeUDF.TosecondsImpl;
import org.opensearch.sql.expression.function.datetimeUDF.UTCDateImpl;
import org.opensearch.sql.expression.function.datetimeUDF.UTCTimeImpl;
import org.opensearch.sql.expression.function.datetimeUDF.UTCTimestampImpl;
import org.opensearch.sql.expression.function.datetimeUDF.UnixTimestampImpl;
import org.opensearch.sql.expression.function.datetimeUDF.WeekImpl;
import org.opensearch.sql.expression.function.datetimeUDF.WeekdayImpl;
import org.opensearch.sql.expression.function.datetimeUDF.YearImpl;
import org.opensearch.sql.expression.function.datetimeUDF.YearweekImpl;

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

  // Datetime function
  public static final SqlOperator TIMESTAMP = new TimestampImpl().toUDF("TIMESTAMP");
  public static final SqlOperator UTC_TIME = new UTCTimeImpl().toUDF("UTC_TIME");
  public static final SqlOperator UTC_TIMESTAMP = new UTCTimestampImpl().toUDF("UTC_TIMESTAMP");
//  public static final SqlOperator UTC_DATE = new UTCDateImpl().toUDF("UTC_DATE");
  public static final SqlOperator DATE = new DateImpl().toUDF("DATE");
//  public static final SqlOperator TIME = new TimeImpl().toUDF("TIME");
  public static final SqlOperator YEAR = new YearImpl().toUDF("YEAR");
  public static final SqlOperator YEARWEEK = new YearweekImpl().toUDF("YEARWEEK");
  public static final SqlOperator WEEKDAY = new WeekdayImpl().toUDF("WEEKDAY");
  public static final SqlOperator UNIX_TIMESTAMP = new UnixTimestampImpl().toUDF("UNIX_TIMESTAMP");
  public static final SqlOperator TO_SECONDS = new TosecondsImpl().toUDF("TO_SECONDS");
//  public static final SqlOperator TO_DAYS = new TodaysImpl().toUDF("TO_DAYS");

  public static final SqlOperator ADDTIME = new TimeAddSubFunctionImpl(true).toUDF("ADDTIME");
  public static final SqlOperator SUBTIME = new TimeAddSubFunctionImpl(false).toUDF("SUBTIME");
  public static final SqlOperator ADDDATE =
      new DateAddSubFunctionImpl(true, false).toUDF("ADDDATE");
  public static final SqlOperator SUBDATE =
      new DateAddSubFunctionImpl(false, false).toUDF("SUBDATE");
  public static final SqlOperator DATE_ADD =
      new DateAddSubFunctionImpl(true, true).toUDF("DATE_ADD");
  public static final SqlOperator DATE_SUB =
      new DateAddSubFunctionImpl(false, true).toUDF("DATE_SUB");
  public static final SqlOperator EXTRACT = new ExtractFunctionImpl().toUDF("EXTRACT");
  public static final SqlOperator QUARTER =
      new DatePartFunctionImpl(TimeUnit.QUARTER).toUDF("QUARTER");
  public static final SqlOperator MONTH = new DatePartFunctionImpl(TimeUnit.MONTH).toUDF("MONTH");
  public static final SqlOperator MONTH_OF_YEAR =
      new DatePartFunctionImpl(TimeUnit.MONTH).toUDF("MONTH_OF_YEAR");
  public static final SqlOperator DAY = new DatePartFunctionImpl(TimeUnit.DAY).toUDF("DAY");
  public static final SqlOperator DAYOFMONTH =
      new DatePartFunctionImpl(TimeUnit.DAY).toUDF("DAYOFMONTH");
  public static final SqlOperator DAY_OF_MONTH =
      new DatePartFunctionImpl(TimeUnit.DAY).toUDF("DAY_OF_MONTH");
  public static final SqlOperator DAYOFWEEK =
      new DatePartFunctionImpl(TimeUnit.DOW).toUDF("DAYOFWEEK");
  public static final SqlOperator DAY_OF_WEEK =
      new DatePartFunctionImpl(TimeUnit.DOW).toUDF("DAY_OF_WEEK");
  public static final SqlOperator DAYOFYEAR =
      new DatePartFunctionImpl(TimeUnit.DOY).toUDF("DAYOFYEAR");
  public static final SqlOperator DAY_OF_YEAR =
      new DatePartFunctionImpl(TimeUnit.DOY).toUDF("DAY_OF_YEAR");
  public static final SqlOperator HOUR = new DatePartFunctionImpl(TimeUnit.HOUR).toUDF("HOUR");
  public static final SqlOperator HOUR_OF_DAY =
      new DatePartFunctionImpl(TimeUnit.HOUR).toUDF("HOUR_OF_DAY");
  public static final SqlOperator MINUTE =
      new DatePartFunctionImpl(TimeUnit.MINUTE).toUDF("MINUTE");
  public static final SqlOperator MINUTE_OF_HOUR =
      new DatePartFunctionImpl(TimeUnit.MINUTE).toUDF("MINUTE_OF_HOUR");
  public static final SqlOperator MINUTE_OF_DAY =
      new MinuteOfDayFunctionImpl().toUDF("MINUTE_OF_DAY");
  public static final SqlOperator SECOND =
      new DatePartFunctionImpl(TimeUnit.SECOND).toUDF("SECOND");
  public static final SqlOperator SECOND_OF_MINUTE =
      new DatePartFunctionImpl(TimeUnit.SECOND).toUDF("SECOND_OF_MINUTE");
  public static final SqlOperator MICROSECOND =
      new DatePartFunctionImpl(TimeUnit.MICROSECOND).toUDF("MICROSECOND");
  public static final SqlOperator CURRENT_TIMESTAMP =
      new CurrentFunctionImpl(SqlTypeName.TIMESTAMP).toUDF("CURRENT_TIMESTAMP");
  public static final SqlOperator NOW = new CurrentFunctionImpl(SqlTypeName.TIMESTAMP).toUDF("NOW");
  public static final SqlOperator LOCALTIMESTAMP =
      new CurrentFunctionImpl(SqlTypeName.TIMESTAMP).toUDF("LOCALTIMESTAMP");
  public static final SqlOperator LOCALTIME =
      new CurrentFunctionImpl(SqlTypeName.TIMESTAMP).toUDF("LOCALTIME");
  public static final SqlOperator CURTIME =
      new CurrentFunctionImpl(SqlTypeName.TIME).toUDF("CURTIME");
  public static final SqlOperator CURRENT_TIME =
      new CurrentFunctionImpl(SqlTypeName.TIME).toUDF("CURRENT_TIME");
  public static final SqlOperator CURRENT_DATE =
      new CurrentFunctionImpl(SqlTypeName.DATE).toUDF("CURRENT_DATE");
  public static final SqlOperator CURDATE =
      new CurrentFunctionImpl(SqlTypeName.DATE).toUDF("CURDATE");
  public static final SqlOperator DATE_FORMAT =
      new FormatFunctionImpl(SqlTypeName.DATE).toUDF("DATE_FORMAT");
  public static final SqlOperator TIME_FORMAT =
      new FormatFunctionImpl(SqlTypeName.TIME).toUDF("TIME_FORMAT");
  public static final SqlOperator DAYNAME =
      new PeriodNameFunctionImpl(TimeUnit.DAY).toUDF("DAYNAME");
  public static final SqlOperator MONTHNAME =
      new PeriodNameFunctionImpl(TimeUnit.MONTH).toUDF("MONTHNAME");
  public static final SqlOperator CONVERT_TZ = new ConvertTZFunctionImpl().toUDF("CONVERT_TZ");
  public static final SqlOperator DATEDIFF = DiffFunctionImpl.datediff().toUDF("DATEDIFF");
  public static final SqlOperator TIMESTAMPDIFF =
      DiffFunctionImpl.timestampdiff().toUDF("TIMESTAMPDIFF");
  public static final SqlOperator LAST_DAY = new LastDayFunctionImpl().toUDF("LAST_DAY");
  public static final SqlOperator FROM_DAYS =
      UserDefinedFunctionUtils.adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprFromDays",
              op -> UserDefinedFunctionUtils.nullableDateUDT,
              NullPolicy.ANY)
          .toUDF("FROM_DAYS");
  public static final SqlOperator FROM_UNIXTIME =
      new FromUnixTimeFunctionImpl().toUDF("FROM_UNIXTIME");
  public static final SqlOperator GET_FORMAT =
      UserDefinedFunctionUtils.adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprGetFormat",
              ReturnTypes.VARCHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE),
              NullPolicy.ANY)
          .toUDF("GET_FORMAT");
  public static final SqlOperator MAKEDATE =
      UserDefinedFunctionUtils.adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprMakeDate",
              op -> UserDefinedFunctionUtils.nullableDateUDT,
              NullPolicy.ANY)
          .toUDF("MAKEDATE");
  public static final SqlOperator MAKETIME =
      UserDefinedFunctionUtils.adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprMakeTime",
              op -> UserDefinedFunctionUtils.nullableTimeUDT,
              NullPolicy.ANY)
          .toUDF("MAKETIME");
  public static final SqlOperator PERIOD_DIFF =
      UserDefinedFunctionUtils.adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprPeriodDiff",
              ReturnTypes.INTEGER.andThen(SqlTypeTransforms.FORCE_NULLABLE),
              NullPolicy.ANY)
          .toUDF("PERIOD_DIFF");
  public static final SqlOperator PERIOD_ADD =
      UserDefinedFunctionUtils.adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprPeriodAdd",
              ReturnTypes.INTEGER.andThen(SqlTypeTransforms.FORCE_NULLABLE),
              NullPolicy.ANY)
          .toUDF("PERIOD_ADD");
  public static final SqlOperator STR_TO_DATE =
      UserDefinedFunctionUtils.adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprStrToDate",
              op -> UserDefinedFunctionUtils.nullableTimestampUDT,
              NullPolicy.ANY)
          .toUDF("STR_TO_DATE");
  public static final SqlOperator SYSDATE = new SysdateFunctionImpl().toUDF("SYSDATE");
  public static final SqlOperator SEC_TO_TIME = new SecToTimeFunctionImpl().toUDF("SEC_TO_TIME");
  public static final SqlOperator TIME = UserDefinedFunctionUtils.adaptExprMethodToUDF(DateTimeFunctions.class, "exprTime", op -> UserDefinedFunctionUtils.nullableTimeUDT, NullPolicy.ARG0).toUDF("TIME");
  public static final SqlOperator TIME_TO_SEC = UserDefinedFunctionUtils.adaptExprMethodToUDF(DateTimeFunctions.class, "exprTimeToSec", ReturnTypes.BIGINT_FORCE_NULLABLE, NullPolicy.ARG0).toUDF("TIME_TO_SEC");
  public static final SqlOperator TIMEDIFF = UserDefinedFunctionUtils.adaptExprMethodToUDF(DateTimeFunctions.class, "exprTimeDiff", opBinding -> UserDefinedFunctionUtils.nullableTimeUDT, NullPolicy.ANY).toUDF("TIME_DIFF");
  public static final SqlOperator TIMESTAMPADD = new TimestampAddFunctionImpl().toUDF("TIMESTAMPADD");
  public static final SqlOperator TO_DAYS = UserDefinedFunctionUtils.adaptExprMethodToUDF(DateTimeFunctions.class, "exprToDays", ReturnTypes.BIGINT_FORCE_NULLABLE, NullPolicy.ARG0).toUDF("TO_DAYS");
  public static final SqlOperator DATETIME = new DatetimeFunctionImpl().toUDF("DATETIME");
  public static final SqlOperator UTC_DATE = UserDefinedFunctionUtils.adaptExprMethodWithPropertiesToUDF(DateTimeFunctions.class, "exprUtcDate", op -> UserDefinedFunctionUtils.nullableDateUDT, NullPolicy.NONE).toUDF("UTC_DATE");

  public static final SqlOperator WEEK = new WeekImpl().toUDF("WEEK");
  public static final SqlOperator WEEK_OF_YEAR = new WeekImpl().toUDF("WEEK_OF_YEAR");
  public static final SqlOperator WEEKOFYEAR = new WeekImpl().toUDF("WEEKOFYEAR");

  public static final SqlOperator IF = new IfImpl().toUDF("IF");
  public static final SqlOperator IFNULL = new IfNullImpl().toUDF("IFNULL");
  public static final SqlOperator NULLIF = new NullIfImpl().toUDF("NULLIF");

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
