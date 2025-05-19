/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.adaptExprMethodToUDF;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.adaptExprMethodWithPropertiesToUDF;

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
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.util.BuiltInMethod;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.jsonUDF.JsonAppendFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonArrayLengthFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonDeleteFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonExtendFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonExtractFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonKeysFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonSetFunctionImpl;
import org.opensearch.sql.expression.function.udf.CryptographicFunction;
import org.opensearch.sql.expression.function.udf.datetime.AddSubDateFunction;
import org.opensearch.sql.expression.function.udf.datetime.CurrentFunction;
import org.opensearch.sql.expression.function.udf.datetime.DateAddSubFunction;
import org.opensearch.sql.expression.function.udf.datetime.DatePartFunction;
import org.opensearch.sql.expression.function.udf.datetime.DatetimeFunction;
import org.opensearch.sql.expression.function.udf.datetime.ExtractFunction;
import org.opensearch.sql.expression.function.udf.datetime.FormatFunction;
import org.opensearch.sql.expression.function.udf.datetime.FromUnixTimeFunction;
import org.opensearch.sql.expression.function.udf.datetime.LastDayFunction;
import org.opensearch.sql.expression.function.udf.datetime.PeriodNameFunction;
import org.opensearch.sql.expression.function.udf.datetime.SecToTimeFunction;
import org.opensearch.sql.expression.function.udf.datetime.SysdateFunction;
import org.opensearch.sql.expression.function.udf.datetime.TimestampAddFunction;
import org.opensearch.sql.expression.function.udf.datetime.TimestampDiffFunction;
import org.opensearch.sql.expression.function.udf.datetime.TimestampFunction;
import org.opensearch.sql.expression.function.udf.datetime.ToSecondsFunction;
import org.opensearch.sql.expression.function.udf.datetime.UnixTimestampFunction;
import org.opensearch.sql.expression.function.udf.datetime.WeekFunction;
import org.opensearch.sql.expression.function.udf.datetime.WeekdayFunction;
import org.opensearch.sql.expression.function.udf.datetime.YearweekFunction;
import org.opensearch.sql.expression.function.udf.math.CRC32Function;
import org.opensearch.sql.expression.function.udf.math.ConvFunction;
import org.opensearch.sql.expression.function.udf.math.DivideFunction;
import org.opensearch.sql.expression.function.udf.math.EulerFunction;
import org.opensearch.sql.expression.function.udf.math.ModFunction;

/** Defines functions and operators that are implemented only by PPL */
public class PPLBuiltinOperators extends ReflectiveSqlOperatorTable {

  // Math functions
  public static final SqlOperator SPAN = new SpanFunctionImpl().toUDF("SPAN");
  public static final SqlOperator JSON = new JsonFunctionImpl().toUDF("JSON");
  public static final SqlOperator JSON_ARRAY_LENGTH =
      new JsonArrayLengthFunctionImpl().toUDF("JSON_ARRAY_LENGTH");
  public static final SqlOperator JSON_EXTRACT =
      new JsonExtractFunctionImpl().toUDF("JSON_EXTRACT");
  public static final SqlOperator JSON_KEYS = new JsonKeysFunctionImpl().toUDF("JSON_KEYS");
  public static final SqlOperator JSON_SET = new JsonSetFunctionImpl().toUDF("JSON_SET");
  public static final SqlOperator JSON_DELETE = new JsonDeleteFunctionImpl().toUDF("JSON_DELETE");
  public static final SqlOperator JSON_APPEND = new JsonAppendFunctionImpl().toUDF("JSON_APPEND");
  public static final SqlOperator JSON_EXTEND = new JsonExtendFunctionImpl().toUDF("JSON_EXTEND");

  public static final SqlOperator E = new EulerFunction().toUDF("E");
  public static final SqlOperator CONV = new ConvFunction().toUDF("CONVERT");
  public static final SqlOperator MOD = new ModFunction().toUDF("MOD");
  public static final SqlOperator CRC32 = new CRC32Function().toUDF("CRC32");
  public static final SqlOperator DIVIDE = new DivideFunction().toUDF("DIVIDE");
  public static final SqlOperator SHA2 = CryptographicFunction.sha2().toUDF("SHA2");

  // Datetime function
  public static final SqlOperator TIMESTAMP = new TimestampFunction().toUDF("TIMESTAMP");
  public static final SqlOperator DATE =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprDate",
              PPLReturnTypes.DATE_FORCE_NULLABLE,
              NullPolicy.ARG0)
          .toUDF("DATE");
  public static final SqlOperator YEARWEEK = new YearweekFunction().toUDF("YEARWEEK");
  public static final SqlOperator WEEKDAY = new WeekdayFunction().toUDF("WEEKDAY");
  public static final SqlOperator UNIX_TIMESTAMP =
      new UnixTimestampFunction().toUDF("UNIX_TIMESTAMP");
  public static final SqlOperator TO_SECONDS = new ToSecondsFunction().toUDF("TO_SECONDS");
  public static final SqlOperator ADDTIME =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprAddTime",
              PPLReturnTypes.TIME_APPLY_RETURN_TYPE,
              NullPolicy.ANY)
          .toUDF("ADDTIME");
  public static final SqlOperator SUBTIME =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprSubTime",
              PPLReturnTypes.TIME_APPLY_RETURN_TYPE,
              NullPolicy.ANY)
          .toUDF("SUBTIME");
  public static final SqlOperator ADDDATE = new AddSubDateFunction(true).toUDF("ADDDATE");
  public static final SqlOperator SUBDATE = new AddSubDateFunction(false).toUDF("SUBDATE");
  public static final SqlOperator DATE_ADD = new DateAddSubFunction(true).toUDF("DATE_ADD");
  public static final SqlOperator DATE_SUB = new DateAddSubFunction(false).toUDF("DATE_SUB");
  public static final SqlOperator EXTRACT = new ExtractFunction().toUDF("EXTRACT");
  public static final SqlOperator YEAR = new DatePartFunction(TimeUnit.YEAR).toUDF("YEAR");
  public static final SqlOperator QUARTER = new DatePartFunction(TimeUnit.QUARTER).toUDF("QUARTER");
  public static final SqlOperator MONTH = new DatePartFunction(TimeUnit.MONTH).toUDF("MONTH");
  public static final SqlOperator DAY = new DatePartFunction(TimeUnit.DAY).toUDF("DAY");
  public static final SqlOperator DAY_OF_WEEK =
      new DatePartFunction(TimeUnit.DOW).toUDF("DAY_OF_WEEK");
  public static final SqlOperator DAY_OF_YEAR =
      new DatePartFunction(TimeUnit.DOY).toUDF("DAY_OF_YEAR");
  public static final SqlOperator HOUR = new DatePartFunction(TimeUnit.HOUR).toUDF("HOUR");
  public static final SqlOperator MINUTE = new DatePartFunction(TimeUnit.MINUTE).toUDF("MINUTE");
  public static final SqlOperator MINUTE_OF_DAY =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprMinuteOfDay",
              PPLReturnTypes.INTEGER_FORCE_NULLABLE,
              NullPolicy.ARG0)
          .toUDF("MINUTE_OF_DAY");
  public static final SqlOperator SECOND = new DatePartFunction(TimeUnit.SECOND).toUDF("SECOND");
  public static final SqlOperator MICROSECOND =
      new DatePartFunction(TimeUnit.MICROSECOND).toUDF("MICROSECOND");
  public static final SqlOperator NOW = new CurrentFunction(ExprCoreType.TIMESTAMP).toUDF("NOW");
  public static final SqlOperator CURRENT_TIME =
      new CurrentFunction(ExprCoreType.TIME).toUDF("CURRENT_TIME");
  public static final SqlOperator CURRENT_DATE =
      new CurrentFunction(ExprCoreType.DATE).toUDF("CURRENT_DATE");
  public static final SqlOperator DATE_FORMAT =
      new FormatFunction(ExprCoreType.DATE).toUDF("DATE_FORMAT");
  public static final SqlOperator TIME_FORMAT =
      new FormatFunction(ExprCoreType.TIME).toUDF("TIME_FORMAT");
  public static final SqlOperator DAYNAME = new PeriodNameFunction(TimeUnit.DAY).toUDF("DAYNAME");
  public static final SqlOperator MONTHNAME =
      new PeriodNameFunction(TimeUnit.MONTH).toUDF("MONTHNAME");
  public static final SqlOperator CONVERT_TZ =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprConvertTZ",
              PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE,
              NullPolicy.ANY)
          .toUDF("CONVERT_TZ");
  public static final SqlOperator DATEDIFF =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprDateDiff",
              ReturnTypes.BIGINT_FORCE_NULLABLE,
              NullPolicy.ANY)
          .toUDF("DATEDIFF");
  public static final SqlOperator TIMESTAMPDIFF =
      new TimestampDiffFunction().toUDF("TIMESTAMPDIFF");
  public static final SqlOperator LAST_DAY = new LastDayFunction().toUDF("LAST_DAY");
  public static final SqlOperator FROM_DAYS =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprFromDays",
              PPLReturnTypes.DATE_FORCE_NULLABLE,
              NullPolicy.ANY)
          .toUDF("FROM_DAYS");
  public static final SqlOperator FROM_UNIXTIME = new FromUnixTimeFunction().toUDF("FROM_UNIXTIME");
  public static final SqlOperator GET_FORMAT =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprGetFormat",
              ReturnTypes.VARCHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE),
              NullPolicy.ANY)
          .toUDF("GET_FORMAT");
  public static final SqlOperator MAKEDATE =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprMakeDate",
              PPLReturnTypes.DATE_FORCE_NULLABLE,
              NullPolicy.ANY)
          .toUDF("MAKEDATE");
  public static final SqlOperator MAKETIME =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprMakeTime",
              PPLReturnTypes.TIME_FORCE_NULLABLE,
              NullPolicy.ANY)
          .toUDF("MAKETIME");
  public static final SqlOperator PERIOD_DIFF =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprPeriodDiff",
              PPLReturnTypes.INTEGER_FORCE_NULLABLE,
              NullPolicy.ANY)
          .toUDF("PERIOD_DIFF");
  public static final SqlOperator PERIOD_ADD =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprPeriodAdd",
              PPLReturnTypes.INTEGER_FORCE_NULLABLE,
              NullPolicy.ANY)
          .toUDF("PERIOD_ADD");
  public static final SqlOperator STR_TO_DATE =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprStrToDate",
              PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE,
              NullPolicy.ANY)
          .toUDF("STR_TO_DATE");
  public static final SqlOperator SYSDATE = new SysdateFunction().toUDF("SYSDATE");
  public static final SqlOperator SEC_TO_TIME = new SecToTimeFunction().toUDF("SEC_TO_TIME");
  public static final SqlOperator TIME =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprTime",
              PPLReturnTypes.TIME_FORCE_NULLABLE,
              NullPolicy.ARG0)
          .toUDF("TIME");
  public static final SqlOperator TIME_TO_SEC =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprTimeToSec",
              ReturnTypes.BIGINT_FORCE_NULLABLE,
              NullPolicy.ARG0)
          .toUDF("TIME_TO_SEC");
  public static final SqlOperator TIMEDIFF =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprTimeDiff",
              PPLReturnTypes.TIME_FORCE_NULLABLE,
              NullPolicy.ANY)
          .toUDF("TIME_DIFF");
  public static final SqlOperator TIMESTAMPADD = new TimestampAddFunction().toUDF("TIMESTAMPADD");
  public static final SqlOperator TO_DAYS =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprToDays",
              ReturnTypes.BIGINT_FORCE_NULLABLE,
              NullPolicy.ARG0)
          .toUDF("TO_DAYS");
  public static final SqlOperator DATETIME = new DatetimeFunction().toUDF("DATETIME");
  public static final SqlOperator UTC_DATE =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprUtcDate",
              PPLReturnTypes.DATE_FORCE_NULLABLE,
              NullPolicy.NONE)
          .toUDF("UTC_DATE");
  public static final SqlOperator UTC_TIME =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprUtcTime",
              PPLReturnTypes.TIME_FORCE_NULLABLE,
              NullPolicy.NONE)
          .toUDF("UTC_TIME");
  public static final SqlOperator UTC_TIMESTAMP =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprUtcTimestamp",
              PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE,
              NullPolicy.NONE)
          .toUDF("UTC_TIMESTAMP");
  public static final SqlOperator WEEK = new WeekFunction().toUDF("WEEK");

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
