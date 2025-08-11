/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.adaptExprMethodToUDF;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.adaptExprMethodWithPropertiesToUDF;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.adaptMathFunctionToUDF;

import com.google.common.base.Suppliers;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Supplier;
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
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.CollectionUDF.ArrayFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.ExistsFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.FilterFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.ForallFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.ReduceFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.TransformFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonAppendFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonArrayLengthFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonDeleteFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonExtendFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonExtractFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonKeysFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonSetFunctionImpl;
import org.opensearch.sql.expression.function.udf.CryptographicFunction;
import org.opensearch.sql.expression.function.udf.GrokFunction;
import org.opensearch.sql.expression.function.udf.RelevanceQueryFunction;
import org.opensearch.sql.expression.function.udf.SpanFunction;
import org.opensearch.sql.expression.function.udf.condition.EarliestFunction;
import org.opensearch.sql.expression.function.udf.condition.LatestFunction;
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
import org.opensearch.sql.expression.function.udf.ip.CidrMatchFunction;
import org.opensearch.sql.expression.function.udf.ip.CompareIpFunction;
import org.opensearch.sql.expression.function.udf.ip.IPFunction;
import org.opensearch.sql.expression.function.udf.math.CRC32Function;
import org.opensearch.sql.expression.function.udf.math.ConvFunction;
import org.opensearch.sql.expression.function.udf.math.DivideFunction;
import org.opensearch.sql.expression.function.udf.math.EulerFunction;
import org.opensearch.sql.expression.function.udf.math.ModFunction;

/** Defines functions and operators that are implemented only by PPL */
public class PPLBuiltinOperators extends ReflectiveSqlOperatorTable {

  private static final Supplier<PPLBuiltinOperators> INSTANCE =
      Suppliers.memoize(() -> (PPLBuiltinOperators) new PPLBuiltinOperators().init());

  // Json Functions
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

  // Math functions
  public static final SqlOperator SPAN = new SpanFunction().toUDF("SPAN");
  public static final SqlOperator E = new EulerFunction().toUDF("E");
  public static final SqlOperator CONV = new ConvFunction().toUDF("CONVERT");
  public static final SqlOperator MOD = new ModFunction().toUDF("MOD");
  public static final SqlOperator CRC32 = new CRC32Function().toUDF("CRC32");
  public static final SqlOperator DIVIDE = new DivideFunction().toUDF("DIVIDE");
  public static final SqlOperator SHA2 = CryptographicFunction.sha2().toUDF("SHA2");
  public static final SqlOperator CIDRMATCH = new CidrMatchFunction().toUDF("CIDRMATCH");

  public static final SqlOperator COSH =
      adaptMathFunctionToUDF(
              "cosh", ReturnTypes.DOUBLE_FORCE_NULLABLE, NullPolicy.ANY, PPLOperandTypes.NUMERIC)
          .toUDF("COSH");

  public static final SqlOperator SINH =
      adaptMathFunctionToUDF(
              "sinh", ReturnTypes.DOUBLE_FORCE_NULLABLE, NullPolicy.ANY, PPLOperandTypes.NUMERIC)
          .toUDF("SINH");

  public static final SqlOperator RINT =
      adaptMathFunctionToUDF(
              "rint", ReturnTypes.DOUBLE_FORCE_NULLABLE, NullPolicy.ANY, PPLOperandTypes.NUMERIC)
          .toUDF("RINT");

  public static final SqlOperator EXPM1 =
      adaptMathFunctionToUDF(
              "expm1", ReturnTypes.DOUBLE_FORCE_NULLABLE, NullPolicy.ANY, PPLOperandTypes.NUMERIC)
          .toUDF("EXPM1");

  // IP comparing functions
  public static final SqlOperator NOT_EQUALS_IP =
      CompareIpFunction.notEquals().toUDF("NOT_EQUALS_IP");
  public static final SqlOperator EQUALS_IP = CompareIpFunction.equals().toUDF("EQUALS_IP");
  public static final SqlOperator GREATER_IP = CompareIpFunction.greater().toUDF("GREATER_IP");
  public static final SqlOperator GTE_IP = CompareIpFunction.greaterOrEquals().toUDF("GTE_IP");
  public static final SqlOperator LESS_IP = CompareIpFunction.less().toUDF("LESS_IP");
  public static final SqlOperator LTE_IP = CompareIpFunction.lessOrEquals().toUDF("LTE_IP");

  // Condition function
  public static final SqlOperator EARLIEST = new EarliestFunction().toUDF("EARLIEST");
  public static final SqlOperator LATEST = new LatestFunction().toUDF("LATEST");

  // Datetime function
  public static final SqlOperator TIMESTAMP = new TimestampFunction().toUDF("TIMESTAMP");
  public static final SqlOperator DATE =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprDate",
              PPLReturnTypes.DATE_FORCE_NULLABLE,
              NullPolicy.ARG0,
              PPLOperandTypes.DATE_OR_TIMESTAMP_OR_STRING)
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
              NullPolicy.ANY,
              PPLOperandTypes.DATETIME_DATETIME)
          .toUDF("ADDTIME");
  public static final SqlOperator SUBTIME =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprSubTime",
              PPLReturnTypes.TIME_APPLY_RETURN_TYPE,
              NullPolicy.ANY,
              PPLOperandTypes.DATETIME_DATETIME)
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
              NullPolicy.ARG0,
              PPLOperandTypes.TIME_OR_TIMESTAMP_OR_STRING)
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
              NullPolicy.ANY,
              PPLOperandTypes.TIMESTAMP_OR_STRING_STRING_STRING)
          .toUDF("CONVERT_TZ");
  public static final SqlOperator DATEDIFF =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprDateDiff",
              ReturnTypes.BIGINT_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.DATETIME_DATETIME)
          .toUDF("DATEDIFF");
  public static final SqlOperator TIMESTAMPDIFF =
      new TimestampDiffFunction().toUDF("TIMESTAMPDIFF");
  public static final SqlOperator LAST_DAY = new LastDayFunction().toUDF("LAST_DAY");
  public static final SqlOperator FROM_DAYS =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprFromDays",
              PPLReturnTypes.DATE_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.INTEGER)
          .toUDF("FROM_DAYS");
  public static final SqlOperator FROM_UNIXTIME = new FromUnixTimeFunction().toUDF("FROM_UNIXTIME");
  public static final SqlOperator GET_FORMAT =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprGetFormat",
              ReturnTypes.VARCHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE),
              NullPolicy.ANY,
              PPLOperandTypes.STRING_STRING)
          .toUDF("GET_FORMAT");
  public static final SqlOperator MAKEDATE =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprMakeDate",
              PPLReturnTypes.DATE_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.NUMERIC_NUMERIC)
          .toUDF("MAKEDATE");
  public static final SqlOperator MAKETIME =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprMakeTime",
              PPLReturnTypes.TIME_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.NUMERIC_NUMERIC_NUMERIC)
          .toUDF("MAKETIME");
  public static final SqlOperator PERIOD_DIFF =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprPeriodDiff",
              PPLReturnTypes.INTEGER_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.INTEGER_INTEGER)
          .toUDF("PERIOD_DIFF");
  public static final SqlOperator PERIOD_ADD =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprPeriodAdd",
              PPLReturnTypes.INTEGER_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.INTEGER_INTEGER)
          .toUDF("PERIOD_ADD");
  public static final SqlOperator STR_TO_DATE =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprStrToDate",
              PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.STRING_STRING)
          .toUDF("STR_TO_DATE");
  public static final SqlOperator SYSDATE = new SysdateFunction().toUDF("SYSDATE");
  public static final SqlOperator SEC_TO_TIME = new SecToTimeFunction().toUDF("SEC_TO_TIME");
  public static final SqlOperator TIME =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprTime",
              PPLReturnTypes.TIME_FORCE_NULLABLE,
              NullPolicy.ARG0,
              PPLOperandTypes.DATETIME_OR_STRING)
          .toUDF("TIME");

  // IP cast function
  public static final SqlOperator IP = new IPFunction().toUDF("IP");
  public static final SqlOperator TIME_TO_SEC =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprTimeToSec",
              ReturnTypes.BIGINT_FORCE_NULLABLE,
              NullPolicy.ARG0,
              PPLOperandTypes.TIME_OR_TIMESTAMP_OR_STRING)
          .toUDF("TIME_TO_SEC");
  public static final SqlOperator TIMEDIFF =
      UserDefinedFunctionUtils.adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprTimeDiff",
              PPLReturnTypes.TIME_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.TIME_TIME)
          .toUDF("TIME_DIFF");
  public static final SqlOperator TIMESTAMPADD = new TimestampAddFunction().toUDF("TIMESTAMPADD");
  public static final SqlOperator TO_DAYS =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprToDays",
              ReturnTypes.BIGINT_FORCE_NULLABLE,
              NullPolicy.ARG0,
              PPLOperandTypes.DATE_OR_TIMESTAMP_OR_STRING)
          .toUDF("TO_DAYS");
  public static final SqlOperator DATETIME = new DatetimeFunction().toUDF("DATETIME");
  public static final SqlOperator UTC_DATE =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprUtcDate",
              PPLReturnTypes.DATE_FORCE_NULLABLE,
              NullPolicy.NONE,
              PPLOperandTypes.NONE)
          .toUDF("UTC_DATE");
  public static final SqlOperator UTC_TIME =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprUtcTime",
              PPLReturnTypes.TIME_FORCE_NULLABLE,
              NullPolicy.NONE,
              PPLOperandTypes.NONE)
          .toUDF("UTC_TIME");
  public static final SqlOperator UTC_TIMESTAMP =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprUtcTimestamp",
              PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE,
              NullPolicy.NONE,
              PPLOperandTypes.NONE)
          .toUDF("UTC_TIMESTAMP");
  public static final SqlOperator WEEK = new WeekFunction().toUDF("WEEK");
  public static final SqlOperator GROK = new GrokFunction().toUDF("GROK");
  public static final SqlOperator PATTERN_PARSER =
      new PatternParserFunctionImpl().toUDF("PATTERN_PARSER");

  public static final SqlOperator FORALL = new ForallFunctionImpl().toUDF("forall");
  public static final SqlOperator EXISTS = new ExistsFunctionImpl().toUDF("exists");
  public static final SqlOperator ARRAY = new ArrayFunctionImpl().toUDF("array");
  public static final SqlOperator FILTER = new FilterFunctionImpl().toUDF("filter");
  public static final SqlOperator TRANSFORM = new TransformFunctionImpl().toUDF("transform");
  public static final SqlOperator REDUCE = new ReduceFunctionImpl().toUDF("reduce");

  private static final RelevanceQueryFunction RELEVANCE_QUERY_FUNCTION_INSTANCE =
      new RelevanceQueryFunction();
  public static final SqlOperator MATCH = RELEVANCE_QUERY_FUNCTION_INSTANCE.toUDF("match");
  public static final SqlOperator MATCH_PHRASE =
      RELEVANCE_QUERY_FUNCTION_INSTANCE.toUDF("match_phrase");
  public static final SqlOperator MATCH_BOOL_PREFIX =
      RELEVANCE_QUERY_FUNCTION_INSTANCE.toUDF("match_bool_prefix");
  public static final SqlOperator MATCH_PHRASE_PREFIX =
      RELEVANCE_QUERY_FUNCTION_INSTANCE.toUDF("match_phrase_prefix");
  public static final SqlOperator SIMPLE_QUERY_STRING =
      RELEVANCE_QUERY_FUNCTION_INSTANCE.toUDF("simple_query_string", false);
  public static final SqlOperator QUERY_STRING =
      RELEVANCE_QUERY_FUNCTION_INSTANCE.toUDF("query_string", false);
  public static final SqlOperator MULTI_MATCH =
      RELEVANCE_QUERY_FUNCTION_INSTANCE.toUDF("multi_match", false);

  /**
   * Returns the PPL specific operator table, creating it if necessary.
   *
   * @return PPLBuiltinOperators operator table
   */
  public static PPLBuiltinOperators instance() {
    return INSTANCE.get();
  }

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
