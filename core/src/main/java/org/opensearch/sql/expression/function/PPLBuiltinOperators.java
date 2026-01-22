/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.adaptExprMethodToUDF;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.adaptExprMethodWithPropertiesToUDF;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.adaptMathFunctionToUDF;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.createUserDefinedAggFunction;

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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.BuiltInMethod;
import org.opensearch.sql.calcite.udf.udaf.FirstAggFunction;
import org.opensearch.sql.calcite.udf.udaf.LastAggFunction;
import org.opensearch.sql.calcite.udf.udaf.ListAggFunction;
import org.opensearch.sql.calcite.udf.udaf.LogPatternAggFunction;
import org.opensearch.sql.calcite.udf.udaf.NullableSqlAvgAggFunction;
import org.opensearch.sql.calcite.udf.udaf.PercentileApproxFunction;
import org.opensearch.sql.calcite.udf.udaf.TakeAggFunction;
import org.opensearch.sql.calcite.udf.udaf.ValuesAggFunction;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;
import org.opensearch.sql.expression.function.CollectionUDF.AppendFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.ArrayFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.ExistsFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.FilterFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.ForallFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.MVAppendFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.MVFindFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.MVZipFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.MapAppendFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.MapRemoveFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.ReduceFunctionImpl;
import org.opensearch.sql.expression.function.CollectionUDF.TransformFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonAppendFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonArrayLengthFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonDeleteFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonExtendFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonExtractAllFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonExtractFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonKeysFunctionImpl;
import org.opensearch.sql.expression.function.jsonUDF.JsonSetFunctionImpl;
import org.opensearch.sql.expression.function.udf.CryptographicFunction;
import org.opensearch.sql.expression.function.udf.ParseFunction;
import org.opensearch.sql.expression.function.udf.RelevanceQueryFunction;
import org.opensearch.sql.expression.function.udf.RexExtractFunction;
import org.opensearch.sql.expression.function.udf.RexExtractMultiFunction;
import org.opensearch.sql.expression.function.udf.RexOffsetFunction;
import org.opensearch.sql.expression.function.udf.SpanFunction;
import org.opensearch.sql.expression.function.udf.ToNumberFunction;
import org.opensearch.sql.expression.function.udf.ToStringFunction;
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
import org.opensearch.sql.expression.function.udf.datetime.StrftimeFunction;
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
import org.opensearch.sql.expression.function.udf.math.ConvFunction;
import org.opensearch.sql.expression.function.udf.math.DivideFunction;
import org.opensearch.sql.expression.function.udf.math.EulerFunction;
import org.opensearch.sql.expression.function.udf.math.ModFunction;
import org.opensearch.sql.expression.function.udf.math.NumberToStringFunction;
import org.opensearch.sql.expression.function.udf.math.ScalarMaxFunction;
import org.opensearch.sql.expression.function.udf.math.ScalarMinFunction;

/** Defines functions and operators that are implemented only by PPL */
public class PPLBuiltinOperators extends ReflectiveSqlOperatorTable {

  private static final Supplier<PPLBuiltinOperators> INSTANCE =
      Suppliers.memoize(() -> (PPLBuiltinOperators) new PPLBuiltinOperators().init());

  // Json Functions
  public static final SqlFunction JSON = new JsonFunctionImpl().toUDF("JSON");
  public static final SqlFunction JSON_ARRAY_LENGTH =
      new JsonArrayLengthFunctionImpl().toUDF("JSON_ARRAY_LENGTH");
  public static final SqlFunction JSON_EXTRACT =
      new JsonExtractFunctionImpl().toUDF("JSON_EXTRACT");
  public static final SqlFunction JSON_EXTRACT_ALL =
      new JsonExtractAllFunctionImpl().toUDF("JSON_EXTRACT_ALL");
  public static final SqlFunction JSON_KEYS = new JsonKeysFunctionImpl().toUDF("JSON_KEYS");
  public static final SqlFunction JSON_SET = new JsonSetFunctionImpl().toUDF("JSON_SET");
  public static final SqlFunction JSON_DELETE = new JsonDeleteFunctionImpl().toUDF("JSON_DELETE");
  public static final SqlFunction JSON_APPEND = new JsonAppendFunctionImpl().toUDF("JSON_APPEND");
  public static final SqlFunction JSON_EXTEND = new JsonExtendFunctionImpl().toUDF("JSON_EXTEND");

  // Math functions
  public static final SqlFunction SPAN = new SpanFunction().toUDF("SPAN");
  public static final SqlFunction E = new EulerFunction().toUDF("E");
  public static final SqlFunction CONV = new ConvFunction().toUDF("CONVERT");
  public static final SqlFunction MOD = new ModFunction().toUDF("MOD");
  public static final SqlFunction DIVIDE = new DivideFunction().toUDF("DIVIDE");
  public static final SqlFunction SHA2 = CryptographicFunction.sha2().toUDF("SHA2");
  public static final SqlFunction CIDRMATCH = new CidrMatchFunction().toUDF("CIDRMATCH");
  public static final SqlFunction SCALAR_MAX = new ScalarMaxFunction().toUDF("SCALAR_MAX");
  public static final SqlFunction SCALAR_MIN = new ScalarMinFunction().toUDF("SCALAR_MIN");

  public static final SqlFunction COSH =
      adaptMathFunctionToUDF(
              "cosh", ReturnTypes.DOUBLE_FORCE_NULLABLE, NullPolicy.ANY, PPLOperandTypes.NUMERIC)
          .toUDF("COSH");

  public static final SqlFunction SINH =
      adaptMathFunctionToUDF(
              "sinh", ReturnTypes.DOUBLE_FORCE_NULLABLE, NullPolicy.ANY, PPLOperandTypes.NUMERIC)
          .toUDF("SINH");

  public static final SqlFunction RINT =
      adaptMathFunctionToUDF(
              "rint", ReturnTypes.DOUBLE_FORCE_NULLABLE, NullPolicy.ANY, PPLOperandTypes.NUMERIC)
          .toUDF("RINT");

  public static final SqlFunction EXPM1 =
      adaptMathFunctionToUDF(
              "expm1", ReturnTypes.DOUBLE_FORCE_NULLABLE, NullPolicy.ANY, PPLOperandTypes.NUMERIC)
          .toUDF("EXPM1");

  // IP comparing functions
  public static final SqlFunction NOT_EQUALS_IP =
      CompareIpFunction.notEquals().toUDF("NOT_EQUALS_IP");
  public static final SqlFunction EQUALS_IP = CompareIpFunction.equals().toUDF("EQUALS_IP");
  public static final SqlFunction GREATER_IP = CompareIpFunction.greater().toUDF("GREATER_IP");
  public static final SqlFunction GTE_IP = CompareIpFunction.greaterOrEquals().toUDF("GTE_IP");
  public static final SqlFunction LESS_IP = CompareIpFunction.less().toUDF("LESS_IP");
  public static final SqlFunction LTE_IP = CompareIpFunction.lessOrEquals().toUDF("LTE_IP");

  // Condition function
  public static final SqlFunction EARLIEST = new EarliestFunction().toUDF("EARLIEST");
  public static final SqlFunction LATEST = new LatestFunction().toUDF("LATEST");

  // Datetime function
  public static final SqlFunction TIMESTAMP = new TimestampFunction().toUDF("TIMESTAMP");
  public static final SqlFunction DATE =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprDate",
              PPLReturnTypes.DATE_FORCE_NULLABLE,
              NullPolicy.ARG0,
              PPLOperandTypes.DATE_OR_TIMESTAMP_OR_STRING)
          .toUDF("DATE");
  public static final SqlFunction YEARWEEK = new YearweekFunction().toUDF("YEARWEEK");
  public static final SqlFunction WEEKDAY = new WeekdayFunction().toUDF("WEEKDAY");
  public static final SqlFunction UNIX_TIMESTAMP =
      new UnixTimestampFunction().toUDF("UNIX_TIMESTAMP");
  public static final SqlFunction STRFTIME = new StrftimeFunction().toUDF("STRFTIME");
  public static final SqlFunction TO_SECONDS = new ToSecondsFunction().toUDF("TO_SECONDS");
  public static final SqlFunction ADDTIME =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprAddTime",
              PPLReturnTypes.TIME_APPLY_RETURN_TYPE,
              NullPolicy.ANY,
              PPLOperandTypes.DATETIME_DATETIME)
          .toUDF("ADDTIME");
  public static final SqlFunction SUBTIME =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprSubTime",
              PPLReturnTypes.TIME_APPLY_RETURN_TYPE,
              NullPolicy.ANY,
              PPLOperandTypes.DATETIME_DATETIME)
          .toUDF("SUBTIME");
  public static final SqlFunction ADDDATE = new AddSubDateFunction(true).toUDF("ADDDATE");
  public static final SqlFunction SUBDATE = new AddSubDateFunction(false).toUDF("SUBDATE");
  public static final SqlFunction DATE_ADD = new DateAddSubFunction(true).toUDF("DATE_ADD");
  public static final SqlFunction DATE_SUB = new DateAddSubFunction(false).toUDF("DATE_SUB");
  public static final SqlFunction EXTRACT = new ExtractFunction().toUDF("EXTRACT");
  public static final SqlFunction YEAR = new DatePartFunction(TimeUnit.YEAR).toUDF("YEAR");
  public static final SqlFunction QUARTER = new DatePartFunction(TimeUnit.QUARTER).toUDF("QUARTER");
  public static final SqlFunction MONTH = new DatePartFunction(TimeUnit.MONTH).toUDF("MONTH");
  public static final SqlFunction DAY = new DatePartFunction(TimeUnit.DAY).toUDF("DAY");
  public static final SqlFunction DAY_OF_WEEK =
      new DatePartFunction(TimeUnit.DOW).toUDF("DAY_OF_WEEK");
  public static final SqlFunction DAY_OF_YEAR =
      new DatePartFunction(TimeUnit.DOY).toUDF("DAY_OF_YEAR");
  public static final SqlFunction HOUR = new DatePartFunction(TimeUnit.HOUR).toUDF("HOUR");
  public static final SqlFunction MINUTE = new DatePartFunction(TimeUnit.MINUTE).toUDF("MINUTE");
  public static final SqlFunction MINUTE_OF_DAY =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprMinuteOfDay",
              PPLReturnTypes.INTEGER_FORCE_NULLABLE,
              NullPolicy.ARG0,
              PPLOperandTypes.TIME_OR_TIMESTAMP_OR_STRING)
          .toUDF("MINUTE_OF_DAY");
  public static final SqlFunction SECOND = new DatePartFunction(TimeUnit.SECOND).toUDF("SECOND");
  public static final SqlFunction MICROSECOND =
      new DatePartFunction(TimeUnit.MICROSECOND).toUDF("MICROSECOND");
  public static final SqlFunction NOW = new CurrentFunction(ExprCoreType.TIMESTAMP).toUDF("NOW");
  public static final SqlFunction CURRENT_TIME =
      new CurrentFunction(ExprCoreType.TIME).toUDF("CURRENT_TIME");
  public static final SqlFunction CURRENT_DATE =
      new CurrentFunction(ExprCoreType.DATE).toUDF("CURRENT_DATE");
  public static final SqlFunction DATE_FORMAT =
      new FormatFunction(ExprCoreType.DATE).toUDF("DATE_FORMAT");
  public static final SqlFunction TIME_FORMAT =
      new FormatFunction(ExprCoreType.TIME).toUDF("TIME_FORMAT");
  public static final SqlFunction DAYNAME = new PeriodNameFunction(TimeUnit.DAY).toUDF("DAYNAME");
  public static final SqlFunction MONTHNAME =
      new PeriodNameFunction(TimeUnit.MONTH).toUDF("MONTHNAME");
  public static final SqlFunction CONVERT_TZ =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprConvertTZ",
              PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.TIMESTAMP_OR_STRING_STRING_STRING)
          .toUDF("CONVERT_TZ");
  public static final SqlFunction DATEDIFF =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprDateDiff",
              ReturnTypes.BIGINT_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.DATETIME_DATETIME)
          .toUDF("DATEDIFF");
  public static final SqlFunction TIMESTAMPDIFF =
      new TimestampDiffFunction().toUDF("TIMESTAMPDIFF");
  public static final SqlFunction LAST_DAY = new LastDayFunction().toUDF("LAST_DAY");
  public static final SqlFunction FROM_DAYS =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprFromDays",
              PPLReturnTypes.DATE_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.INTEGER)
          .toUDF("FROM_DAYS");
  public static final SqlFunction FROM_UNIXTIME = new FromUnixTimeFunction().toUDF("FROM_UNIXTIME");
  public static final SqlFunction GET_FORMAT =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprGetFormat",
              ReturnTypes.VARCHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE),
              NullPolicy.ANY,
              PPLOperandTypes.STRING_STRING)
          .toUDF("GET_FORMAT");
  public static final SqlFunction MAKEDATE =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprMakeDate",
              PPLReturnTypes.DATE_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.NUMERIC_NUMERIC)
          .toUDF("MAKEDATE");
  public static final SqlFunction MAKETIME =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprMakeTime",
              PPLReturnTypes.TIME_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.NUMERIC_NUMERIC_NUMERIC)
          .toUDF("MAKETIME");
  public static final SqlFunction PERIOD_DIFF =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprPeriodDiff",
              PPLReturnTypes.INTEGER_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.INTEGER_INTEGER)
          .toUDF("PERIOD_DIFF");
  public static final SqlFunction PERIOD_ADD =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprPeriodAdd",
              PPLReturnTypes.INTEGER_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.INTEGER_INTEGER)
          .toUDF("PERIOD_ADD");
  public static final SqlFunction STR_TO_DATE =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprStrToDate",
              PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.STRING_STRING)
          .toUDF("STR_TO_DATE");
  public static final SqlFunction SYSDATE = new SysdateFunction().toUDF("SYSDATE");
  public static final SqlFunction SEC_TO_TIME = new SecToTimeFunction().toUDF("SEC_TO_TIME");
  public static final SqlFunction TIME =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprTime",
              PPLReturnTypes.TIME_FORCE_NULLABLE,
              NullPolicy.ARG0,
              PPLOperandTypes.DATETIME_OR_STRING)
          .toUDF("TIME");

  // IP cast function
  public static final SqlFunction IP =
      new IPFunction().toUDF(UserDefinedFunctionUtils.IP_FUNCTION_NAME);
  public static final SqlFunction TIME_TO_SEC =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprTimeToSec",
              ReturnTypes.BIGINT_FORCE_NULLABLE,
              NullPolicy.ARG0,
              PPLOperandTypes.TIME_OR_TIMESTAMP_OR_STRING)
          .toUDF("TIME_TO_SEC");
  public static final SqlFunction TIMEDIFF =
      UserDefinedFunctionUtils.adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprTimeDiff",
              PPLReturnTypes.TIME_FORCE_NULLABLE,
              NullPolicy.ANY,
              PPLOperandTypes.TIME_TIME)
          .toUDF("TIME_DIFF");
  public static final SqlFunction TIMESTAMPADD = new TimestampAddFunction().toUDF("TIMESTAMPADD");
  public static final SqlFunction TO_DAYS =
      adaptExprMethodToUDF(
              DateTimeFunctions.class,
              "exprToDays",
              ReturnTypes.BIGINT_FORCE_NULLABLE,
              NullPolicy.ARG0,
              PPLOperandTypes.DATE_OR_TIMESTAMP_OR_STRING)
          .toUDF("TO_DAYS");
  public static final SqlFunction DATETIME = new DatetimeFunction().toUDF("DATETIME");
  public static final SqlFunction UTC_DATE =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprUtcDate",
              PPLReturnTypes.DATE_FORCE_NULLABLE,
              NullPolicy.NONE,
              PPLOperandTypes.NONE)
          .toUDF("UTC_DATE");
  public static final SqlFunction UTC_TIME =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprUtcTime",
              PPLReturnTypes.TIME_FORCE_NULLABLE,
              NullPolicy.NONE,
              PPLOperandTypes.NONE)
          .toUDF("UTC_TIME");
  public static final SqlFunction UTC_TIMESTAMP =
      adaptExprMethodWithPropertiesToUDF(
              DateTimeFunctions.class,
              "exprUtcTimestamp",
              PPLReturnTypes.TIMESTAMP_FORCE_NULLABLE,
              NullPolicy.NONE,
              PPLOperandTypes.NONE)
          .toUDF("UTC_TIMESTAMP");
  public static final SqlFunction WEEK = new WeekFunction().toUDF("WEEK");
  public static final SqlFunction GROK = new ParseFunction().toUDF("GROK");
  // TODO: Figure out if there is other option to perform multiple group match in Calcite
  // For now, keep V2's regexExpression logic to avoid breaking change
  public static final SqlFunction PARSE = new ParseFunction().toUDF("PARSE");
  public static final SqlFunction PATTERN_PARSER =
      new PatternParserFunctionImpl().toUDF("PATTERN_PARSER");

  public static final SqlFunction FORALL = new ForallFunctionImpl().toUDF("forall");
  public static final SqlFunction EXISTS = new ExistsFunctionImpl().toUDF("exists");
  public static final SqlFunction ARRAY = new ArrayFunctionImpl().toUDF("array");
  public static final SqlFunction MAP_APPEND = new MapAppendFunctionImpl().toUDF("map_append");
  public static final SqlFunction MAP_REMOVE = new MapRemoveFunctionImpl().toUDF("map_remove");
  public static final SqlFunction MVAPPEND = new MVAppendFunctionImpl().toUDF("mvappend");
  public static final SqlFunction INTERNAL_APPEND = new AppendFunctionImpl().toUDF("append");
  public static final SqlFunction MVZIP = new MVZipFunctionImpl().toUDF("mvzip");
  public static final SqlFunction MVFIND = new MVFindFunctionImpl().toUDF("mvfind");
  public static final SqlFunction FILTER = new FilterFunctionImpl().toUDF("filter");
  public static final SqlFunction TRANSFORM = new TransformFunctionImpl().toUDF("transform");
  public static final SqlFunction REDUCE = new ReduceFunctionImpl().toUDF("reduce");

  private static final RelevanceQueryFunction RELEVANCE_QUERY_FUNCTION_INSTANCE =
      new RelevanceQueryFunction();
  public static final SqlFunction MATCH = RELEVANCE_QUERY_FUNCTION_INSTANCE.toUDF("match");
  public static final SqlFunction MATCH_PHRASE =
      RELEVANCE_QUERY_FUNCTION_INSTANCE.toUDF("match_phrase");
  public static final SqlFunction MATCH_BOOL_PREFIX =
      RELEVANCE_QUERY_FUNCTION_INSTANCE.toUDF("match_bool_prefix");
  public static final SqlFunction MATCH_PHRASE_PREFIX =
      RELEVANCE_QUERY_FUNCTION_INSTANCE.toUDF("match_phrase_prefix");
  public static final SqlFunction SIMPLE_QUERY_STRING =
      RELEVANCE_QUERY_FUNCTION_INSTANCE.toUDF("simple_query_string", false);
  public static final SqlFunction QUERY_STRING =
      RELEVANCE_QUERY_FUNCTION_INSTANCE.toUDF("query_string", false);
  public static final SqlFunction MULTI_MATCH =
      RELEVANCE_QUERY_FUNCTION_INSTANCE.toUDF("multi_match", false);
  public static final SqlFunction NUMBER_TO_STRING =
      new NumberToStringFunction().toUDF("NUMBER_TO_STRING");
  public static final SqlFunction TONUMBER = new ToNumberFunction().toUDF("TONUMBER");
  public static final SqlFunction TOSTRING = new ToStringFunction().toUDF("TOSTRING");
  public static final SqlFunction WIDTH_BUCKET =
      new org.opensearch.sql.expression.function.udf.binning.WidthBucketFunction()
          .toUDF("WIDTH_BUCKET");
  public static final SqlFunction SPAN_BUCKET =
      new org.opensearch.sql.expression.function.udf.binning.SpanBucketFunction()
          .toUDF("SPAN_BUCKET");
  public static final SqlFunction MINSPAN_BUCKET =
      new org.opensearch.sql.expression.function.udf.binning.MinspanBucketFunction()
          .toUDF("MINSPAN_BUCKET");
  public static final SqlFunction RANGE_BUCKET =
      new org.opensearch.sql.expression.function.udf.binning.RangeBucketFunction()
          .toUDF("RANGE_BUCKET");
  public static final SqlFunction REX_EXTRACT = new RexExtractFunction().toUDF("REX_EXTRACT");
  public static final SqlFunction REX_EXTRACT_MULTI =
      new RexExtractMultiFunction().toUDF("REX_EXTRACT_MULTI");
  public static final SqlFunction REX_OFFSET = new RexOffsetFunction().toUDF("REX_OFFSET");

  // Aggregation functions
  public static final SqlAggFunction AVG_NULLABLE = new NullableSqlAvgAggFunction(SqlKind.AVG);
  public static final SqlAggFunction STDDEV_POP_NULLABLE =
      new NullableSqlAvgAggFunction(SqlKind.STDDEV_POP);
  public static final SqlAggFunction STDDEV_SAMP_NULLABLE =
      new NullableSqlAvgAggFunction(SqlKind.STDDEV_SAMP);
  public static final SqlAggFunction VAR_POP_NULLABLE =
      new NullableSqlAvgAggFunction(SqlKind.VAR_POP);
  public static final SqlAggFunction VAR_SAMP_NULLABLE =
      new NullableSqlAvgAggFunction(SqlKind.VAR_SAMP);
  public static final SqlAggFunction TAKE =
      createUserDefinedAggFunction(
          TakeAggFunction.class,
          "TAKE",
          PPLReturnTypes.ARG0_ARRAY,
          PPLOperandTypes.ANY_OPTIONAL_INTEGER);
  public static final SqlAggFunction FIRST =
      createUserDefinedAggFunction(
          FirstAggFunction.class, "FIRST", ReturnTypes.ARG0, PPLOperandTypes.ANY_OPTIONAL_INTEGER);
  public static final SqlAggFunction LAST =
      createUserDefinedAggFunction(
          LastAggFunction.class, "LAST", ReturnTypes.ARG0, PPLOperandTypes.ANY_OPTIONAL_INTEGER);
  public static final SqlAggFunction PERCENTILE_APPROX =
      createUserDefinedAggFunction(
          PercentileApproxFunction.class,
          "percentile_approx",
          ReturnTypes.ARG0_FORCE_NULLABLE,
          PPLOperandTypes.NUMERIC_NUMERIC_OPTIONAL_NUMERIC_SYMBOL);
  public static final SqlAggFunction INTERNAL_PATTERN =
      createUserDefinedAggFunction(
          LogPatternAggFunction.class,
          "pattern",
          ReturnTypes.explicit(UserDefinedFunctionUtils.nullablePatternAggList),
          UDFOperandMetadata.wrap(
              OperandTypes.VARIADIC)); // operand types of patterns are very flexible
  public static final SqlAggFunction LIST =
      createUserDefinedAggFunction(
          ListAggFunction.class, "LIST", PPLReturnTypes.STRING_ARRAY, PPLOperandTypes.SCALAR);
  public static final SqlAggFunction VALUES =
      createUserDefinedAggFunction(
          ValuesAggFunction.class,
          "VALUES",
          PPLReturnTypes.STRING_ARRAY,
          PPLOperandTypes.SCALAR_OPTIONAL_INTEGER);

  public static final SqlFunction ATAN =
      new SqlFunction(
          "ATAN",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.NUMERIC_OPTIONAL_NUMERIC,
          SqlFunctionCategory.NUMERIC) {
        @Override
        public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
          if (call instanceof SqlBasicCall) {
            SqlOperator op =
                call.getOperandList().size() == 2
                    ? SqlStdOperatorTable.ATAN2
                    : SqlStdOperatorTable.ATAN;
            ((SqlBasicCall) call).setOperator(op);
          }
          return call;
        }
      };
  // SPARK dialect is not included in lookup table to resolve overrides issues (e.g. reverse
  // function won't work if spark is included because there are multiple overrides of reverse, and
  // it will choose none of them in the end.) Therefore, SPARK functions used are explicitly
  // declared here for lookup.
  public static final SqlFunction REGEXP = SqlLibraryOperators.REGEXP;

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
      RexToLixTranslator translator, RexCall call, SqlFunction operator, Expression field)
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
