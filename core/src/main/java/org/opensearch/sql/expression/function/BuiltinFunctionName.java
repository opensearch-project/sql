/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import com.google.common.collect.ImmutableMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** Builtin Function Name. */
@Getter
@RequiredArgsConstructor
public enum BuiltinFunctionName {
  /** Mathematical Functions. */
  ABS(FunctionName.of("abs")),
  CEIL(FunctionName.of("ceil")),
  CEILING(FunctionName.of("ceiling")),
  CONV(FunctionName.of("conv")),
  CRC32(FunctionName.of("crc32")),
  E(FunctionName.of("e")),
  EXP(FunctionName.of("exp")),
  EXPM1(FunctionName.of("expm1")),
  FLOOR(FunctionName.of("floor")),
  LN(FunctionName.of("ln")),
  LOG(FunctionName.of("log")),
  LOG10(FunctionName.of("log10")),
  LOG2(FunctionName.of("log2")),
  PI(FunctionName.of("pi")),
  POW(FunctionName.of("pow")),
  POWER(FunctionName.of("power")),
  RAND(FunctionName.of("rand")),
  RINT(FunctionName.of("rint")),
  ROUND(FunctionName.of("round")),
  SIGN(FunctionName.of("sign")),
  SIGNUM(FunctionName.of("signum")),
  SINH(FunctionName.of("sinh")),
  SQRT(FunctionName.of("sqrt")),
  CBRT(FunctionName.of("cbrt")),
  TRUNCATE(FunctionName.of("truncate")),

  ACOS(FunctionName.of("acos")),
  ASIN(FunctionName.of("asin")),
  ATAN(FunctionName.of("atan")),
  ATAN2(FunctionName.of("atan2")),
  COS(FunctionName.of("cos")),
  COSH(FunctionName.of("cosh")),
  COT(FunctionName.of("cot")),
  DEGREES(FunctionName.of("degrees")),
  RADIANS(FunctionName.of("radians")),
  SIN(FunctionName.of("sin")),
  TAN(FunctionName.of("tan")),

  /** Date and Time Functions. */
  ADDDATE(FunctionName.of("adddate")),
  ADDTIME(FunctionName.of("addtime")),
  CONVERT_TZ(FunctionName.of("convert_tz")),
  DATE(FunctionName.of("date")),
  DATEDIFF(FunctionName.of("datediff")),
  DATETIME(FunctionName.of("datetime")),
  DATE_ADD(FunctionName.of("date_add")),
  DATE_FORMAT(FunctionName.of("date_format")),
  DATE_SUB(FunctionName.of("date_sub")),
  DAY(FunctionName.of("day")),
  DAYNAME(FunctionName.of("dayname")),
  DAYOFMONTH(FunctionName.of("dayofmonth")),
  DAY_OF_MONTH(FunctionName.of("day_of_month")),
  DAYOFWEEK(FunctionName.of("dayofweek")),
  DAYOFYEAR(FunctionName.of("dayofyear")),
  DAY_OF_WEEK(FunctionName.of("day_of_week")),
  DAY_OF_YEAR(FunctionName.of("day_of_year")),
  EXTRACT(FunctionName.of("extract")),
  FROM_DAYS(FunctionName.of("from_days")),
  FROM_UNIXTIME(FunctionName.of("from_unixtime")),
  GET_FORMAT(FunctionName.of("get_format")),
  HOUR(FunctionName.of("hour")),
  HOUR_OF_DAY(FunctionName.of("hour_of_day")),
  LAST_DAY(FunctionName.of("last_day")),
  MAKEDATE(FunctionName.of("makedate")),
  MAKETIME(FunctionName.of("maketime")),
  MICROSECOND(FunctionName.of("microsecond")),
  MINUTE(FunctionName.of("minute")),
  MINUTE_OF_DAY(FunctionName.of("minute_of_day")),
  MINUTE_OF_HOUR(FunctionName.of("minute_of_hour")),
  MONTH(FunctionName.of("month")),
  MONTH_OF_YEAR(FunctionName.of("month_of_year")),
  MONTHNAME(FunctionName.of("monthname")),
  PERIOD_ADD(FunctionName.of("period_add")),
  PERIOD_DIFF(FunctionName.of("period_diff")),
  QUARTER(FunctionName.of("quarter")),
  SEC_TO_TIME(FunctionName.of("sec_to_time")),
  SECOND(FunctionName.of("second")),
  SECOND_OF_MINUTE(FunctionName.of("second_of_minute")),
  STR_TO_DATE(FunctionName.of("str_to_date")),
  SUBDATE(FunctionName.of("subdate")),
  SUBTIME(FunctionName.of("subtime")),
  TIME(FunctionName.of("time")),
  TIMEDIFF(FunctionName.of("timediff")),
  TIME_TO_SEC(FunctionName.of("time_to_sec")),
  TIMESTAMP(FunctionName.of("timestamp")),
  TIMESTAMPADD(FunctionName.of("timestampadd")),
  TIMESTAMPDIFF(FunctionName.of("timestampdiff")),
  TIME_FORMAT(FunctionName.of("time_format")),
  TO_DAYS(FunctionName.of("to_days")),
  TO_SECONDS(FunctionName.of("to_seconds")),
  UTC_DATE(FunctionName.of("utc_date")),
  UTC_TIME(FunctionName.of("utc_time")),
  UTC_TIMESTAMP(FunctionName.of("utc_timestamp")),
  UNIX_TIMESTAMP(FunctionName.of("unix_timestamp")),
  WEEK(FunctionName.of("week")),
  WEEKDAY(FunctionName.of("weekday")),
  WEEKOFYEAR(FunctionName.of("weekofyear")),
  WEEK_OF_YEAR(FunctionName.of("week_of_year")),
  YEAR(FunctionName.of("year")),
  YEARWEEK(FunctionName.of("yearweek")),

  // `now`-like functions
  NOW(FunctionName.of("now")),
  CURDATE(FunctionName.of("curdate")),
  CURRENT_DATE(FunctionName.of("current_date")),
  CURTIME(FunctionName.of("curtime")),
  CURRENT_TIME(FunctionName.of("current_time")),
  LOCALTIME(FunctionName.of("localtime")),
  CURRENT_TIMESTAMP(FunctionName.of("current_timestamp")),
  LOCALTIMESTAMP(FunctionName.of("localtimestamp")),
  SYSDATE(FunctionName.of("sysdate")),

  /** Text Functions. */
  TOSTRING(FunctionName.of("tostring")),

  /** Arithmetic Operators. */
  ADD(FunctionName.of("+")),
  ADDFUNCTION(FunctionName.of("add")),
  DIVIDE(FunctionName.of("/")),
  DIVIDEFUNCTION(FunctionName.of("divide")),
  MOD(FunctionName.of("mod")),
  MODULUS(FunctionName.of("%")),
  MODULUSFUNCTION(FunctionName.of("modulus")),
  MULTIPLY(FunctionName.of("*")),
  MULTIPLYFUNCTION(FunctionName.of("multiply")),
  SUBTRACT(FunctionName.of("-")),
  SUBTRACTFUNCTION(FunctionName.of("subtract")),

  /** Boolean Operators. */
  AND(FunctionName.of("and")),
  OR(FunctionName.of("or")),
  XOR(FunctionName.of("xor")),
  NOT(FunctionName.of("not")),
  EQUAL(FunctionName.of("=")),
  NOTEQUAL(FunctionName.of("!=")),
  LESS(FunctionName.of("<")),
  LTE(FunctionName.of("<=")),
  GREATER(FunctionName.of(">")),
  GTE(FunctionName.of(">=")),
  LIKE(FunctionName.of("like")),
  NOT_LIKE(FunctionName.of("not like")),

  /** Aggregation Function. */
  AVG(FunctionName.of("avg")),
  SUM(FunctionName.of("sum")),
  COUNT(FunctionName.of("count")),
  MIN(FunctionName.of("min")),
  MAX(FunctionName.of("max")),
  // sample variance
  VARSAMP(FunctionName.of("var_samp")),
  // population standard variance
  VARPOP(FunctionName.of("var_pop")),
  // sample standard deviation.
  STDDEV_SAMP(FunctionName.of("stddev_samp")),
  // population standard deviation.
  STDDEV_POP(FunctionName.of("stddev_pop")),
  // take top documents from aggregation bucket.
  TAKE(FunctionName.of("take")),
  // t-digest percentile which is used in OpenSearch core by default.
  PERCENTILE_APPROX(FunctionName.of("percentile_approx")),
  // Not always an aggregation query
  NESTED(FunctionName.of("nested")),

  /** Text Functions. */
  ASCII(FunctionName.of("ascii")),
  CONCAT(FunctionName.of("concat")),
  CONCAT_WS(FunctionName.of("concat_ws")),
  LEFT(FunctionName.of("left")),
  LENGTH(FunctionName.of("length")),
  LOCATE(FunctionName.of("locate")),
  LOWER(FunctionName.of("lower")),
  LTRIM(FunctionName.of("ltrim")),
  POSITION(FunctionName.of("position")),
  REGEXP(FunctionName.of("regexp")),
  REPLACE(FunctionName.of("replace")),
  REVERSE(FunctionName.of("reverse")),
  RIGHT(FunctionName.of("right")),
  RTRIM(FunctionName.of("rtrim")),
  STRCMP(FunctionName.of("strcmp")),
  SUBSTR(FunctionName.of("substr")),
  SUBSTRING(FunctionName.of("substring")),
  TRIM(FunctionName.of("trim")),
  UPPER(FunctionName.of("upper")),

  /** NULL Test. */
  IS_NULL(FunctionName.of("is null")),
  IS_NOT_NULL(FunctionName.of("is not null")),
  IFNULL(FunctionName.of("ifnull")),
  IF(FunctionName.of("if")),
  NULLIF(FunctionName.of("nullif")),
  ISNULL(FunctionName.of("isnull")),

  ROW_NUMBER(FunctionName.of("row_number")),
  RANK(FunctionName.of("rank")),
  DENSE_RANK(FunctionName.of("dense_rank")),

  INTERVAL(FunctionName.of("interval")),

  /** Data Type Convert Function. */
  CAST_TO_STRING(FunctionName.of("cast_to_string")),
  CAST_TO_BYTE(FunctionName.of("cast_to_byte")),
  CAST_TO_SHORT(FunctionName.of("cast_to_short")),
  CAST_TO_INT(FunctionName.of("cast_to_int")),
  CAST_TO_LONG(FunctionName.of("cast_to_long")),
  CAST_TO_FLOAT(FunctionName.of("cast_to_float")),
  CAST_TO_DOUBLE(FunctionName.of("cast_to_double")),
  CAST_TO_BOOLEAN(FunctionName.of("cast_to_boolean")),
  CAST_TO_DATE(FunctionName.of("cast_to_date")),
  CAST_TO_TIME(FunctionName.of("cast_to_time")),
  CAST_TO_TIMESTAMP(FunctionName.of("cast_to_timestamp")),
  CAST_TO_DATETIME(FunctionName.of("cast_to_datetime")),
  TYPEOF(FunctionName.of("typeof")),

  /** Relevance Function. */
  MATCH(FunctionName.of("match")),
  SIMPLE_QUERY_STRING(FunctionName.of("simple_query_string")),
  MATCH_PHRASE(FunctionName.of("match_phrase")),
  MATCHPHRASE(FunctionName.of("matchphrase")),
  MATCHPHRASEQUERY(FunctionName.of("matchphrasequery")),
  QUERY_STRING(FunctionName.of("query_string")),
  MATCH_BOOL_PREFIX(FunctionName.of("match_bool_prefix")),
  HIGHLIGHT(FunctionName.of("highlight")),
  MATCH_PHRASE_PREFIX(FunctionName.of("match_phrase_prefix")),
  SCORE(FunctionName.of("score")),
  SCOREQUERY(FunctionName.of("scorequery")),
  SCORE_QUERY(FunctionName.of("score_query")),

  /** Legacy Relevance Function. */
  QUERY(FunctionName.of("query")),
  MATCH_QUERY(FunctionName.of("match_query")),
  MATCHQUERY(FunctionName.of("matchquery")),
  MULTI_MATCH(FunctionName.of("multi_match")),
  MULTIMATCH(FunctionName.of("multimatch")),
  MULTIMATCHQUERY(FunctionName.of("multimatchquery")),
  WILDCARDQUERY(FunctionName.of("wildcardquery")),
  WILDCARD_QUERY(FunctionName.of("wildcard_query")),

  /** Geospatial Function. */
  GEOIP(FunctionName.of("geoip")),
  IPLOCATION(FunctionName.of("iplocation"));

  private final FunctionName name;

  private static final Map<FunctionName, BuiltinFunctionName> ALL_NATIVE_FUNCTIONS;

  static {
    ImmutableMap.Builder<FunctionName, BuiltinFunctionName> builder = new ImmutableMap.Builder<>();
    for (BuiltinFunctionName func : BuiltinFunctionName.values()) {
      builder.put(func.getName(), func);
    }
    ALL_NATIVE_FUNCTIONS = builder.build();
  }

  private static final Map<String, BuiltinFunctionName> AGGREGATION_FUNC_MAPPING =
      new ImmutableMap.Builder<String, BuiltinFunctionName>()
          .put("max", BuiltinFunctionName.MAX)
          .put("min", BuiltinFunctionName.MIN)
          .put("avg", BuiltinFunctionName.AVG)
          .put("count", BuiltinFunctionName.COUNT)
          .put("sum", BuiltinFunctionName.SUM)
          .put("var_pop", BuiltinFunctionName.VARPOP)
          .put("var_samp", BuiltinFunctionName.VARSAMP)
          .put("variance", BuiltinFunctionName.VARPOP)
          .put("std", BuiltinFunctionName.STDDEV_POP)
          .put("stddev", BuiltinFunctionName.STDDEV_POP)
          .put("stddev_pop", BuiltinFunctionName.STDDEV_POP)
          .put("stddev_samp", BuiltinFunctionName.STDDEV_SAMP)
          .put("take", BuiltinFunctionName.TAKE)
          .put("percentile", BuiltinFunctionName.PERCENTILE_APPROX)
          .put("percentile_approx", BuiltinFunctionName.PERCENTILE_APPROX)
          .build();

  public static Optional<BuiltinFunctionName> of(String str) {
    return Optional.ofNullable(ALL_NATIVE_FUNCTIONS.getOrDefault(FunctionName.of(str), null));
  }

  public static Optional<BuiltinFunctionName> ofAggregation(String functionName) {
    return Optional.ofNullable(
        AGGREGATION_FUNC_MAPPING.getOrDefault(functionName.toLowerCase(Locale.ROOT), null));
  }
}
