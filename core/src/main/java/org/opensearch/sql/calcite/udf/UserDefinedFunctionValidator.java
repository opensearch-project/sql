/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;

public class UserDefinedFunctionValidator {
  private static final Logger logger = LogManager.getLogger(UserDefinedFunctionValidator.class);

  public static final Set<SqlTypeName> STRING_TYPES = Set.of(SqlTypeName.VARCHAR, SqlTypeName.CHAR);
  public static final Set<SqlTypeName> INTEGRAL_TYPES =
      Set.of(SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.TINYINT, SqlTypeName.SMALLINT);
  public static final Set<SqlTypeName> NUMERIC_TYPES =
      Set.of(
          SqlTypeName.TINYINT,
          SqlTypeName.SMALLINT,
          SqlTypeName.INTEGER,
          SqlTypeName.DECIMAL,
          SqlTypeName.DOUBLE,
          SqlTypeName.FLOAT,
          SqlTypeName.BIGINT,
          SqlTypeName.REAL);
  public static final Set<SqlTypeName> DATE_TIMESTAMP_TYPES =
      Set.of(SqlTypeName.DATE, SqlTypeName.TIMESTAMP, SqlTypeName.VARCHAR, SqlTypeName.CHAR);
  public static final Set<SqlTypeName> DATE_TIME_TIMESTAMP_TYPES =
      Set.of(
          SqlTypeName.DATE,
          SqlTypeName.TIME,
          SqlTypeName.TIMESTAMP,
          SqlTypeName.VARCHAR,
          SqlTypeName.CHAR);
  public static final Set<SqlTypeName> INTERVAL_TYPES =
      ImmutableSet.copyOf(SqlTypeFamily.DATETIME_INTERVAL.getTypeNames());
  public static final List<List<SqlTypeName>> EMPTY_ARG = ImmutableList.of(ImmutableList.of());

  /**
   * Validate the function arguments against the supported overloads.
   *
   * @param op The name of the function to be validated. It is case-insensitive.
   * @param argList The list of arguments passed to the function. Each argument is a RexNode.
   * @return True if the arguments match one of the supported overloads of the function.
   */
  public static boolean validateFunction(String op, List<RexNode> argList) {
    List<SqlTypeName> argTypeNames =
        argList.stream().map(UserDefinedFunctionUtils::transferDateRelatedTimeName).toList();
    op = op.toUpperCase(Locale.ROOT);
    List<List<SqlTypeName>> overloads = getSupportedOverloads(op);
    if (overloads == null) {
      logger.warn(
          "Acceptable parameters are not defined for UDF {}, skipping parameter checking", op);
      return true;
    }
    boolean matched = false;
    for (List<SqlTypeName> overload : overloads) {
      if (validateArguments(argTypeNames, overload)) {
        matched = true;
        break;
      }
    }
    return matched;
  }

  /**
   * Get the supported overloads of the function. The overloads are defined by the UDF
   *
   * @param op The name of the function to be validated. It is case-insensitive.
   * @return A list of overloads, where each overload is a list of SqlTypeName. Each overload
   *     represents a combination of argument types that the function can accept. If the function is
   *     not supported, it returns null.
   */
  public static List<List<SqlTypeName>> getSupportedOverloads(String op) {
    op = op.toUpperCase(Locale.ROOT);
    Iterable<List<SqlTypeName>> overloads =
        switch (op) {
            // STRING FUNCTIONS
          case "REPLACE" -> overload(STRING_TYPES, STRING_TYPES, STRING_TYPES);
            // MATH FUNCTIONS
          case "ABS",
              "ACOS",
              "ASIN",
              "COS",
              "COT",
              "DEGREES",
              "EXP",
              "FLOOR",
              "LN",
              "LOG2",
              "LOG10",
              "RADIANS",
              "SIGN",
              "SIN",
              "SQRT",
              "CBRT" -> overload(NUMERIC_TYPES);
          case "ATAN", "LOG" -> Iterables.concat(
              overload(NUMERIC_TYPES), overload(NUMERIC_TYPES, NUMERIC_TYPES));
          case "ATAN2", "MOD", "POW", "POWER" -> overload(NUMERIC_TYPES, NUMERIC_TYPES);
          case "CEIL", "CEILING" -> overload(NUMERIC_TYPES);
          case "CONV" -> Iterables.concat(
              overload(STRING_TYPES, INTEGRAL_TYPES, INTEGRAL_TYPES),
              overload(INTEGRAL_TYPES, INTEGRAL_TYPES, INTEGRAL_TYPES));
          case "CRC32" -> overload(STRING_TYPES);
          case "E", "PI" -> EMPTY_ARG;
          case "RAND" -> Iterables.concat(EMPTY_ARG, overload(INTEGRAL_TYPES));
          case "ROUND" -> Iterables.concat(
              overload(NUMERIC_TYPES), overload(NUMERIC_TYPES, INTEGRAL_TYPES));

            // DATE TIME FUNCTIONS
          case "ADDDATE", "SUBDATE" -> Iterables.concat(
              overload(DATE_TIME_TIMESTAMP_TYPES, INTERVAL_TYPES),
              overload(DATE_TIME_TIMESTAMP_TYPES, INTEGRAL_TYPES));
          case "DATE_ADD", "DATE_SUB" -> overload(DATE_TIME_TIMESTAMP_TYPES, INTERVAL_TYPES);
          case "ADDTIME", "SUBTIME", "DATEDIFF" -> overload(
              DATE_TIME_TIMESTAMP_TYPES, DATE_TIME_TIMESTAMP_TYPES);
          case "CONVERT_TZ" -> overload(DATE_TIME_TIMESTAMP_TYPES, STRING_TYPES, STRING_TYPES);
          case "CURDATE",
              "CURRENT_DATE",
              "CURRENT_TIME",
              "LOCALTIMESTAMP",
              "LOCALTIME",
              "CURRENT_TIMESTAMP",
              "NOW",
              "CURTIME",
              "UTC_DATE",
              "UTC_TIME",
              "UTC_TIMESTAMP" -> EMPTY_ARG;
          case "DAY",
              "DAY_OF_WEEK",
              "DAYOFWEEK",
              "DAY_OF_YEAR",
              "DAYOFYEAR",
              "DAYNAME",
              "DAYOFMONTH",
              "DAY_OF_MONTH",
              "MONTHNAME",
              "QUARTER",
              "TO_DAYS",
              "YEAR",
              "DATE" -> overload(DATE_TIMESTAMP_TYPES);
          case "HOUR",
              "HOUR_OF_DAY",
              "LAST_DAY",
              "MICROSECOND",
              "MINUTE",
              "MINUTE_OF_DAY",
              "MINUTE_OF_HOUR",
              "MONTH",
              "MONTH_OF_YEAR",
              "SECOND",
              "SECOND_OF_MINUTE",
              "WEEKDAY",
              "TIME",
              "TIME_TO_SEC" -> overload(DATE_TIME_TIMESTAMP_TYPES);
          case "MAKEDATE" -> overload(NUMERIC_TYPES, NUMERIC_TYPES);
          case "MAKETIME" -> overload(NUMERIC_TYPES, NUMERIC_TYPES, NUMERIC_TYPES);
          case "EXTRACT" -> overload(STRING_TYPES, DATE_TIME_TIMESTAMP_TYPES);
          case "FROM_DAYS" -> overload(INTEGRAL_TYPES);
          case "SEC_TO_TIME" -> overload(NUMERIC_TYPES);
          case "FROM_UNIXTIME" -> Iterables.concat(
              overload(NUMERIC_TYPES), overload(NUMERIC_TYPES, STRING_TYPES));
          case "GET_FORMAT", "STR_TO_DATE" -> overload(STRING_TYPES, STRING_TYPES);
          case "DATETIME" -> Iterables.concat(
              overload(DATE_TIME_TIMESTAMP_TYPES, STRING_TYPES),
              overload(DATE_TIME_TIMESTAMP_TYPES));
          case "SYSDATE" -> Iterables.concat(EMPTY_ARG, overload(INTEGRAL_TYPES));
          case "DATE_FORMAT", "TIME_FORMAT" -> overload(DATE_TIME_TIMESTAMP_TYPES, STRING_TYPES);
          case "PERIOD_ADD", "PERIOD_DIFF" -> overload(INTEGRAL_TYPES, INTEGRAL_TYPES);
          case "TIMESTAMP" -> Iterables.concat(
              overload(DATE_TIME_TIMESTAMP_TYPES),
              overload(DATE_TIME_TIMESTAMP_TYPES, DATE_TIME_TIMESTAMP_TYPES));
          case "TIMESTAMPADD" -> overload(STRING_TYPES, NUMERIC_TYPES, DATE_TIME_TIMESTAMP_TYPES);
          case "TIMESTAMPDIFF" -> overload(
              STRING_TYPES, DATE_TIME_TIMESTAMP_TYPES, DATE_TIME_TIMESTAMP_TYPES);
          case "TIMEDIFF" -> Iterables.concat(
              overload(SqlTypeName.TIME, SqlTypeName.TIME),
              overload(SqlTypeName.TIME, STRING_TYPES),
              overload(STRING_TYPES, SqlTypeName.TIME),
              overload(STRING_TYPES, STRING_TYPES));
          case "TO_SECONDS" -> Iterables.concat(
              overload(DATE_TIME_TIMESTAMP_TYPES), overload(INTEGRAL_TYPES));
          case "UNIX_TIMESTAMP" -> Iterables.concat(
              EMPTY_ARG, overload(NUMERIC_TYPES), overload(DATE_TIMESTAMP_TYPES));
          case "WEEK", "WEEK_OF_YEAR" -> Iterables.concat(
              overload(DATE_TIME_TIMESTAMP_TYPES),
              overload(DATE_TIME_TIMESTAMP_TYPES, INTEGRAL_TYPES));
          case "YEARWEEK" -> Iterables.concat(
              overload(DATE_TIME_TIMESTAMP_TYPES),
              overload(DATE_TIME_TIMESTAMP_TYPES, NUMERIC_TYPES));
          default -> null;
        };
    if (overloads == null) {
      return null;
    }
    return ImmutableList.copyOf(overloads);
  }

  /**
   * * Get the overloads of the function. When some elements are Set, the returned overloads will be
   * the cartesian product of those elements.
   *
   * @param args Each element can be either a SqlTypeName or a Set of SqlTypeName
   * @return A list of possible overloads
   */
  static List<List<SqlTypeName>> overload(Object... args) {
    // 1. Convert the input to a list of list of SqlTypeName
    List<List<SqlTypeName>> argSets = new ArrayList<>();
    for (Object arg : args) {
      if (arg instanceof SqlTypeName) {
        argSets.add(ImmutableList.of((SqlTypeName) arg));
      } else if (arg instanceof Collection<?>) {
        Set<SqlTypeName> s = new HashSet<>();
        for (Object o : (Collection<?>) arg) {
          if (o instanceof SqlTypeName) {
            s.add((SqlTypeName) o);
          } else {
            throw new IllegalArgumentException("Invalid argument type: " + o.getClass());
          }
        }
        argSets.add(ImmutableList.copyOf(s));
      } else {
        throw new IllegalArgumentException("Invalid argument type: " + arg.getClass());
      }
    }
    // 2. Get the cartesian product of the list of set
    return Lists.cartesianProduct(argSets);
  }

  static boolean validateArguments(List<SqlTypeName> inputTypes, List<SqlTypeName> expectedTypes) {
    if (inputTypes.size() != expectedTypes.size()) {
      return false;
    }
    for (int i = 0; i < inputTypes.size(); i++) {
      SqlTypeName inputType = inputTypes.get(i);
      SqlTypeName expectedType = expectedTypes.get(i);
      if (!expectedType.equals(inputType)) {
        return false;
      }
    }
    return true;
  }
}
