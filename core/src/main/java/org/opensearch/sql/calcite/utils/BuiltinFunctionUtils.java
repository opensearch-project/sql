/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static java.lang.Math.E;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.getLegacyTypeName;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.ExtendedRexBuilder;
import org.opensearch.sql.calcite.udf.SpanFunction;
import org.opensearch.sql.calcite.udf.conditionUDF.IfFunction;
import org.opensearch.sql.calcite.udf.conditionUDF.IfNullFunction;
import org.opensearch.sql.calcite.udf.conditionUDF.NullIfFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.ConvertTZFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.DateAddSubFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.DateDiffFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.DateFormatFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.DateFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.DatetimeFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.DayFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.DayOfWeekFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.DayOfYearFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.ExtractFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.FromDaysFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.FromUnixTimestampFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.GetFormatFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.HourFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.LastDayFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.MakeDateFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.MakeTimeFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.MicrosecondFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.MinuteFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.MinuteOfDayFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.MonthFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.PeriodAddFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.PeriodDiffFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.PeriodNameFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.PostprocessForUDTFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.QuarterFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.SecondFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.SecondToTimeFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.StrToDateFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.SysdateFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimeAddSubFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimeDiffFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimeFormatFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimeFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimeToSecondFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimestampAddFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimestampDiffFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimestampFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.ToDaysFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.ToSecondsFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.UnixTimeStampFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.UtcDateFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.UtcTimeFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.UtcTimeStampFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.WeekDayFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.WeekFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.YearFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.YearWeekFunction;
import org.opensearch.sql.calcite.udf.mathUDF.CRC32Function;
import org.opensearch.sql.calcite.udf.mathUDF.ConvFunction;
import org.opensearch.sql.calcite.udf.mathUDF.DivideFunction;
import org.opensearch.sql.calcite.udf.mathUDF.EulerFunction;
import org.opensearch.sql.calcite.udf.mathUDF.ModFunction;
import org.opensearch.sql.calcite.udf.mathUDF.SqrtFunction;
import org.opensearch.sql.calcite.udf.systemUDF.TypeOfFunction;
import org.opensearch.sql.calcite.udf.textUDF.LocateFunction;
import org.opensearch.sql.calcite.udf.textUDF.ReplaceFunction;

/**
 * TODO: We need to refactor code to make all related part together and directly return call TODO:
 * Remove all makeFlag and use literal directly
 */
public interface BuiltinFunctionUtils {

  Set<String> TIME_EXCLUSIVE_OPS =
      Set.of("SECOND", "SECOND_OF_MINUTE", "MINUTE", "MINUTE_OF_HOUR", "HOUR", "HOUR_OF_DAY");

  static SqlReturnTypeInference VARCHAR_FORCE_NULLABLE =
      ReturnTypes.VARCHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE);

  static SqlOperator translate(String op) {
    String capitalOP = op.toUpperCase(Locale.ROOT);
    switch (capitalOP) {
      case "AND":
        return SqlStdOperatorTable.AND;
      case "OR":
        return SqlStdOperatorTable.OR;
      case "NOT":
        return SqlStdOperatorTable.NOT;
      case "XOR":
      case "!=":
        return SqlStdOperatorTable.NOT_EQUALS;
      case "=":
        return SqlStdOperatorTable.EQUALS;
      case "<>":
      case ">":
        return SqlStdOperatorTable.GREATER_THAN;
      case ">=":
        return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
      case "<":
        return SqlStdOperatorTable.LESS_THAN;
      case "<=":
        return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
      case "REGEXP":
        return SqlLibraryOperators.REGEXP;
      case "+":
        return SqlStdOperatorTable.PLUS;
      case "-":
        return SqlStdOperatorTable.MINUS;
      case "*":
        return SqlStdOperatorTable.MULTIPLY;
      case "/":
        return TransferUserDefinedFunction(
            DivideFunction.class, "/", ReturnTypes.QUOTIENT_NULLABLE);
        // Built-in String Functions
      case "ASCII":
        return SqlStdOperatorTable.ASCII;
      case "CONCAT":
        return SqlLibraryOperators.CONCAT_FUNCTION;
      case "CONCAT_WS":
        return SqlLibraryOperators.CONCAT_WS;
      case "LIKE":
        return SqlLibraryOperators.ILIKE;
      case "LTRIM", "RTRIM", "TRIM":
        return SqlStdOperatorTable.TRIM;
      case "LENGTH":
        return SqlStdOperatorTable.CHAR_LENGTH;
      case "LOWER":
        return SqlStdOperatorTable.LOWER;
      case "POSITION":
        return SqlStdOperatorTable.POSITION;
      case "REVERSE":
        return SqlLibraryOperators.REVERSE;
      case "RIGHT":
        return SqlLibraryOperators.RIGHT;
      case "LEFT":
        return SqlLibraryOperators.LEFT;
      case "SUBSTRING", "SUBSTR":
        return SqlStdOperatorTable.SUBSTRING;
      case "STRCMP":
        return SqlLibraryOperators.STRCMP;
      case "REPLACE":
        return TransferUserDefinedFunction(
            ReplaceFunction.class, "REPLACE", VARCHAR_FORCE_NULLABLE);
      case "LOCATE":
        return TransferUserDefinedFunction(
            LocateFunction.class,
            "LOCATE",
            ReturnTypes.INTEGER.andThen(SqlTypeTransforms.FORCE_NULLABLE));
      case "UPPER":
        return SqlStdOperatorTable.UPPER;
        // Built-in Math Functions
      case "ABS":
        return SqlStdOperatorTable.ABS;
      case "ACOS":
        return SqlStdOperatorTable.ACOS;
      case "ASIN":
        return SqlStdOperatorTable.ASIN;
      case "ATAN", "ATAN2":
        return SqlStdOperatorTable.ATAN2;
      case "CEILING":
        return SqlStdOperatorTable.CEIL;
      case "CONV":
        // The CONV function in PPL converts between numerical bases,
        // while SqlStdOperatorTable.CONVERT converts between charsets.
        return TransferUserDefinedFunction(ConvFunction.class, "CONVERT", VARCHAR_FORCE_NULLABLE);
      case "COS":
        return SqlStdOperatorTable.COS;
      case "COT":
        return SqlStdOperatorTable.COT;
      case "CRC32":
        return TransferUserDefinedFunction(CRC32Function.class, "CRC32", ReturnTypes.BIGINT);
      case "DEGREES":
        return SqlStdOperatorTable.DEGREES;
      case "E":
        return TransferUserDefinedFunction(EulerFunction.class, "E", ReturnTypes.DOUBLE);
      case "EXP":
        return SqlStdOperatorTable.EXP;
      case "FLOOR":
        return SqlStdOperatorTable.FLOOR;
      case "LN":
        return SqlStdOperatorTable.LN;
      case "LOG":
        return SqlLibraryOperators.LOG;
      case "LOG2":
        return SqlLibraryOperators.LOG2;
      case "LOG10":
        return SqlStdOperatorTable.LOG10;
      case "MOD", "%":
        // The MOD function in PPL supports floating-point parameters, e.g., MOD(5.5, 2) = 1.5,
        // MOD(3.1, 2.1) = 1.1,
        // whereas SqlStdOperatorTable.MOD supports only integer / long parameters.
        return TransferUserDefinedFunction(ModFunction.class, "MOD", ReturnTypes.LEAST_RESTRICTIVE);
      case "PI":
        return SqlStdOperatorTable.PI;
      case "POW", "POWER":
        return SqlStdOperatorTable.POWER;
      case "RADIANS":
        return SqlStdOperatorTable.RADIANS;
      case "RAND":
        return SqlStdOperatorTable.RAND;
      case "ROUND":
        return SqlStdOperatorTable.ROUND;
      case "SIGN":
        return SqlStdOperatorTable.SIGN;
      case "SIN":
        return SqlStdOperatorTable.SIN;
      case "SQRT":
        // SqlStdOperatorTable.SQRT is declared but not implemented, therefore we use a custom
        // implementation.
        return TransferUserDefinedFunction(
            SqrtFunction.class, "SQRT", ReturnTypes.DOUBLE_FORCE_NULLABLE);
      case "CBRT":
        return SqlStdOperatorTable.CBRT;
        // Built-in Date Functions
      case "CURRENT_TIMESTAMP", "NOW", "LOCALTIMESTAMP", "LOCALTIME":
        return TransferUserDefinedFunction(
            PostprocessForUDTFunction.class, "POSTPROCESS", timestampInference);
      case "CURTIME", "CURRENT_TIME":
        return TransferUserDefinedFunction(
            PostprocessForUDTFunction.class, "POSTPROCESS", timeInference);
      case "CURRENT_DATE", "CURDATE":
        return TransferUserDefinedFunction(
            PostprocessForUDTFunction.class, "POSTPROCESS", dateInference);
      case "DATE":
        return TransferUserDefinedFunction(DateFunction.class, "DATE", dateInference);
      case "DATE_ADD":
        return TransferUserDefinedFunction(
            DateAddSubFunction.class, "DATE_ADD", timestampInference);
      case "ADDDATE":
        return TransferUserDefinedFunction(
            DateAddSubFunction.class, "ADDDATE", DateAddSubFunction.getReturnTypeForAddOrSubDate());
      case "SUBDATE":
        return TransferUserDefinedFunction(
            DateAddSubFunction.class, "SUBDATE", DateAddSubFunction.getReturnTypeForAddOrSubDate());
      case "DATE_SUB":
        return TransferUserDefinedFunction(
            DateAddSubFunction.class, "DATE_SUB", timestampInference);
      case "ADDTIME", "SUBTIME":
        return TransferUserDefinedFunction(
            TimeAddSubFunction.class, capitalOP, TimeAddSubFunction.getReturnTypeForTimeAddSub());
      case "DAY_OF_WEEK", "DAYOFWEEK":
        return TransferUserDefinedFunction(
            DayOfWeekFunction.class, capitalOP, INTEGER_FORCE_NULLABLE);
      case "DAY_OF_YEAR", "DAYOFYEAR":
        return TransferUserDefinedFunction(
            DayOfYearFunction.class, capitalOP, INTEGER_FORCE_NULLABLE);
      case "EXTRACT":
        // Reuse OpenSearch PPL's implementation
        return TransferUserDefinedFunction(ExtractFunction.class, "EXTRACT", ReturnTypes.BIGINT);
      case "CONVERT_TZ":
        return TransferUserDefinedFunction(
            ConvertTZFunction.class, "CONVERT_TZ", timestampInference);
      case "DATETIME":
        return TransferUserDefinedFunction(DatetimeFunction.class, "DATETIME", timestampInference);

      case "FROM_DAYS":
        return TransferUserDefinedFunction(FromDaysFunction.class, "FROM_DAYS", dateInference);
      case "DATE_FORMAT":
        return TransferUserDefinedFunction(
            DateFormatFunction.class, "DATE_FORMAT", VARCHAR_FORCE_NULLABLE);
      case "GET_FORMAT":
        return TransferUserDefinedFunction(
            GetFormatFunction.class, "GET_FORMAT", VARCHAR_FORCE_NULLABLE);
      case "MAKETIME":
        return TransferUserDefinedFunction(MakeTimeFunction.class, "MAKETIME", timeInference);
      case "MAKEDATE":
        return TransferUserDefinedFunction(MakeDateFunction.class, "MAKEDATE", dateInference);
      case "MINUTE_OF_DAY":
        return TransferUserDefinedFunction(
            MinuteOfDayFunction.class, "MINUTE_OF_DAY", ReturnTypes.INTEGER);
      case "PERIOD_ADD":
        return TransferUserDefinedFunction(
            PeriodAddFunction.class, "PERIOD_ADD", ReturnTypes.INTEGER);
      case "PERIOD_DIFF":
        return TransferUserDefinedFunction(
            PeriodDiffFunction.class, "PERIOD_DIFF", ReturnTypes.INTEGER);
      case "STR_TO_DATE":
        return TransferUserDefinedFunction(
            StrToDateFunction.class, "STR_TO_DATE", timestampInference);
      case "WEEK", "WEEK_OF_YEAR":
        // WEEK in PPL support an additional mode argument, therefore we need to use a custom
        // implementation.
        return TransferUserDefinedFunction(WeekFunction.class, "WEEK", ReturnTypes.INTEGER);
        // UDF Functions
      case "SPAN":
        return TransferUserDefinedFunction(
            SpanFunction.class, "SPAN", ReturnTypes.ARG0_FORCE_NULLABLE);
        // Built-in condition functions
      case "IF":
        return TransferUserDefinedFunction(
            IfFunction.class, "if", ReturnTypes.ARG1.andThen(SqlTypeTransforms.FORCE_NULLABLE));
      case "IFNULL":
        return TransferUserDefinedFunction(
            IfNullFunction.class, "ifnull", ReturnTypes.ARG0_FORCE_NULLABLE);
      case "NULLIF":
        return TransferUserDefinedFunction(
            NullIfFunction.class, "nullif", ReturnTypes.ARG0_FORCE_NULLABLE);
      case "IS NOT NULL":
        return SqlStdOperatorTable.IS_NOT_NULL;
      case "IS NULL":
        return SqlStdOperatorTable.IS_NULL;
      case "TYPEOF":
        // TODO optimize this function to ImplementableFunction
        return TransferUserDefinedFunction(
            TypeOfFunction.class, "typeof", ReturnTypes.VARCHAR_2000_NULLABLE);
      case "DAYNAME":
        return TransferUserDefinedFunction(
            PeriodNameFunction.class, "DAYNAME", VARCHAR_FORCE_NULLABLE);
      case "MONTHNAME":
        return TransferUserDefinedFunction(
            PeriodNameFunction.class, "MONTHNAME", VARCHAR_FORCE_NULLABLE);
      case "LAST_DAY":
        return TransferUserDefinedFunction(LastDayFunction.class, "LAST_DAY", dateInference);
      case "UNIX_TIMESTAMP":
        return TransferUserDefinedFunction(
            UnixTimeStampFunction.class, "unix_timestamp", ReturnTypes.DOUBLE);
      case "SYSDATE":
        return TransferUserDefinedFunction(SysdateFunction.class, "SYSDATE", timestampInference);
      case "TIME":
        return TransferUserDefinedFunction(TimeFunction.class, "TIME", timeInference);
      case "TIMEDIFF":
        return TransferUserDefinedFunction(TimeDiffFunction.class, "TIMEDIFF", timeInference);
      case "TIME_TO_SEC":
        return TransferUserDefinedFunction(
            TimeToSecondFunction.class, "TIME_TO_SEC", ReturnTypes.BIGINT);
      case "TIME_FORMAT":
        return TransferUserDefinedFunction(
            TimeFormatFunction.class, "TIME_FORMAT", VARCHAR_FORCE_NULLABLE);
      case "TIMESTAMP":
        // return SqlLibraryOperators.TIMESTAMP;
        return TransferUserDefinedFunction(
            TimestampFunction.class, "timestamp", timestampInference);
      case "TIMESTAMPADD":
        // return SqlLibraryOperators.TIMESTAMP;
        return TransferUserDefinedFunction(
            TimestampAddFunction.class, "TIMESTAMPADD", timestampInference);
      case "TIMESTAMPDIFF":
        return TransferUserDefinedFunction(
            TimestampDiffFunction.class, "TIMESTAMPDIFF", ReturnTypes.BIGINT);
      case "DATEDIFF":
        return TransferUserDefinedFunction(DateDiffFunction.class, "DATEDIFF", ReturnTypes.BIGINT);
      case "TO_SECONDS":
        return TransferUserDefinedFunction(
            ToSecondsFunction.class, "TO_SECONDS", ReturnTypes.BIGINT);
      case "TO_DAYS":
        return TransferUserDefinedFunction(ToDaysFunction.class, "TO_DAYS", ReturnTypes.BIGINT);
      case "SEC_TO_TIME":
        return TransferUserDefinedFunction(
            SecondToTimeFunction.class, "SEC_TO_TIME", timeInference);
      case "YEAR":
        return TransferUserDefinedFunction(YearFunction.class, capitalOP, INTEGER_FORCE_NULLABLE);
      case "QUARTER":
        return TransferUserDefinedFunction(
            QuarterFunction.class, capitalOP, INTEGER_FORCE_NULLABLE);
      case "MINUTE", "MINUTE_OF_HOUR":
        return TransferUserDefinedFunction(MinuteFunction.class, capitalOP, INTEGER_FORCE_NULLABLE);
      case "HOUR", "HOUR_OF_DAY":
        return TransferUserDefinedFunction(HourFunction.class, capitalOP, INTEGER_FORCE_NULLABLE);
      case "MONTH", "MONTH_OF_YEAR":
        return TransferUserDefinedFunction(MonthFunction.class, capitalOP, INTEGER_FORCE_NULLABLE);
      case "DAY_OF_MONTH", "DAYOFMONTH", "DAY":
        return TransferUserDefinedFunction(DayFunction.class, capitalOP, INTEGER_FORCE_NULLABLE);
      case "SECOND", "SECOND_OF_MINUTE":
        return TransferUserDefinedFunction(SecondFunction.class, capitalOP, INTEGER_FORCE_NULLABLE);
      case "MICROSECOND":
        return TransferUserDefinedFunction(
            MicrosecondFunction.class, "MICROSECOND", ReturnTypes.INTEGER);
      case "YEARWEEK":
        return TransferUserDefinedFunction(YearWeekFunction.class, "YEARWEEK", ReturnTypes.INTEGER);
      case "FROM_UNIXTIME":
        return TransferUserDefinedFunction(
            FromUnixTimestampFunction.class,
            "FROM_UNIXTIME",
            FromUnixTimestampFunction.interReturnTypes());
      case "WEEKDAY":
        return TransferUserDefinedFunction(WeekDayFunction.class, "WEEKDAY", ReturnTypes.INTEGER);
      case "UTC_TIMESTAMP":
        return TransferUserDefinedFunction(
            UtcTimeStampFunction.class, "utc_timestamp", timestampInference);
      case "UTC_TIME":
        return TransferUserDefinedFunction(UtcTimeFunction.class, "utc_time", timeInference);
      case "UTC_DATE":
        return TransferUserDefinedFunction(UtcDateFunction.class, "utc_date", dateInference);
        // TODO Add more, ref RexImpTable
      default:
        throw new IllegalArgumentException("Unsupported operator: " + op);
    }
  }

  /**
   * Translates function arguments to align with Calcite's expectations, ensuring compatibility with
   * PPL (Piped Processing Language). This is necessary because Calcite's input argument order or
   * default values may differ from PPL's function definitions.
   *
   * @param op The function name as a string.
   * @param argList A list of {@link RexNode} representing the parsed arguments from the PPL
   *     statement.
   * @param context The {@link CalcitePlanContext} providing necessary utilities such as {@code
   *     rexBuilder}.
   * @return A modified list of {@link RexNode} that correctly maps to Calcite’s function
   *     expectations.
   */
  static List<RexNode> translateArgument(
      String op, List<RexNode> argList, CalcitePlanContext context, String currentTimestampStr) {
    String capitalOP = op.toUpperCase(Locale.ROOT);
    switch (capitalOP) {
      case "TRIM":
        List<RexNode> trimArgs =
            new ArrayList<>(
                List.of(
                    context.rexBuilder.makeFlag(SqlTrimFunction.Flag.BOTH),
                    context.rexBuilder.makeLiteral(" ")));
        trimArgs.addAll(argList);
        return trimArgs;
      case "LTRIM":
        List<RexNode> LTrimArgs =
            new ArrayList<>(
                List.of(
                    context.rexBuilder.makeFlag(SqlTrimFunction.Flag.LEADING),
                    context.rexBuilder.makeLiteral(" ")));
        LTrimArgs.addAll(argList);
        return LTrimArgs;
      case "RTRIM":
        List<RexNode> RTrimArgs =
            new ArrayList<>(
                List.of(
                    context.rexBuilder.makeFlag(SqlTrimFunction.Flag.TRAILING),
                    context.rexBuilder.makeLiteral(" ")));
        RTrimArgs.addAll(argList);
        return RTrimArgs;
      case "STRCMP":
        List<RexNode> StrcmpArgs = List.of(argList.get(1), argList.get(0));
        return StrcmpArgs;
      case "ATAN":
        List<RexNode> AtanArgs = new ArrayList<>(argList);
        if (AtanArgs.size() == 1) {
          BigDecimal divideNumber = BigDecimal.valueOf(1);
          AtanArgs.add(context.rexBuilder.makeBigintLiteral(divideNumber));
        }
        return AtanArgs;
      case "LOG":
        List<RexNode> LogArgs = new ArrayList<>();
        RelDataTypeFactory typeFactory = context.rexBuilder.getTypeFactory();
        if (argList.size() == 1) {
          LogArgs.add(argList.getFirst());
          LogArgs.add(
              context.rexBuilder.makeExactLiteral(
                  BigDecimal.valueOf(E), typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        } else if (argList.size() == 2) {
          LogArgs.add(argList.get(1));
          LogArgs.add(argList.get(0));
        } else {
          throw new IllegalArgumentException("Log cannot accept argument list: " + argList);
        }
        return LogArgs;
      case "DATE":
        List<RexNode> dateArgs =
            List.of(
                argList.get(0),
                context.rexBuilder.makeFlag(transferDateRelatedTimeName(argList.get(0))),
                context.rexBuilder.makeLiteral(currentTimestampStr));
        return dateArgs;
      case "HOUR",
          "HOUR_OF_DAY",
          "MINUTE",
          "MINUTE_OF_HOUR",
          "QUARTER",
          "YEAR",
          "LAST_DAY",
          "DAY_OF_WEEK",
          "DAYOFWEEK",
          "DAY_OF_YEAR",
          "DAYOFYEAR",
          "MONTH",
          "MONTH_OF_YEAR",
          "DAY",
          "DAY_OF_MONTH",
          "DAYOFMONTH",
          "SECOND",
          "SECOND_OF_MINUTE":
        return List.of(
            argList.get(0),
            context.rexBuilder.makeFlag(transferDateRelatedTimeName(argList.get(0))),
            context.rexBuilder.makeLiteral(currentTimestampStr));
      case "CURRENT_TIMESTAMP", "NOW", "LOCALTIMESTAMP", "LOCALTIME":
        RexNode currentTimestampCall =
            context.rexBuilder.makeCall(SqlStdOperatorTable.CURRENT_TIMESTAMP, List.of());
        return List.of(currentTimestampCall, context.rexBuilder.makeFlag(SqlTypeName.TIMESTAMP));
      case "CURTIME", "CURRENT_TIME":
        RexNode currentTimeCall =
            context.rexBuilder.makeCall(SqlStdOperatorTable.CURRENT_TIME, List.of());
        return List.of(currentTimeCall, context.rexBuilder.makeFlag(SqlTypeName.TIME));
      case "CURRENT_DATE", "CURDATE":
        RexNode currentDateCall =
            context.rexBuilder.makeCall(SqlStdOperatorTable.CURRENT_DATE, List.of());
        return List.of(currentDateCall, context.rexBuilder.makeFlag(SqlTypeName.DATE));
      case "TIMESTAMP",
          "TIMEDIFF",
          "TIME_TO_SEC",
          "TIME_FORMAT",
          "TO_SECONDS",
          "TO_DAYS",
          "CONVERT_TZ":
        List<RexNode> timestampArgs = new ArrayList<>(argList);
        timestampArgs.addAll(
            argList.stream()
                .map(p -> context.rexBuilder.makeFlag(transferDateRelatedTimeName(p)))
                .collect(Collectors.toList()));
        timestampArgs.add(context.rexBuilder.makeLiteral(currentTimestampStr));
        return timestampArgs;
      case "YEARWEEK", "WEEKDAY":
        List<RexNode> weekdayArgs = new ArrayList<>(argList);
        weekdayArgs.addAll(
            argList.stream()
                .map(p -> context.rexBuilder.makeFlag(transferDateRelatedTimeName(p)))
                .collect(Collectors.toList()));
        weekdayArgs.add(context.rexBuilder.makeLiteral(currentTimestampStr));
        return weekdayArgs;
      case "TIMESTAMPADD":
        List<RexNode> timestampAddArgs = new ArrayList<>(argList);
        timestampAddArgs.add(
            context.rexBuilder.makeFlag(argList.get(2).getType().getSqlTypeName()));
        timestampAddArgs.add(context.rexBuilder.makeLiteral(currentTimestampStr));
        return timestampAddArgs;
      case "TIMESTAMPDIFF":
        List<RexNode> timestampDiffArgs = new ArrayList<>();
        timestampDiffArgs.add(argList.getFirst());
        timestampDiffArgs.addAll(buildArgsWithTypes(context.rexBuilder, argList, 1, 2));
        timestampDiffArgs.add(context.rexBuilder.makeLiteral(currentTimestampStr));
        return timestampDiffArgs;
      case "DATEDIFF":
        // datediff differs with timestamp diff in that it
        List<RexNode> dateDiffArgs = buildArgsWithTypes(context.rexBuilder, argList, 0, 1);
        dateDiffArgs.add(context.rexBuilder.makeLiteral(currentTimestampStr));
        return dateDiffArgs;
      case "DAYNAME", "MONTHNAME":
        List<RexNode> periodNameArgs = new ArrayList<>();
        periodNameArgs.add(argList.getFirst());
        periodNameArgs.add(context.rexBuilder.makeLiteral(capitalOP));
        periodNameArgs.add(
            context.rexBuilder.makeFlag(argList.getFirst().getType().getSqlTypeName()));
        return periodNameArgs;
      case "DATE_SUB":
        List<RexNode> dateSubArgs = transformDateManipulationArgs(argList, context.rexBuilder);
        // A flag that represents isAdd
        dateSubArgs.add(context.rexBuilder.makeLiteral(false));
        dateSubArgs.add(context.rexBuilder.makeFlag(SqlTypeName.TIMESTAMP));
        dateSubArgs.add(context.rexBuilder.makeLiteral(currentTimestampStr));
        return dateSubArgs;
      case "DATE_ADD":
        List<RexNode> dateAddArgs = transformDateManipulationArgs(argList, context.rexBuilder);
        dateAddArgs.add(context.rexBuilder.makeLiteral(true));
        dateAddArgs.add(context.rexBuilder.makeFlag(SqlTypeName.TIMESTAMP));
        dateAddArgs.add(context.rexBuilder.makeLiteral(currentTimestampStr));
        return dateAddArgs;
      case "ADDTIME":
        SqlTypeName arg0Type = transferDateRelatedTimeName(argList.getFirst());
        SqlTypeName arg1Type = transferDateRelatedTimeName(argList.get(1));
        RexNode type0 = context.rexBuilder.makeFlag(arg0Type);
        RexNode type1 = context.rexBuilder.makeFlag(arg1Type);
        RexNode isAdd = context.rexBuilder.makeLiteral(true);

        return List.of(
            argList.getFirst(),
            type0,
            argList.get(1),
            type1,
            isAdd,
            context.rexBuilder.makeLiteral(currentTimestampStr));
      case "ADDDATE":
        return transformAddOrSubDateArgs(argList, context.rexBuilder, true, currentTimestampStr);
      case "SUBDATE":
        return transformAddOrSubDateArgs(argList, context.rexBuilder, false, currentTimestampStr);
      case "SUBTIME":
        List<RexNode> subTimeArgs = transformTimeManipulationArgs(argList, context.rexBuilder);
        subTimeArgs.add(context.rexBuilder.makeLiteral(false));
        subTimeArgs.add(context.rexBuilder.makeLiteral(currentTimestampStr));
        return subTimeArgs;
      case "TIME":
        return ImmutableList.of(
            argList.getFirst(),
            context.rexBuilder.makeFlag(transferDateRelatedTimeName(argList.getFirst())));
      case "DATE_FORMAT", "FORMAT_TIMESTAMP":
        RexNode dateExpr = argList.get(0);
        RexNode dateFormatPatternExpr = argList.get(1);
        RexNode datetimeType;
        datetimeType = context.rexBuilder.makeFlag(transferDateRelatedTimeName(dateExpr));
        return ImmutableList.of(
            dateExpr,
            datetimeType,
            dateFormatPatternExpr,
            context.rexBuilder.makeLiteral(currentTimestampStr));
      case "UNIX_TIMESTAMP":
        List<RexNode> unixArgs = new ArrayList<>(argList);
        unixArgs.add(context.rexBuilder.makeFlag(transferDateRelatedTimeName(argList.getFirst())));
        unixArgs.add(context.rexBuilder.makeLiteral(currentTimestampStr));
        return unixArgs;
      case "WEEK", "WEEK_OF_YEAR":
        RexNode woyMode;
        if (argList.size() >= 2) {
          woyMode = argList.get(1);
        } else {
          woyMode =
              context.rexBuilder.makeLiteral(
                  0, context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER));
        }
        return List.of(
            argList.getFirst(),
            woyMode,
            context.rexBuilder.makeFlag(argList.getFirst().getType().getSqlTypeName()));
      case "STR_TO_DATE":
        List<RexNode> strToDateArgs = new ArrayList<>(argList);
        strToDateArgs.add(context.rexBuilder.makeLiteral(currentTimestampStr));
        return strToDateArgs;
      case "MINUTE_OF_DAY", "MICROSECOND":
        // Convert STRING/TIME/TIMESTAMP to TIMESTAMP
        return ImmutableList.of(
            argList.getFirst(),
            context.rexBuilder.makeFlag(transferDateRelatedTimeName(argList.getFirst())));
      case "EXTRACT":
        return ImmutableList.of(
            argList.getFirst(),
            argList.get(1),
            context.rexBuilder.makeFlag(transferDateRelatedTimeName(argList.get(1))),
            context.rexBuilder.makeLiteral(currentTimestampStr));
      case "DATETIME":
        // Convert timestamp to a string to reuse OS PPL V2's implementation
        RexNode argTimestamp = argList.getFirst();
        if (argTimestamp.getType().getSqlTypeName().equals(SqlTypeName.TIMESTAMP)) {
          argTimestamp =
              makeConversionCall(
                  "DATE_FORMAT",
                  ImmutableList.of(argTimestamp, context.rexBuilder.makeLiteral("%Y-%m-%d %T")),
                  context,
                  currentTimestampStr);
        }
        return Stream.concat(Stream.of(argTimestamp), argList.stream().skip(1)).toList();
      case "UTC_TIMESTAMP", "UTC_TIME", "UTC_DATE":
        return List.of(context.rexBuilder.makeLiteral(currentTimestampStr));
      case "TYPEOF":
        return List.of(
            context.rexBuilder.makeLiteral(
                getLegacyTypeName(argList.getFirst().getType(), context.queryType)));
      default:
        return argList;
    }
  }

  private static RexNode makeConversionCall(
      String funcName,
      List<RexNode> arguments,
      CalcitePlanContext context,
      String currentTimestampStr) {
    SqlOperator operator = translate(funcName);
    List<RexNode> translatedArguments =
        translateArgument(funcName, arguments, context, currentTimestampStr);
    RelDataType returnType =
        deriveReturnType(funcName, context.rexBuilder, operator, translatedArguments);
    return context.rexBuilder.makeCall(returnType, operator, translatedArguments);
  }

  static RelDataType deriveReturnType(
      String funcName, RexBuilder rexBuilder, SqlOperator operator, List<? extends RexNode> exprs) {
    RelDataType returnType =
        switch (funcName.toUpperCase(Locale.ROOT)) {
            // This effectively invalidates the operand type check, which leads to unnecessary
            // incompatible parameter type errors
          case "DATEDIFF" -> rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
          case "TIMESTAMPDIFF" -> rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
          case "YEAR",
              "MINUTE",
              "HOUR",
              "HOUR_OF_DAY",
              "MONTH",
              "MONTH_OF_YEAR",
              "DAY_OF_YEAR",
              "DAYOFYEAR",
              "DAY_OF_MONTH",
              "DAYOFMONTH",
              "DAY_OF_WEEK",
              "DAYOFWEEK",
              "DAY",
              "MINUTE_OF_HOUR",
              "QUARTER",
              "SECOND",
              "SECOND_OF_MINUTE" -> rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
          default -> rexBuilder.deriveReturnType(operator, exprs);
        };
    // Make all return types nullable
    return rexBuilder.getTypeFactory().createTypeWithNullability(returnType, true);
  }

  private static List<RexNode> transformDateManipulationArgs(
      List<RexNode> argList, ExtendedRexBuilder rexBuilder) {
    List<RexNode> dateAddArgs = new ArrayList<>();
    RexNode baseTimestampExpr = argList.get(0);
    RexNode intervalExpr = argList.get(1);
    // 1. Add time unit
    RexLiteral timeFrameName =
        rexBuilder.makeFlag(
            Objects.requireNonNull(intervalExpr.getType().getIntervalQualifier()).getUnit());
    dateAddArgs.add(timeFrameName);
    // 2. Add interval
    RexLiteral intervalArg =
        rexBuilder.makeBigintLiteral(((RexLiteral) intervalExpr).getValueAs(BigDecimal.class));
    dateAddArgs.add(intervalArg);
    // 3. Add timestamp
    dateAddArgs.add(baseTimestampExpr);
    // 4. Add original sql type
    dateAddArgs.add(rexBuilder.makeFlag(transferDateRelatedTimeName(baseTimestampExpr)));
    return dateAddArgs;
  }

  private static List<RexNode> transformAddOrSubDateArgs(
      List<RexNode> argList,
      ExtendedRexBuilder rexBuilder,
      Boolean isAdd,
      String currentTimestampStr) {
    List<RexNode> addOrSubDateArgs = new ArrayList<>();
    addOrSubDateArgs.add(argList.getFirst());
    SqlTypeName addType = argList.get(1).getType().getSqlTypeName();
    if (addType == SqlTypeName.BIGINT
        || addType == SqlTypeName.DECIMAL
        || addType == SqlTypeName.INTEGER) {
      Number value = ((RexLiteral) argList.get(1)).getValueAs(Number.class);
      addOrSubDateArgs.add(
          rexBuilder.makeIntervalLiteral(
              new BigDecimal(value.toString()),
              new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO)));
    } else {
      addOrSubDateArgs.add(argList.get(1));
    }
    List<RexNode> addOrSubDateRealInput =
        transformDateManipulationArgs(addOrSubDateArgs, rexBuilder);
    addOrSubDateRealInput.add(rexBuilder.makeLiteral(isAdd));
    SqlTypeName firstType = transferDateRelatedTimeName(argList.getFirst());
    if (firstType == SqlTypeName.DATE
        && (addType == SqlTypeName.BIGINT
            || addType == SqlTypeName.DECIMAL
            || addType == SqlTypeName.INTEGER)) {
      addOrSubDateRealInput.add(rexBuilder.makeFlag(SqlTypeName.DATE));
      addOrSubDateRealInput.add(
          rexBuilder.makeLiteral(0, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DATE)));
    } else {
      addOrSubDateRealInput.add(rexBuilder.makeFlag(SqlTypeName.TIMESTAMP));
      addOrSubDateRealInput.add(
          rexBuilder.makeLiteral(
              0L, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP)));
    }
    addOrSubDateRealInput.add(rexBuilder.makeLiteral(currentTimestampStr));
    return addOrSubDateRealInput;
  }

  private static List<RexNode> transformTimeManipulationArgs(
      List<RexNode> argList, ExtendedRexBuilder rexBuilder) {
    SqlTypeName arg0Type = transferDateRelatedTimeName(argList.getFirst());
    SqlTypeName arg1Type = transferDateRelatedTimeName(argList.get(1));
    RexNode type0 = rexBuilder.makeFlag(arg0Type);
    RexNode type1 = rexBuilder.makeFlag(arg1Type);
    return new ArrayList<>(List.of(argList.getFirst(), type0, argList.get(1), type1));
  }

  /**
   * Builds a list of RexNodes where each selected argument is followed by a RexNode representing
   * its SQL type.
   *
   * @param rexBuilder the RexBuilder instance used to create type flags
   * @param args the original list of arguments
   * @return A new list of RexNodes: [arg, typeFlag, arg, typeFlag, ...]
   */
  private static List<RexNode> buildArgsWithTypes(
      RexBuilder rexBuilder, List<RexNode> args, int... indexes) {
    List<RexNode> result = new ArrayList<>();
    for (int index : indexes) {
      RexNode arg = args.get(index);
      result.add(arg);
      result.add(rexBuilder.makeFlag(transferDateRelatedTimeName(arg)));
    }
    return result;
  }
}
