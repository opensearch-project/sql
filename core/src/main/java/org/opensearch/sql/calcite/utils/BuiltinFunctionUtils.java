/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static java.lang.Math.E;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.TransferUserDefinedFunction;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.createNullableReturnType;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.getLeastRestrictiveReturnTypeAmongArgsAt;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.getReturnTypeInference;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.transferStringExprToDateValue;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimestampString;
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
import org.opensearch.sql.calcite.udf.datetimeUDF.DatetimeFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.ExtractFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.FromDaysFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.FromUnixTimestampFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.GetFormatFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.MakeDateFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.MakeTimeFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.MinuteOfDay;
import org.opensearch.sql.calcite.udf.datetimeUDF.PeriodAddFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.PeriodDiffFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.PeriodNameFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.SecondToTimeFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.StrToDateFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.SysdateFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimeAddSubFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimeDiffFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimeFormatFunction;
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
import org.opensearch.sql.calcite.udf.datetimeUDF.YearWeekFunction;
import org.opensearch.sql.calcite.udf.mathUDF.CRC32Function;
import org.opensearch.sql.calcite.udf.mathUDF.ConvFunction;
import org.opensearch.sql.calcite.udf.mathUDF.EulerFunction;
import org.opensearch.sql.calcite.udf.mathUDF.ModFunction;
import org.opensearch.sql.calcite.udf.mathUDF.SqrtFunction;
import org.opensearch.sql.calcite.utils.datetime.DateTimeParser;

public interface BuiltinFunctionUtils {

  static SqlOperator translate(String op) {
    switch (op.toUpperCase(Locale.ROOT)) {
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
        return SqlStdOperatorTable.DIVIDE;
        // Built-in String Functions
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
      case "SUBSTRING":
        return SqlStdOperatorTable.SUBSTRING;
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
        return TransferUserDefinedFunction(ConvFunction.class, "CONVERT", ReturnTypes.VARCHAR);
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
        return TransferUserDefinedFunction(
            ModFunction.class,
            "MOD",
            getLeastRestrictiveReturnTypeAmongArgsAt(List.of(0, 1), true));
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
        return SqlStdOperatorTable.CURRENT_TIMESTAMP;
      case "CURTIME", "CURRENT_TIME":
        return SqlStdOperatorTable.CURRENT_TIME;
      case "CURRENT_DATE", "CURDATE":
        return SqlStdOperatorTable.CURRENT_DATE;
      case "DATE":
        return SqlLibraryOperators.DATE;
      case "DATE_ADD":
        return TransferUserDefinedFunction(
            DateAddSubFunction.class, "DATE_ADD", ReturnTypes.TIMESTAMP);
      case "ADDDATE":
        return TransferUserDefinedFunction(
            DateAddSubFunction.class, "ADDDATE", DateAddSubFunction.getReturnTypeForAddOrSubDate());
      case "SUBDATE":
        return TransferUserDefinedFunction(
            DateAddSubFunction.class, "SUBDATE", DateAddSubFunction.getReturnTypeForAddOrSubDate());
      case "DATE_SUB":
        return TransferUserDefinedFunction(
            DateAddSubFunction.class, "DATE_SUB", ReturnTypes.TIMESTAMP);
      case "ADDTIME", "SUBTIME":
        return TransferUserDefinedFunction(
            TimeAddSubFunction.class,
            "ADDTIME",
            UserDefinedFunctionUtils.getReturnTypeForTimeAddSub());
      case "DAY_OF_WEEK", "DAY_OF_YEAR", "DAYOFWEEK", "DAYOFYEAR":
        // SqlStdOperatorTable.DAYOFWEEK, SqlStdOperatorTable.DAYOFYEAR is not implemented in
        // RexImpTable. Therefore, we replace it with their lower-level
        // calls SqlStdOperatorTable.EXTRACT and convert the arguments accordingly.
        return SqlStdOperatorTable.EXTRACT;
      case "EXTRACT":
        // Reuse OpenSearch PPL's implementation
        return TransferUserDefinedFunction(ExtractFunction.class, "EXTRACT", ReturnTypes.BIGINT);
      case "CONVERT_TZ":
        return TransferUserDefinedFunction(
            ConvertTZFunction.class, "CONVERT_TZ", createNullableReturnType(SqlTypeName.TIMESTAMP));
      case "DATETIME":
        return TransferUserDefinedFunction(
            DatetimeFunction.class, "DATETIME", createNullableReturnType(SqlTypeName.TIMESTAMP));

      case "FROM_DAYS":
        return TransferUserDefinedFunction(FromDaysFunction.class, "FROM_DAYS", ReturnTypes.DATE);
      case "DATE_FORMAT":
        return TransferUserDefinedFunction(
            DateFormatFunction.class, "DATE_FORMAT", ReturnTypes.VARCHAR);
      case "GET_FORMAT":
        return TransferUserDefinedFunction(
            GetFormatFunction.class, "GET_FORMAT", ReturnTypes.VARCHAR);
      case "MAKETIME":
        return TransferUserDefinedFunction(MakeTimeFunction.class, "MAKETIME", ReturnTypes.TIME);
      case "MAKEDATE":
        return TransferUserDefinedFunction(
            MakeDateFunction.class, "MAKEDATE", createNullableReturnType(SqlTypeName.DATE));
      case "MINUTE_OF_DAY":
        return TransferUserDefinedFunction(MinuteOfDay.class, "MINUTE_OF_DAY", ReturnTypes.INTEGER);
      case "PERIOD_ADD":
        return TransferUserDefinedFunction(
            PeriodAddFunction.class, "PERIOD_ADD", ReturnTypes.INTEGER);
      case "PERIOD_DIFF":
        return TransferUserDefinedFunction(
            PeriodDiffFunction.class, "PERIOD_DIFF", ReturnTypes.INTEGER);
      case "STR_TO_DATE":
        return TransferUserDefinedFunction(
            StrToDateFunction.class,
            "STR_TO_DATE",
            createNullableReturnType(SqlTypeName.TIMESTAMP));
      case "WEEK", "WEEK_OF_YEAR":
        return TransferUserDefinedFunction(WeekFunction.class, "WEEK", ReturnTypes.INTEGER);
        // UDF Functions
      case "SPAN":
        return TransferUserDefinedFunction(
            SpanFunction.class, "SPAN", ReturnTypes.ARG0_FORCE_NULLABLE);
        // Built-in condition functions
      case "IF":
        return TransferUserDefinedFunction(IfFunction.class, "if", getReturnTypeInference(1));
      case "IFNULL":
        return TransferUserDefinedFunction(
            IfNullFunction.class, "ifnull", getReturnTypeInference(1));
      case "NULLIF":
        return TransferUserDefinedFunction(
            NullIfFunction.class, "ifnull", getReturnTypeInference(0));
      case "IS NOT NULL":
        return SqlStdOperatorTable.IS_NOT_NULL;
      case "IS NULL":
        return SqlStdOperatorTable.IS_NULL;
        // TODO Add more, ref RexImpTable
      case "DAYNAME":
        return TransferUserDefinedFunction(PeriodNameFunction.class, "DAYNAME", ReturnTypes.CHAR);
      case "MONTHNAME":
        return TransferUserDefinedFunction(PeriodNameFunction.class, "MONTHNAME", ReturnTypes.CHAR);
      case "LAST_DAY":
        return SqlStdOperatorTable.LAST_DAY;
      case "UNIX_TIMESTAMP":
        return TransferUserDefinedFunction(
            UnixTimeStampFunction.class, "unix_timestamp", ReturnTypes.DOUBLE);
      case "SYSDATE":
        return TransferUserDefinedFunction(SysdateFunction.class, "SYSDATE", ReturnTypes.TIMESTAMP);
      case "TIME":
        return SqlLibraryOperators.TIME;
      case "TIMEDIFF":
        return TransferUserDefinedFunction(TimeDiffFunction.class, "TIMEDIFF", ReturnTypes.TIME);
      case "TIME_TO_SEC":
        return TransferUserDefinedFunction(
            TimeToSecondFunction.class, "TIME_TO_SEC", ReturnTypes.BIGINT);
      case "TIME_FORMAT":
        return TransferUserDefinedFunction(
            TimeFormatFunction.class, "TIME_FORMAT", ReturnTypes.CHAR);
      case "TIMESTAMP":
        // return SqlLibraryOperators.TIMESTAMP;
        return TransferUserDefinedFunction(
            TimestampFunction.class, "timestamp", ReturnTypes.TIMESTAMP);
      case "TIMESTAMPADD":
        // return SqlLibraryOperators.TIMESTAMP;
        return TransferUserDefinedFunction(
            TimestampAddFunction.class, "TIMESTAMPADD", ReturnTypes.TIMESTAMP);
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
            SecondToTimeFunction.class, "SEC_TO_TIME", ReturnTypes.TIME);
      case "YEAR",
          "QUARTER",
          "MINUTE",
          "HOUR",
          "HOUR_OF_DAY",
          "MONTH",
          "MONTH_OF_YEAR",
          "DAY_OF_MONTH",
          "DAYOFMONTH",
          "DAY",
          "MINUTE_OF_HOUR",
          "SECOND",
          "SECOND_OF_MINUTE":
        return SqlLibraryOperators.DATE_PART;
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
            UtcTimeStampFunction.class, "utc_timestamp", ReturnTypes.TIMESTAMP);
      case "UTC_TIME":
        return TransferUserDefinedFunction(UtcTimeFunction.class, "utc_time", ReturnTypes.TIME);
      case "UTC_DATE":
        return TransferUserDefinedFunction(UtcDateFunction.class, "utc_date", ReturnTypes.DATE);
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
      String op, List<RexNode> argList, CalcitePlanContext context) {
    switch (op.toUpperCase(Locale.ROOT)) {
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
        List<RexNode> DateArgs = new ArrayList<>();
        RexNode timestampExpr = argList.get(0);
        if (timestampExpr instanceof RexLiteral) {
          RexLiteral dateLiteral = (RexLiteral) timestampExpr;
          String dateStringValue = dateLiteral.getValueAs(String.class);
          List<Integer> dateValueList = transferStringExprToDateValue(dateStringValue);
          DateArgs.add(
              context.rexBuilder.makeLiteral(
                  dateValueList.get(0),
                  context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)));
          DateArgs.add(
              context.rexBuilder.makeLiteral(
                  dateValueList.get(1),
                  context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)));
          DateArgs.add(
              context.rexBuilder.makeLiteral(
                  dateValueList.get(2),
                  context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)));
        } else {
          DateArgs.add(timestampExpr);
        }
        return DateArgs;
      case "LAST_DAY":
        List<RexNode> LastDateArgs = new ArrayList<>();
        RexNode lastDayTimestampExpr = argList.get(0);
        if (lastDayTimestampExpr instanceof RexLiteral) {
          RexLiteral dateLiteral = (RexLiteral) lastDayTimestampExpr;
          String dateStringValue = dateLiteral.getValueAs(String.class);
          List<Integer> dateValues = transferStringExprToDateValue(dateStringValue);
          DateString dateString =
              new DateString(dateValues.get(0), dateValues.get(1), dateValues.get(2));
          RexNode dateNode = context.rexBuilder.makeDateLiteral(dateString);
          LastDateArgs.add(dateNode);
        } else {
          LastDateArgs.add(lastDayTimestampExpr);
        }
        return LastDateArgs;
      case "TIMESTAMP",
          "TIMEDIFF",
          "TIME_TO_SEC",
          "TIME_FORMAT",
          "YEARWEEK",
          "WEEKDAY",
          "TO_SECONDS",
          "TO_DAYS":
        List<RexNode> timestampArgs = new ArrayList<>(argList);
        timestampArgs.addAll(
            argList.stream()
                .map(p -> context.rexBuilder.makeFlag(p.getType().getSqlTypeName()))
                .collect(Collectors.toList()));
        return timestampArgs;
      case "TIMESTAMPADD":
        List<RexNode> timestampAddArgs = new ArrayList<>(argList);
        timestampAddArgs.add(
            context.rexBuilder.makeFlag(argList.get(2).getType().getSqlTypeName()));
        return timestampAddArgs;
      case "TIMESTAMPDIFF":
        List<RexNode> timestampDiffArgs = new ArrayList<>();
        timestampDiffArgs.add(argList.getFirst());
        timestampDiffArgs.addAll(buildArgsWithTypes(context.rexBuilder, argList, 1, 2));
        return timestampDiffArgs;
      case "DATEDIFF":
        // datediff differs with timestamp diff in that it
        return buildArgsWithTypes(context.rexBuilder, argList, 0, 1);
      case "DAYNAME", "MONTHNAME":
        List<RexNode> periodNameArgs = new ArrayList<>();
        periodNameArgs.add(argList.getFirst());
        periodNameArgs.add(context.rexBuilder.makeLiteral(op.toUpperCase(Locale.ROOT)));
        periodNameArgs.add(
            context.rexBuilder.makeFlag(argList.getFirst().getType().getSqlTypeName()));
        return periodNameArgs;
      case "YEAR",
          "QUARTER",
          "MINUTE",
          "HOUR",
          "HOUR_OF_DAY",
          "DAY",
          "DAY_OF_MONTH",
          "DAYOFMONTH",
          "MONTH",
          "MONTH_OF_YEAR",
          "SECOND",
          "SECOND_OF_MINUTE",
          "MINUTE_OF_HOUR":
        List<RexNode> extractArgs = new ArrayList<>();
        TimeUnitRange timeUnitRange;
        if (op.equalsIgnoreCase("MINUTE_OF_HOUR")) {
          timeUnitRange = TimeUnitRange.MINUTE;
        } else if (op.equalsIgnoreCase("SECOND_OF_MINUTE")) {
          timeUnitRange = TimeUnitRange.SECOND;
        } else if (op.equalsIgnoreCase("DAY_OF_MONTH") || op.equalsIgnoreCase("DAYOFMONTH")) {
          timeUnitRange = TimeUnitRange.DAY;
        } else if (op.equalsIgnoreCase("HOUR_OF_DAY")) {
          timeUnitRange = TimeUnitRange.HOUR;
        } else if (op.equalsIgnoreCase("MONTH_OF_YEAR")) {
          timeUnitRange = TimeUnitRange.MONTH;
        } else {
          timeUnitRange = TimeUnitRange.valueOf(op.toUpperCase(Locale.ROOT));
        }
        extractArgs.add(context.rexBuilder.makeFlag(timeUnitRange));
        if (argList.getFirst() instanceof RexLiteral) {
          Object stringExpr = ((RexLiteral) argList.getFirst()).getValue();
          if (!(argList.getFirst().getType().getSqlTypeName() == SqlTypeName.CHAR
              || argList.getFirst().getType().getSqlTypeName() == SqlTypeName.VARCHAR)) {
            throw new IllegalArgumentException(
                op + " need first argument string/datetime/time/timestamp");
          }
          String expression;
          if (stringExpr instanceof NlsString) {
            expression = ((NlsString) stringExpr).getValue();
          } else {
            expression = Objects.requireNonNull(stringExpr).toString();
          }
          extractArgs.add(
              context.rexBuilder.makeTimestampLiteral(
                  createTimestampString(DateTimeParser.parse(expression)),
                  RelDataType.PRECISION_NOT_SPECIFIED));
        } else {
          extractArgs.add(argList.getFirst());
        }
        return extractArgs;
      case "DATE_SUB":
        List<RexNode> dateSubArgs = transformDateManipulationArgs(argList, context.rexBuilder);
        // A flag that represents isAdd
        dateSubArgs.add(context.rexBuilder.makeLiteral(false));
        dateSubArgs.add(context.rexBuilder.makeFlag(SqlTypeName.TIMESTAMP));
        return dateSubArgs;
      case "DATE_ADD":
        List<RexNode> dateAddArgs = transformDateManipulationArgs(argList, context.rexBuilder);
        dateAddArgs.add(context.rexBuilder.makeLiteral(true));
        dateAddArgs.add(context.rexBuilder.makeFlag(SqlTypeName.TIMESTAMP));
        return dateAddArgs;
      case "ADDTIME":
        SqlTypeName arg0Type = argList.getFirst().getType().getSqlTypeName();
        SqlTypeName arg1Type = argList.get(1).getType().getSqlTypeName();
        RexNode type0 = context.rexBuilder.makeFlag(arg0Type);
        RexNode type1 = context.rexBuilder.makeFlag(arg1Type);
        RexNode isAdd = context.rexBuilder.makeLiteral(true);
        return List.of(argList.getFirst(), type0, argList.get(1), type1, isAdd);
      case "ADDDATE":
        return transformAddOrSubDateArgs(argList, context.rexBuilder, true);
      case "SUBDATE":
        return transformAddOrSubDateArgs(argList, context.rexBuilder, false);
      case "SUBTIME":
        List<RexNode> subTimeArgs = transformTimeManipulationArgs(argList, context.rexBuilder);
        subTimeArgs.add(context.rexBuilder.makeLiteral(false));
        return subTimeArgs;
      case "TIME":
        List<RexNode> timeArgs = new ArrayList<>();
        RexNode timeExpr = argList.getFirst();
        RexNode timeNode;
        if (timeExpr instanceof RexLiteral timeLiteral) {
          // Convert time string to milliseconds that can be recognized by the builtin TIME function
          String timeStringValue = Objects.requireNonNull(timeLiteral.getValueAs(String.class));
          LocalDateTime dateTime = DateTimeParser.parse(timeStringValue);
          timeNode =
              context.rexBuilder.makeBigintLiteral(
                  BigDecimal.valueOf(dateTime.toInstant(ZoneOffset.UTC).toEpochMilli()));
          timeArgs.add(timeNode);
        }
        // Convert date to timestamp
        else if (timeExpr.getType().getSqlTypeName().equals(SqlTypeName.DATE)) {
          timeNode =
              context.rexBuilder.makeCall(
                  TransferUserDefinedFunction(
                      TimestampFunction.class, "timestamp", ReturnTypes.TIMESTAMP),
                  translateArgument("TIMESTAMP", ImmutableList.of(timeExpr), context));
        } else {
          timeNode = timeExpr;
        }
        return ImmutableList.of(timeNode);
      case "DATE_FORMAT", "FORMAT_TIMESTAMP":
        RexNode dateExpr = argList.get(0);
        RexNode dateFormatPatternExpr = argList.get(1);
        RexNode datetimeNode;
        RexNode datetimeType;
        // Convert to timestamp if is string
        if (dateExpr instanceof RexLiteral dateLiteral) {
          String dateStringValue = Objects.requireNonNull(dateLiteral.getValueAs(String.class));
          datetimeNode =
              context.rexBuilder.makeTimestampLiteral(
                  createTimestampString(DateTimeParser.parse(dateStringValue)), 6);
          datetimeType = context.rexBuilder.makeFlag(SqlTypeName.TIMESTAMP);
        } else {
          datetimeNode = dateExpr;
          datetimeType = context.rexBuilder.makeFlag(dateExpr.getType().getSqlTypeName());
        }
        return ImmutableList.of(datetimeNode, datetimeType, dateFormatPatternExpr);
      case "UNIX_TIMESTAMP":
        List<RexNode> UnixArgs = new ArrayList<>(argList);
        UnixArgs.add(context.rexBuilder.makeFlag(argList.getFirst().getType().getSqlTypeName()));
        return UnixArgs;
      case "DAY_OF_WEEK", "DAYOFWEEK":
        RexNode dowUnit =
            context.rexBuilder.makeIntervalLiteral(
                new SqlIntervalQualifier(TimeUnit.DOW, null, SqlParserPos.ZERO));
        return List.of(
            dowUnit, convertToDateLiteralIfString(context.rexBuilder, argList.getFirst()));
      case "DAY_OF_YEAR", "DAYOFYEAR":
        RexNode domUnit =
            context.rexBuilder.makeIntervalLiteral(
                new SqlIntervalQualifier(TimeUnit.DOY, null, SqlParserPos.ZERO));
        return List.of(
            domUnit, convertToDateLiteralIfString(context.rexBuilder, argList.getFirst()));
      case "WEEK", "WEEK_OF_YEAR":
        RexNode timestamp =
            makeConversionCall("TIMESTAMP", ImmutableList.of(argList.getFirst()), context);
        RexNode woyMode;
        if (argList.size() >= 2) {
          woyMode = argList.get(1);
        } else {
          woyMode =
              context.rexBuilder.makeLiteral(
                  0, context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER));
        }
        return List.of(timestamp, woyMode);
      case "MINUTE_OF_DAY":
        // Convert STRING/TIME/TIMESTAMP to TIMESTAMP
        return ImmutableList.of(
            makeConversionCall("TIMESTAMP", ImmutableList.of(argList.getFirst()), context));
      case "EXTRACT":
        // Convert the second argument to TIMESTAMP
        return ImmutableList.of(
            argList.getFirst(),
            makeConversionCall("TIMESTAMP", ImmutableList.of(argList.get(1)), context));
      case "CONVERT_TZ":
        return ImmutableList.of(
            makeConversionCall("TIMESTAMP", ImmutableList.of(argList.getFirst()), context),
            argList.get(1),
            argList.get(2));
      case "DATETIME":
        // Convert timestamp to a string to reuse OS PPL V2's implementation
        RexNode argTimestamp = argList.getFirst();
        if (argTimestamp.getType().getSqlTypeName().equals(SqlTypeName.TIMESTAMP)) {
          argTimestamp =
              makeConversionCall(
                  "DATE_FORMAT",
                  ImmutableList.of(argTimestamp, context.rexBuilder.makeLiteral("%Y-%m-%d %T")),
                  context);
        }
        return Stream.concat(Stream.of(argTimestamp), argList.stream().skip(1)).toList();
      default:
        return argList;
    }
  }

  private static RexNode makeConversionCall(
      String funcName, List<RexNode> arguments, CalcitePlanContext context) {
    SqlOperator operator = translate(funcName);
    List<RexNode> translatedArguments = translateArgument(funcName, arguments, context);
    return context.rexBuilder.makeCall(operator, translatedArguments);
  }

  static RelDataType deriveReturnType(
      String funcName, RexBuilder rexBuilder, SqlOperator operator, List<? extends RexNode> exprs) {
    return switch (funcName.toUpperCase()) {
        // This effectively invalidates the operand type check, which leads to unnecessary
        // incompatible parameter type errors
      case "DATEDIFF" -> rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
      case "TIMESTAMPADD" -> rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);
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
  }

  private static RexNode convertToDateIfNecessary(CalcitePlanContext context, RexNode expr) {
    if (!expr.getType().getSqlTypeName().equals(SqlTypeName.DATE)) {
      return makeConversionCall("DATE", ImmutableList.of(expr), context);
    }
    return expr;
  }

  private static RexNode convertToDateLiteralIfString(
      RexBuilder rexBuilder, RexNode dateOrTimestampExpr) {
    if (dateOrTimestampExpr instanceof RexLiteral dateLiteral) {
      String dateStringValue = Objects.requireNonNull(dateLiteral.getValueAs(String.class));
      List<Integer> dateValues = transferStringExprToDateValue(dateStringValue);
      DateString dateString =
          new DateString(dateValues.get(0), dateValues.get(1), dateValues.get(2));
      return rexBuilder.makeDateLiteral(dateString);
    }
    return dateOrTimestampExpr;
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
    if (baseTimestampExpr instanceof RexLiteral dateLiteral) {
      String dateStringValue = Objects.requireNonNull(dateLiteral.getValueAs(String.class));
      LocalDateTime dateTime = Objects.requireNonNull(DateTimeParser.parse(dateStringValue));
      TimestampString timestampString = createTimestampString(dateTime);
      RexNode timestampNode =
          rexBuilder.makeTimestampLiteral(timestampString, RelDataType.PRECISION_NOT_SPECIFIED);
      dateAddArgs.add(timestampNode);
      // 4. Add timestamp type
      dateAddArgs.add(rexBuilder.makeFlag(SqlTypeName.TIMESTAMP));
    } else {
      dateAddArgs.add(baseTimestampExpr);
      // 4. Add original sql type
      dateAddArgs.add(rexBuilder.makeFlag(baseTimestampExpr.getType().getSqlTypeName()));
    }
    return dateAddArgs;
  }

  private static List<RexNode> transformAddOrSubDateArgs(
      List<RexNode> argList, ExtendedRexBuilder rexBuilder, Boolean isAdd) {
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
    if (argList.getFirst().getType().getSqlTypeName() == SqlTypeName.DATE
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
    return addOrSubDateRealInput;
  }

  private static List<RexNode> transformTimeManipulationArgs(
      List<RexNode> argList, ExtendedRexBuilder rexBuilder) {
    SqlTypeName arg0Type = argList.getFirst().getType().getSqlTypeName();
    SqlTypeName arg1Type = argList.get(1).getType().getSqlTypeName();
    RexNode type0 = rexBuilder.makeFlag(arg0Type);
    RexNode type1 = rexBuilder.makeFlag(arg1Type);
    return new ArrayList<>(List.of(argList.getFirst(), type0, argList.get(1), type1));
  }

  private static TimestampString createTimestampString(LocalDateTime dateTime) {
    return new TimestampString(
            dateTime.getYear(),
            dateTime.getMonthValue(),
            dateTime.getDayOfMonth(),
            dateTime.getHour(),
            dateTime.getMinute(),
            dateTime.getSecond())
        .withNanos(dateTime.getNano());
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
      result.add(rexBuilder.makeFlag(arg.getType().getSqlTypeName()));
    }
    return result;
  }
}
