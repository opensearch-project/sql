/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static java.lang.Math.E;
import static org.opensearch.sql.calcite.utils.UserDefineFunctionUtils.TransferUserDefinedFunction;
import static org.opensearch.sql.calcite.utils.UserDefineFunctionUtils.createNullableReturnType;
import static org.opensearch.sql.calcite.utils.UserDefineFunctionUtils.transferStringExprToDateValue;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.ExtendedRexBuilder;
import org.opensearch.sql.calcite.udf.conditionUDF.IfFunction;
import org.opensearch.sql.calcite.udf.conditionUDF.IfNullFunction;
import org.opensearch.sql.calcite.udf.conditionUDF.NullIfFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.ConvertTZFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.DateAddSubFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.DateFormatFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.DatetimeFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.ExtractFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.FromDaysFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.GetFormatFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.MakeTimeFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.PeriodAddFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.PeriodDiffFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.StrToDateFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.TimeAddSubFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.UnixTimeStampFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.UtcDateFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.UtcTimeFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.UtcTimeStampFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.WeekFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.fromUnixTimestampFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.periodNameFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.timestampFunction;
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
        return SqlStdOperatorTable.BIT_XOR;
      case "=":
        return SqlStdOperatorTable.EQUALS;
      case "<>":
      case "!=":
        return SqlStdOperatorTable.NOT_EQUALS;
      case ">":
        return SqlStdOperatorTable.GREATER_THAN;
      case ">=":
        return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
      case "<":
        return SqlStdOperatorTable.LESS_THAN;
      case "<=":
        return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
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
      case "SQRT":
        return TransferUserDefinedFunction(
            SqrtFunction.class, "SQRT", ReturnTypes.DOUBLE_FORCE_NULLABLE);
      case "ATAN", "ATAN2":
        return SqlStdOperatorTable.ATAN2;
      case "POW", "POWER":
        return SqlStdOperatorTable.POWER;
        // Built-in Date Functions
      case "CURRENT_TIMESTAMP", "NOW":
        return SqlStdOperatorTable.CURRENT_TIMESTAMP;
      case "CURRENT_DATE", "CURDATE":
        return SqlStdOperatorTable.CURRENT_DATE;
      case "DATE":
        return SqlLibraryOperators.DATE;
      case "ADDDATE":
        return SqlLibraryOperators.DATE_ADD_SPARK;
      case "DATE_ADD":
        return TransferUserDefinedFunction(
            DateAddSubFunction.class, "DATE_ADD", ReturnTypes.TIMESTAMP);
      case "DATE_SUB":
        return TransferUserDefinedFunction(
            DateAddSubFunction.class, "DATE_SUB", ReturnTypes.TIMESTAMP);
      case "ADDTIME", "SUBTIME":
        return TransferUserDefinedFunction(
            TimeAddSubFunction.class,
            "ADDTIME",
            UserDefineFunctionUtils.getReturnTypeForTimeAddSub());
      case "DAY_OF_WEEK", "DAY_OF_YEAR":
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
      case "DATEDIFF":
        return SqlStdOperatorTable.TIMESTAMP_DIFF;
      case "DATE_FORMAT":
        return TransferUserDefinedFunction(
            DateFormatFunction.class, "DATE_FORMAT", ReturnTypes.VARCHAR);
      case "GET_FORMAT":
        return TransferUserDefinedFunction(
            GetFormatFunction.class, "GET_FORMAT", ReturnTypes.VARCHAR);
      case "MAKETIME":
        return TransferUserDefinedFunction(MakeTimeFunction.class, "MAKETIME", ReturnTypes.TIME);
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
        // Built-in condition functions
      case "IF":
        return TransferUserDefinedFunction(
            IfFunction.class, "if", UserDefineFunctionUtils.getReturnTypeInference(1));
      case "IFNULL":
        return TransferUserDefinedFunction(
            IfNullFunction.class, "ifnull", UserDefineFunctionUtils.getReturnTypeInference(1));
      case "NULLIF":
        return TransferUserDefinedFunction(
            NullIfFunction.class, "ifnull", UserDefineFunctionUtils.getReturnTypeInference(0));
      case "IS NOT NULL":
        return SqlStdOperatorTable.IS_NOT_NULL;
      case "IS NULL":
        return SqlStdOperatorTable.IS_NULL;
        // TODO Add more, ref RexImpTable
      case "DAYNAME":
        return TransferUserDefinedFunction(periodNameFunction.class, "DAYNAME", ReturnTypes.CHAR);
      case "MONTHNAME":
        return TransferUserDefinedFunction(periodNameFunction.class, "MONTHNAME", ReturnTypes.CHAR);
      case "LAST_DAY":
        return SqlStdOperatorTable.LAST_DAY;
      case "UNIX_TIMESTAMP":
        return TransferUserDefinedFunction(
            UnixTimeStampFunction.class, "unix_timestamp", ReturnTypes.DOUBLE);
      case "TIME":
        return SqlLibraryOperators.TIME;
      case "TIMESTAMP":
        // return SqlLibraryOperators.TIMESTAMP;
        return TransferUserDefinedFunction(
            timestampFunction.class, "timestamp", ReturnTypes.TIMESTAMP);
      case "YEAR", "MINUTE", "HOUR":
        return SqlLibraryOperators.DATE_PART;
      case "FROM_UNIXTIME":
        return TransferUserDefinedFunction(
            fromUnixTimestampFunction.class,
            "FROM_UNIXTIME",
            fromUnixTimestampFunction.interReturnTypes());
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
      case "TIMESTAMP":
        List<RexNode> timestampArgs = new ArrayList<>(argList);
        timestampArgs.addAll(
            argList.stream()
                .map(p -> context.rexBuilder.makeFlag(p.getType().getSqlTypeName()))
                .collect(Collectors.toList()));
        return timestampArgs;
      case "DAYNAME", "MONTHNAME":
        List<RexNode> periodNameArgs = new ArrayList<>();
        periodNameArgs.add(argList.getFirst());
        periodNameArgs.add(context.rexBuilder.makeLiteral(op));
        periodNameArgs.add(
            context.rexBuilder.makeFlag(argList.getFirst().getType().getSqlTypeName()));
        return periodNameArgs;
      case "YEAR", "MINUTE", "HOUR", "DAY":
        List<RexNode> extractArgs = new ArrayList<>();
        extractArgs.add(context.rexBuilder.makeLiteral(op));
        extractArgs.add(argList.getFirst());
        return extractArgs;
      case "DATE_SUB":
        List<RexNode> dateSubArgs = transformDateManipulationArgs(argList, context.rexBuilder);
        // A flag that represents isAdd
        dateSubArgs.add(context.rexBuilder.makeLiteral(false));
        return dateSubArgs;
      case "DATE_ADD":
        List<RexNode> dateAddArgs = transformDateManipulationArgs(argList, context.rexBuilder);
        dateAddArgs.add(context.rexBuilder.makeLiteral(true));
        return dateAddArgs;
      case "ADDTIME":
        List<RexNode> addTimeArgs = transformTimeManipulationArgs(argList, context.rexBuilder);
        addTimeArgs.add(context.rexBuilder.makeLiteral(true));
        return addTimeArgs;
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
                      timestampFunction.class, "timestamp", ReturnTypes.TIMESTAMP),
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
              context.rexBuilder.makeTimestampLiteral(new TimestampString(dateStringValue), 6);
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
      case "DAY_OF_WEEK":
        RexNode dowUnit =
            context.rexBuilder.makeIntervalLiteral(
                new SqlIntervalQualifier(TimeUnit.DOW, null, SqlParserPos.ZERO));
        return List.of(
            dowUnit, convertToDateLiteralIfString(context.rexBuilder, argList.getFirst()));
      case "DAY_OF_YEAR":
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
      case "DATEDIFF":
        RexNode dayUnit = context.rexBuilder.makeLiteral(TimeUnit.DAY.toString());
        RexNode ts1 = convertToDateIfNecessary(context.rexBuilder, argList.getFirst());
        RexNode ts2 = convertToDateIfNecessary(context.rexBuilder, argList.get(1));
        // TimeFrameSet.diffDate calculates difference as op2 - op1.
        // See
        // @link{https://github.com/apache/calcite/blob/618e601b136e92db933523f77dd7af3c1dfe2779/core/src/main/java/org/apache/calcite/runtime/SqlFunctions.java#L5746}
        return ImmutableList.of(dayUnit, ts2, ts1);
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
    return switch (funcName) {
        // This effectively invalidates the operand type check, which leads to unnecessary
        // incompatible parameter type errors
      case "DATEDIFF" -> rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
      default -> rexBuilder.deriveReturnType(operator, exprs);
    };
  }

  private static RexNode convertToDateIfNecessary(RexBuilder rexBuilder, RexNode expr) {
    if (!expr.getType().getSqlTypeName().equals(SqlTypeName.DATE)) {
      return rexBuilder.makeCall(SqlLibraryOperators.DATE, ImmutableList.of(expr));
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
      TimestampString timestampString =
          new TimestampString(
                  dateTime.getYear(),
                  dateTime.getMonthValue(),
                  dateTime.getDayOfMonth(),
                  dateTime.getHour(),
                  dateTime.getMinute(),
                  dateTime.getSecond())
              .withNanos(dateTime.getNano());
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

  private static List<RexNode> transformTimeManipulationArgs(
      List<RexNode> argList, ExtendedRexBuilder rexBuilder) {
    SqlTypeName arg0Type = argList.getFirst().getType().getSqlTypeName();
    SqlTypeName arg1Type = argList.get(1).getType().getSqlTypeName();
    RexNode type0 = rexBuilder.makeFlag(arg0Type);
    RexNode type1 = rexBuilder.makeFlag(arg1Type);
    return new ArrayList<>(List.of(argList.getFirst(), type0, argList.get(1), type1));
  }
}
