/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.ExtendedRexBuilder;
import org.opensearch.sql.calcite.udf.conditionUDF.IfFunction;
import org.opensearch.sql.calcite.udf.conditionUDF.IfNullFunction;
import org.opensearch.sql.calcite.udf.conditionUDF.NullIfFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.ConvertTZFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.DateDiffFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.DateFormatFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.DateFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.DatetimeFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.FromDaysFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.FromUnixTimestampFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.GetFormatFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.LastDayFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.MakeDateFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.MakeTimeFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.PeriodAddFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.PeriodDiffFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.PeriodNameFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.PostprocessForUDTFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.SecondToTimeFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.StrToDateFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.SysdateFunction;
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
      case "DATE":
        List<RexNode> dateArgs =
            List.of(
                argList.get(0),
                context.rexBuilder.makeFlag(transferDateRelatedTimeName(argList.get(0))),
                context.rexBuilder.makeLiteral(currentTimestampStr));
        return dateArgs;
      case "YEAR", "LAST_DAY":
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
          case "YEAR" -> rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
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
