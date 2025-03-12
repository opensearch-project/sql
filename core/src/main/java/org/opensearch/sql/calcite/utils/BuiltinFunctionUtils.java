/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static java.lang.Math.E;
import static org.opensearch.sql.calcite.utils.UserDefineFunctionUtils.transferStringExprToDateValue;
import static org.opensearch.sql.calcite.utils.UserDefineFunctionUtils.TransferUserDefinedFunction;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.apache.calcite.avatica.util.TimeUnit;
import java.util.stream.Collectors;

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
import org.opensearch.sql.calcite.udf.datetimeUDF.UnixTimeStampFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.fromUnixTimestampFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.periodNameFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.timestampFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.UtcDateFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.UtcTimeFunction;
import org.opensearch.sql.calcite.udf.datetimeUDF.UtcTimeStampFunction;
import org.opensearch.sql.calcite.udf.mathUDF.CRC32Function;
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
        // Built-in condition Functions
        // case "IFNULL":
        //  return SqlLibraryOperators.IFNULL;
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
        return SqlStdOperatorTable.CONVERT;
      case "COS":
        return SqlStdOperatorTable.COS;
      case "COT":
        return SqlStdOperatorTable.COT;
      case "CRC32":
        return TransferUserDefinedFunction(CRC32Function.class, "crc32", ReturnTypes.BIGINT);
      case "DEGREES":
        return SqlStdOperatorTable.DEGREES;
      case "E":
        return TransferUserDefinedFunction(EulerFunction.class, "e", ReturnTypes.DOUBLE);
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
      case "MOD":
        return TransferUserDefinedFunction(ModFunction.class, "mod", ReturnTypes.DOUBLE);
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
        return TransferUserDefinedFunction(SqrtFunction.class, "sqrt", ReturnTypes.DOUBLE);
      case "DATE_FORMAT":
        return SqlLibraryOperators.FORMAT_TIMESTAMP;
      case "CBRT":
        return SqlStdOperatorTable.CBRT;
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
      case "ADDTIME":
        return TransferUserDefinedFunction(
            TimeAddSubFunction.class,
            "ADDTIME",
            UserDefineFunctionUtils.getReturnTypeForTimeAddSub());
      case "DAY_OF_WEEK", "DAY_OF_YEAR":
        // SqlStdOperatorTable.DAYOFWEEK, SqlStdOperatorTable.DAYOFYEAR is not implemented in
        // RexTableImpl. Therefore, we replace it with their lower-level
        // calls SqlStdOperatorTable.EXTRACT and convert the arguments accordingly.
        return SqlStdOperatorTable.EXTRACT;
        // TODO Add more, ref RexImpTable
      case "DAYNAME":
        return TransferUserDefinedFunction(periodNameFunction.class, "DAYNAME", ReturnTypes.CHAR);
      case "MONTHNAME":
        return TransferUserDefinedFunction(periodNameFunction.class, "MONTHNAME", ReturnTypes.CHAR);
      case "LAST_DAY":
        return SqlStdOperatorTable.LAST_DAY;
      case "UNIX_TIMESTAMP":
        return TransferUserDefinedFunction(UnixTimeStampFunction.class, "unix_timestamp", ReturnTypes.DOUBLE);
      case "TIMESTAMP":
        //return SqlLibraryOperators.TIMESTAMP;
        return TransferUserDefinedFunction(timestampFunction.class, "timestamp", ReturnTypes.TIMESTAMP);
        //        return SqlLibraryOperators.TIMESTAMP;
        return TransferUserDefinedFunction(
            timestampFunction.class, "timestamp", ReturnTypes.TIMESTAMP);
      case "WEEK", "YEAR", "MINUTE", "HOUR":
        return SqlLibraryOperators.DATE_PART;
      case "FROM_UNIXTIME":
        return TransferUserDefinedFunction(fromUnixTimestampFunction.class, "FROM_UNIXTIME", fromUnixTimestampFunction.interReturnTypes());
      case "UTC_TIMESTAMP":
        return TransferUserDefinedFunction(UtcTimeStampFunction.class, "utc_timestamp", ReturnTypes.TIMESTAMP);
      case "UTC_TIME":
        return TransferUserDefinedFunction(UtcTimeFunction.class, "utc_time", ReturnTypes.TIME);
      case "UTC_DATE":
        return TransferUserDefinedFunction(UtcDateFunction.class, "utc_date", ReturnTypes.DATE);
      default:
        throw new IllegalArgumentException("Unsupported operator: " + op);
    }
  }

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
          DateString dateString = new DateString(dateValues.get(0), dateValues.get(1), dateValues.get(2));
          RexNode dateNode = context.rexBuilder.makeDateLiteral(dateString);
          LastDateArgs.add(dateNode);
        }
        else {
          LastDateArgs.add(lastDayTimestampExpr);
        }
        return LastDateArgs;
      case "TIMESTAMP":
        List<RexNode> timestampArgs = new ArrayList<>(argList);
        timestampArgs.addAll(argList.stream().
                map(p -> context.rexBuilder.makeFlag(p.getType().getSqlTypeName())).collect(Collectors.toList()));
        return timestampArgs;
      case "DAYNAME", "MONTHNAME":
        List<RexNode> periodNameArgs = new ArrayList<>();
        periodNameArgs.add(argList.getFirst());
        periodNameArgs.add(context.rexBuilder.makeLiteral(op));
        periodNameArgs.add(context.rexBuilder.makeFlag(argList.getFirst().getType().getSqlTypeName()));
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
        SqlTypeName arg0Type = argList.getFirst().getType().getSqlTypeName();
        SqlTypeName arg1Type = argList.get(1).getType().getSqlTypeName();
        RexNode type0 = context.rexBuilder.makeFlag(arg0Type);
        RexNode type1 = context.rexBuilder.makeFlag(arg1Type);
        RexNode isAdd = context.rexBuilder.makeLiteral(true);
        return List.of(argList.getFirst(), type0, argList.get(1), type1, isAdd);
      case "TIME":
        List<RexNode> timeArgs = new ArrayList<>();
        RexNode timeExpr = argList.getFirst();
        if (timeExpr instanceof RexLiteral timeLiteral) {
          // Convert time string to milliseconds that can be recognized by the builtin TIME function
          String timeStringValue = Objects.requireNonNull(timeLiteral.getValueAs(String.class));
          LocalDateTime dateTime = DateTimeParser.parse(timeStringValue);
          RexNode timestampNode =
              context.rexBuilder.makeBigintLiteral(
                  BigDecimal.valueOf(dateTime.toInstant(ZoneOffset.UTC).toEpochMilli()));
          timeArgs.add(timestampNode);
        } else {
          timeArgs.add(timeExpr);
        }
        return timeArgs;
      case "DATE_FORMAT", "FORMAT_TIMESTAMP":
        RexNode dateExpr = argList.get(0);
        RexNode dateFormatPatternExpr = argList.get(1);
        RexNode timestampNode;
        // Convert to timestamp
        if (dateExpr instanceof RexLiteral dateLiteral) {
          String dateStringValue = Objects.requireNonNull(dateLiteral.getValueAs(String.class));
          timestampNode =
              context.rexBuilder.makeTimestampLiteral(
                  new TimestampString(dateStringValue), RelDataType.PRECISION_NOT_SPECIFIED);
        } else {
          timestampNode = dateExpr;
        }
        return List.of(dateFormatPatternExpr, timestampNode);
      case "UNIX_TIMESTAMP":
        List<RexNode> UnixArgs = new ArrayList<>(argList);
        UnixArgs.add(context.rexBuilder.makeFlag(argList.getFirst().getType().getSqlTypeName()));
        return UnixArgs;
      case "DAY_OF_WEEK":
        RexNode dow =
            context.rexBuilder.makeIntervalLiteral(
                new SqlIntervalQualifier(TimeUnit.DOW, null, SqlParserPos.ZERO));
        return List.of(dow, convertToDateLiteralIfString(context.rexBuilder, argList.getFirst()));
      case "DAY_OF_YEAR":
        RexNode dom =
            context.rexBuilder.makeIntervalLiteral(
                new SqlIntervalQualifier(TimeUnit.DOY, null, SqlParserPos.ZERO));
        return List.of(dom, convertToDateLiteralIfString(context.rexBuilder, argList.getFirst()));
      default:
        return argList;
    }
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
}
