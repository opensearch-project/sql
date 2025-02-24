/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.opensearch.sql.calcite.CalcitePlanContext;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
        return SqlLibraryOperators.LENGTH;
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
      //case "IFNULL":
      //  return SqlLibraryOperators.IFNULL;
      //case "IF":
      //  return SqlLibraryOperators.IF;
      case "IS NOT NULL":
        return SqlStdOperatorTable.IS_NOT_NULL;
      case "IS NULL":
        return SqlStdOperatorTable.IS_NULL;
      //case "NULLIF":
      //  return SqlStdOperatorTable.NULLIF;
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
        return SqlStdOperatorTable.CONVERT;
      case "COS":
        return SqlStdOperatorTable.COS;
      case "COT":
        return SqlStdOperatorTable.COT;
      case "DEGREES":
        return SqlStdOperatorTable.DEGREES;
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
        return SqlStdOperatorTable.MOD;
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
        return SqlStdOperatorTable.SQRT;
      case "CBRT":
        return SqlStdOperatorTable.CBRT;
      // Built-in Date Functions
      case "CURRENT_TIMESTAMP":
        return SqlStdOperatorTable.CURRENT_TIMESTAMP;
      case "CURRENT_DATE":
        return SqlStdOperatorTable.CURRENT_DATE;
      case "DATE":
        return SqlLibraryOperators.DATE;
      case "ADDDATE":
        return SqlLibraryOperators.DATE_ADD_SPARK;
      case "DATE_ADD":
        return SqlLibraryOperators.DATEADD;
        // TODO Add more, ref RexImpTable
      case "DATE_SUB":
        return SqlLibraryOperators.DATE_SUB;
      case "HOUR":
        return SqlStdOperatorTable.HOUR;
      case "MINUTE":
        return SqlStdOperatorTable.MINUTE;
      default:
        throw new IllegalArgumentException("Unsupported operator: " + op);
    }
  }

  static List<RexNode> translateArgument(String op, List<RexNode> argList, CalcitePlanContext context) {
    switch (op.toUpperCase(Locale.ROOT)) {
      case "TRIM":
        List<RexNode> trimArgs = new ArrayList<>(List.of(context.rexBuilder.makeFlag(SqlTrimFunction.Flag.BOTH), context.rexBuilder.makeLiteral(" ")));
        trimArgs.addAll(argList);
        return trimArgs;
      case "LTRIM":
        List<RexNode> LTrimArgs = new ArrayList<>(List.of(context.rexBuilder.makeFlag(SqlTrimFunction.Flag.LEADING), context.rexBuilder.makeLiteral(" ")));
        LTrimArgs.addAll(argList);
        return LTrimArgs;
      case "RTRIM":
        List<RexNode> RTrimArgs = new ArrayList<>(List.of(context.rexBuilder.makeFlag(SqlTrimFunction.Flag.TRAILING), context.rexBuilder.makeLiteral(" ")));
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
        LogArgs.add(argList.get(1));
        LogArgs.add(argList.get(0));
        return LogArgs;
      default:
        return argList;
    }
  }
}
