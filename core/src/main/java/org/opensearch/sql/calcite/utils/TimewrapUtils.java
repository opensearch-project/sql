/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Timewrap;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/** Utility methods for the timewrap command's Calcite plan construction. */
public class TimewrapUtils {

  public static final int MAX_PERIODS = 20;

  /** Check if the span unit is variable-length (month, quarter, year). */
  public static boolean isVariableLengthUnit(SpanUnit unit) {
    return "M".equals(unit.getName()) || "q".equals(unit.getName()) || "y".equals(unit.getName());
  }

  /** Convert a span unit and value to seconds. For variable-length units, returns approximate. */
  public static long spanToSeconds(SpanUnit unit, int value) {
    return switch (unit.getName()) {
      case "s" -> value;
      case "m" -> value * 60L;
      case "h" -> value * 3_600L;
      case "d" -> value * 86_400L;
      case "w" -> value * 7L * 86_400L;
      case "M" -> value * 30L * 86_400L;
      case "q" -> value * 91L * 86_400L;
      case "y" -> value * 365L * 86_400L;
      default ->
          throw new SemanticCheckException("Unsupported time unit in timewrap: " + unit.getName());
    };
  }

  /**
   * Get the timescale base name for column naming. Returns "spanValue|singular|plural" e.g.,
   * "1|day|days".
   */
  public static String unitBaseName(SpanUnit unit, int value) {
    String singular =
        switch (unit.getName()) {
          case "s" -> "second";
          case "m" -> "minute";
          case "h" -> "hour";
          case "d" -> "day";
          case "w" -> "week";
          case "M" -> "month";
          case "q" -> "quarter";
          case "y" -> "year";
          default -> "period";
        };
    String plural = singular + "s";
    return value + "|" + singular + "|" + plural;
  }

  /**
   * Compute a calendar unit number for a timestamp as a Calcite RexNode. For months: year*12 +
   * month. For quarters: year*4 + quarter. For years: year. Divided by spanValue.
   */
  public static RexNode calendarUnitNumber(
      RexBuilder rx, RexNode tsField, SpanUnit unit, int spanValue) {
    RexNode year = rx.makeCall(PPLBuiltinOperators.EXTRACT, rx.makeLiteral("YEAR"), tsField);
    RelDataType bigintType = rx.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

    RexNode unitNum;
    switch (unit.getName()) {
      case "M" -> {
        RexNode month = rx.makeCall(PPLBuiltinOperators.EXTRACT, rx.makeLiteral("MONTH"), tsField);
        unitNum =
            rx.makeCall(
                SqlStdOperatorTable.PLUS,
                rx.makeCall(
                    SqlStdOperatorTable.MULTIPLY,
                    year,
                    rx.makeExactLiteral(BigDecimal.valueOf(12), bigintType)),
                month);
      }
      case "q" -> {
        RexNode month = rx.makeCall(PPLBuiltinOperators.EXTRACT, rx.makeLiteral("MONTH"), tsField);
        RexNode quarter =
            rx.makeCall(
                SqlStdOperatorTable.DIVIDE,
                rx.makeCall(
                    SqlStdOperatorTable.MINUS,
                    month,
                    rx.makeExactLiteral(BigDecimal.ONE, bigintType)),
                rx.makeExactLiteral(BigDecimal.valueOf(3), bigintType));
        unitNum =
            rx.makeCall(
                SqlStdOperatorTable.PLUS,
                rx.makeCall(
                    SqlStdOperatorTable.MULTIPLY,
                    year,
                    rx.makeExactLiteral(BigDecimal.valueOf(4), bigintType)),
                quarter);
      }
      case "y" -> unitNum = year;
      default -> throw new SemanticCheckException("Not a variable-length unit: " + unit.getName());
    }

    if (spanValue > 1) {
      unitNum =
          rx.makeCall(
              SqlStdOperatorTable.DIVIDE,
              unitNum,
              rx.makeExactLiteral(BigDecimal.valueOf(spanValue), bigintType));
    }
    return unitNum;
  }

  /**
   * Compute the epoch seconds of the start of the calendar unit containing a timestamp. Month:
   * first day of the month. Quarter: first day of the quarter (precise with leap year). Year: Jan
   * 1.
   */
  public static RexNode calendarUnitStartEpoch(RexBuilder rx, RexNode tsField, SpanUnit unit) {
    RelDataType bigintType = rx.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    RexNode tsEpoch =
        rx.makeCast(bigintType, rx.makeCall(PPLBuiltinOperators.UNIX_TIMESTAMP, tsField), true);
    RexNode hour = rx.makeCall(PPLBuiltinOperators.EXTRACT, rx.makeLiteral("HOUR"), tsField);
    RexNode minute = rx.makeCall(PPLBuiltinOperators.EXTRACT, rx.makeLiteral("MINUTE"), tsField);
    RexNode second = rx.makeCall(PPLBuiltinOperators.EXTRACT, rx.makeLiteral("SECOND"), tsField);
    RexNode one = rx.makeExactLiteral(BigDecimal.ONE, bigintType);
    RexNode sec86400 = rx.makeExactLiteral(BigDecimal.valueOf(86400), bigintType);

    RexNode timeWithinDay =
        rx.makeCall(
            SqlStdOperatorTable.PLUS,
            rx.makeCall(
                SqlStdOperatorTable.PLUS,
                rx.makeCall(
                    SqlStdOperatorTable.MULTIPLY,
                    hour,
                    rx.makeExactLiteral(BigDecimal.valueOf(3600), bigintType)),
                rx.makeCall(
                    SqlStdOperatorTable.MULTIPLY,
                    minute,
                    rx.makeExactLiteral(BigDecimal.valueOf(60), bigintType))),
            second);

    if ("M".equals(unit.getName())) {
      RexNode dayOfMonth = rx.makeCall(PPLBuiltinOperators.EXTRACT, rx.makeLiteral("DAY"), tsField);
      return rx.makeCall(
          SqlStdOperatorTable.MINUS,
          rx.makeCall(
              SqlStdOperatorTable.MINUS,
              tsEpoch,
              rx.makeCall(
                  SqlStdOperatorTable.MULTIPLY,
                  rx.makeCall(SqlStdOperatorTable.MINUS, dayOfMonth, one),
                  sec86400)),
          timeWithinDay);
    } else if ("y".equals(unit.getName())) {
      RexNode doy = rx.makeCall(PPLBuiltinOperators.EXTRACT, rx.makeLiteral("DOY"), tsField);
      return rx.makeCall(
          SqlStdOperatorTable.MINUS,
          rx.makeCall(
              SqlStdOperatorTable.MINUS,
              tsEpoch,
              rx.makeCall(
                  SqlStdOperatorTable.MULTIPLY,
                  rx.makeCall(SqlStdOperatorTable.MINUS, doy, one),
                  sec86400)),
          timeWithinDay);
    } else {
      // Quarter: precise day-within-quarter via cumulative day lookup + leap year
      RexNode doy = rx.makeCall(PPLBuiltinOperators.EXTRACT, rx.makeLiteral("DOY"), tsField);
      RexNode month = rx.makeCall(PPLBuiltinOperators.EXTRACT, rx.makeLiteral("MONTH"), tsField);
      RexNode year = rx.makeCall(PPLBuiltinOperators.EXTRACT, rx.makeLiteral("YEAR"), tsField);

      RexNode monthsIntoQuarter =
          rx.makeCall(
              SqlStdOperatorTable.MOD,
              rx.makeCall(SqlStdOperatorTable.MINUS, month, one),
              rx.makeExactLiteral(BigDecimal.valueOf(3), bigintType));
      RexNode quarterStartMonth = rx.makeCall(SqlStdOperatorTable.MINUS, month, monthsIntoQuarter);

      RexNode cumDaysBeforeQS = cumDaysBeforeMonth(rx, quarterStartMonth, year, bigintType);
      RexNode quarterStartDOY = rx.makeCall(SqlStdOperatorTable.PLUS, cumDaysBeforeQS, one);
      RexNode dayWithinQuarter = rx.makeCall(SqlStdOperatorTable.MINUS, doy, quarterStartDOY);

      return rx.makeCall(
          SqlStdOperatorTable.MINUS,
          rx.makeCall(
              SqlStdOperatorTable.MINUS,
              tsEpoch,
              rx.makeCall(SqlStdOperatorTable.MULTIPLY, dayWithinQuarter, sec86400)),
          timeWithinDay);
    }
  }

  /**
   * Build a CASE expression for cumulative days before a given month, with leap year handling.
   * Month 1→0, Month 2→31, Month 3→59+leap, ..., Month 12→334+leap.
   */
  public static RexNode cumDaysBeforeMonth(
      RexBuilder rx, RexNode month, RexNode year, RelDataType bigintType) {
    RexNode mod4 =
        rx.makeCall(
            SqlStdOperatorTable.MOD, year, rx.makeExactLiteral(BigDecimal.valueOf(4), bigintType));
    RexNode mod100 =
        rx.makeCall(
            SqlStdOperatorTable.MOD,
            year,
            rx.makeExactLiteral(BigDecimal.valueOf(100), bigintType));
    RexNode mod400 =
        rx.makeCall(
            SqlStdOperatorTable.MOD,
            year,
            rx.makeExactLiteral(BigDecimal.valueOf(400), bigintType));
    RexNode zero = rx.makeExactLiteral(BigDecimal.ZERO, bigintType);
    RexNode isLeap =
        rx.makeCall(
            SqlStdOperatorTable.CASE,
            rx.makeCall(
                SqlStdOperatorTable.AND,
                rx.makeCall(SqlStdOperatorTable.EQUALS, mod4, zero),
                rx.makeCall(
                    SqlStdOperatorTable.OR,
                    rx.makeCall(SqlStdOperatorTable.NOT_EQUALS, mod100, zero),
                    rx.makeCall(SqlStdOperatorTable.EQUALS, mod400, zero))),
            rx.makeExactLiteral(BigDecimal.ONE, bigintType),
            zero);

    int[] cumDays = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

    List<RexNode> caseArgs = new ArrayList<>();
    for (int m = 1; m <= 12; m++) {
      caseArgs.add(
          rx.makeCall(
              SqlStdOperatorTable.EQUALS,
              month,
              rx.makeExactLiteral(BigDecimal.valueOf(m), bigintType)));
      RexNode days = rx.makeExactLiteral(BigDecimal.valueOf(cumDays[m - 1]), bigintType);
      if (m >= 3) {
        days = rx.makeCall(SqlStdOperatorTable.PLUS, days, isLeap);
      }
      caseArgs.add(days);
    }
    caseArgs.add(zero);

    return rx.makeCall(SqlStdOperatorTable.CASE, caseArgs.toArray(new RexNode[0]));
  }

  /** Compute calendar unit number from an epoch at plan time (Java). */
  public static long calendarUnitNumberFromEpoch(long epochSec, SpanUnit unit, int spanValue) {
    java.time.Instant instant = java.time.Instant.ofEpochSecond(epochSec);
    java.time.ZonedDateTime zdt = instant.atZone(java.time.ZoneOffset.UTC);
    long unitNum;
    switch (unit.getName()) {
      case "M" -> unitNum = zdt.getYear() * 12L + zdt.getMonthValue();
      case "q" -> unitNum = zdt.getYear() * 4L + (zdt.getMonthValue() - 1) / 3;
      case "y" -> unitNum = zdt.getYear();
      default -> throw new SemanticCheckException("Not a variable-length unit: " + unit.getName());
    }
    return unitNum / spanValue;
  }

  /**
   * Walk the AST from a Timewrap node to find a WHERE clause with an upper bound on the timestamp
   * field. Returns the upper bound as epoch seconds, or null if not found.
   */
  public static Long extractTimestampUpperBound(Timewrap node) {
    Node current = node;
    while (current != null && !current.getChild().isEmpty()) {
      current = current.getChild().get(0);
      if (current instanceof Filter filter) {
        return findUpperBound(filter.getCondition());
      }
    }
    return null;
  }

  private static Long findUpperBound(UnresolvedExpression expr) {
    if (expr instanceof And and) {
      Long left = findUpperBound(and.getLeft());
      Long right = findUpperBound(and.getRight());
      if (left != null && right != null) return Math.min(left, right);
      return left != null ? left : right;
    }
    if (expr instanceof Compare cmp) {
      String op = cmp.getOperator();
      if (("<=".equals(op) || "<".equals(op)) && isTimestampField(cmp.getLeft())) {
        return parseTimestampLiteral(cmp.getRight());
      }
      if ((">=".equals(op) || ">".equals(op)) && isTimestampField(cmp.getRight())) {
        return parseTimestampLiteral(cmp.getLeft());
      }
    }
    return null;
  }

  private static boolean isTimestampField(UnresolvedExpression expr) {
    if (expr instanceof Field field) {
      String name = field.getField().toString();
      return "@timestamp".equals(name) || "timestamp".equals(name);
    }
    return false;
  }

  private static Long parseTimestampLiteral(UnresolvedExpression expr) {
    if (expr instanceof Literal lit && lit.getValue() instanceof String s) {
      try {
        java.time.LocalDateTime ldt =
            java.time.LocalDateTime.parse(
                s, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        return ldt.toEpochSecond(java.time.ZoneOffset.UTC);
      } catch (Exception e) {
        try {
          return java.time.Instant.parse(s).getEpochSecond();
        } catch (Exception ignored) {
          return null;
        }
      }
    }
    return null;
  }
}
