/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.calcite.type.ExprIPType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

public class ExtendedRexBuilder extends RexBuilder {

  public ExtendedRexBuilder(RexBuilder rexBuilder) {
    super(rexBuilder.getTypeFactory());
  }

  public RexNode coalesce(RexNode... nodes) {
    return this.makeCall(SqlStdOperatorTable.COALESCE, nodes);
  }

  public RexNode equals(RexNode n1, RexNode n2) {
    return this.makeCall(SqlStdOperatorTable.EQUALS, n1, n2);
  }

  public RexNode and(RexNode left, RexNode right) {
    final RelDataType booleanType = this.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
    return this.makeCall(booleanType, SqlStdOperatorTable.AND, List.of(left, right));
  }

  public RelDataType commonType(RexNode... nodes) {
    return this.getTypeFactory()
        .leastRestrictive(Arrays.stream(nodes).map(RexNode::getType).toList());
  }

  public SqlIntervalQualifier createIntervalUntil(SpanUnit unit) {
    TimeUnit timeUnit;
    switch (unit) {
      case MICROSECOND:
      case US:
        timeUnit = TimeUnit.MICROSECOND;
        break;
      case MILLISECOND:
      case MS:
        timeUnit = TimeUnit.MILLISECOND;
        break;
      case SECONDS:
      case SECOND:
      case SECS:
      case SEC:
      case S:
        timeUnit = TimeUnit.SECOND;
        break;
      case MINUTES:
      case MINUTE:
      case MINS:
      case MIN:
      case m:
        timeUnit = TimeUnit.MINUTE;
        break;
      case HOURS:
      case HOUR:
      case HRS:
      case HR:
      case H:
        timeUnit = TimeUnit.HOUR;
        break;
      case DAYS:
      case DAY:
      case D:
        timeUnit = TimeUnit.DAY;
        break;
      case WEEKS:
      case WEEK:
      case W:
        timeUnit = TimeUnit.WEEK;
        break;
      case MONTHS:
      case MONTH:
      case MON:
      case M:
        timeUnit = TimeUnit.MONTH;
        break;
      case QUARTERS:
      case QUARTER:
      case QTRS:
      case QTR:
      case Q:
        timeUnit = TimeUnit.QUARTER;
        break;
      case YEARS:
      case YEAR:
      case Y:
        timeUnit = TimeUnit.YEAR;
        break;
      default:
        timeUnit = TimeUnit.EPOCH;
    }
    return new SqlIntervalQualifier(timeUnit, timeUnit, SqlParserPos.ZERO);
  }

  @Override
  public RexNode makeCast(
      SqlParserPos pos,
      RelDataType type,
      RexNode exp,
      boolean matchNullability,
      boolean safe,
      RexLiteral format) {
    final SqlTypeName sqlType = type.getSqlTypeName();
    RelDataType sourceType = exp.getType();
    // Calcite bug which doesn't consider to cast literal to boolean
    if (exp instanceof RexLiteral && sqlType == SqlTypeName.BOOLEAN) {
      if (exp.equals(makeLiteral("1", typeFactory.createSqlType(SqlTypeName.CHAR, 1)))) {
        return makeLiteral(true, type);
      } else if (exp.equals(makeLiteral("0", typeFactory.createSqlType(SqlTypeName.CHAR, 1)))) {
        return makeLiteral(false, type);
      } else if (SqlTypeUtil.isExactNumeric(sourceType)) {
        return makeCall(
            type,
            SqlStdOperatorTable.NOT_EQUALS,
            ImmutableList.of(exp, makeZeroLiteral(sourceType)));
        // TODO https://github.com/opensearch-project/sql/issues/3443
        // Current, we align the behaviour of Spark and Postgres, to align with OpenSearch V2,
        // enable following commented codes.
        //      } else {
        //        return makeCall(type,
        //            SqlStdOperatorTable.NOT_EQUALS,
        //            ImmutableList.of(exp, makeZeroLiteral(sourceType)));
      }
    } else if (type instanceof ExprIPType) {
      if (RexLiteral.isNullLiteral(exp)) {
        return super.makeCast(pos, type, exp, matchNullability, safe, format);
      }
      if (sourceType instanceof ExprIPType) {
        return exp;
      } else if (SqlTypeUtil.isCharacter(sourceType)) {
        return makeCall(type, PPLBuiltinOperators.IP, List.of(exp));
      }
      // Throwing error inside implementation will be suppressed by Calcite, thus
      // throwing 500 error. Therefore, we throw error here to ensure the error
      // information is displayed properly.
      throw new ExpressionEvaluationException(
          String.format(
              Locale.ROOT,
              "Cannot convert %s to IP, only STRING and IP types are supported",
              sourceType));
    } else if (sqlType == SqlTypeName.DATE
        || sqlType == SqlTypeName.TIME
        || sqlType == SqlTypeName.TIMESTAMP) {
      // Standard SQL temporal target. Lower to PPL UDF so the runtime String → temporal parsing
      // matches v2 semantics. The shuttle in OpenSearchCalcitePreparingStmt rewrites these to UDT
      // at execution time.
      if (RexLiteral.isNullLiteral(exp)) {
        return super.makeCast(pos, type, exp, matchNullability, safe, format);
      }
      if (sqlType == SqlTypeName.DATE) {
        return makeCall(type, PPLBuiltinOperators.DATE, List.of(exp));
      } else if (sqlType == SqlTypeName.TIME) {
        return makeCall(type, PPLBuiltinOperators.TIME, List.of(exp));
      } else {
        return makeCall(type, PPLBuiltinOperators.TIMESTAMP, List.of(exp));
      }
    }
    // Use a custom operator when casting floating point or decimal number to a character type.
    // This patch is necessary because in Calcite, 0.0F is cast to 0E0, decimal 0.x to x
    else if ((SqlTypeUtil.isApproximateNumeric(sourceType) || SqlTypeUtil.isDecimal(sourceType))
        && SqlTypeUtil.isCharacter(type)) {
      // NUMBER_TO_STRING uses java's built-in method to get the string representation of a number
      return makeCall(type, PPLBuiltinOperators.NUMBER_TO_STRING, List.of(exp));
    }
    // VARCHAR → VARBINARY for ip/binary fields. Emit BINARY(varchar) as a placeholder
    // RexCall the analytics backend adapter rewrites into a VARBINARY literal.
    else if (sqlType == SqlTypeName.VARBINARY
        && sourceType.getSqlTypeName() == SqlTypeName.VARCHAR) {
      return makeCall(type, PPLBuiltinOperators.BINARY, List.of(exp));
    }
    return super.makeCast(pos, type, exp, matchNullability, safe, format);
  }
}
