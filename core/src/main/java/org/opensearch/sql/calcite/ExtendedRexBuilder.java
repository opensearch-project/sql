/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
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
      case MILLISECOND:
      case MS:
        timeUnit = TimeUnit.MILLISECOND;
        break;
      case SECOND:
      case S:
        timeUnit = TimeUnit.SECOND;
        break;
      case MINUTE:
      case m:
        timeUnit = TimeUnit.MINUTE;
        break;
      case HOUR:
      case H:
        timeUnit = TimeUnit.HOUR;
        break;
      case DAY:
      case D:
        timeUnit = TimeUnit.DAY;
        break;
      case WEEK:
      case W:
        timeUnit = TimeUnit.WEEK;
        break;
      case MONTH:
      case M:
        timeUnit = TimeUnit.MONTH;
        break;
      case QUARTER:
      case Q:
        timeUnit = TimeUnit.QUARTER;
        break;
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
    // Calcite bug which doesn't consider to cast literal to boolean
    if (exp instanceof RexLiteral && sqlType == SqlTypeName.BOOLEAN) {
      if (exp.equals(makeLiteral("1", typeFactory.createSqlType(SqlTypeName.CHAR, 1)))) {
        return makeLiteral(true, type);
      } else if (exp.equals(makeLiteral("0", typeFactory.createSqlType(SqlTypeName.CHAR, 1)))) {
        return makeLiteral(false, type);
      } else if (SqlTypeUtil.isExactNumeric(exp.getType())) {
        return makeCall(
            type,
            SqlStdOperatorTable.NOT_EQUALS,
            ImmutableList.of(exp, makeZeroLiteral(exp.getType())));
        // TODO https://github.com/opensearch-project/sql/issues/3443
        // Current, we align the behaviour of Spark and Postgres, to align with OpenSearch V2,
        // enable following commented codes.
        //      } else {
        //        return makeCall(type,
        //            SqlStdOperatorTable.NOT_EQUALS,
        //            ImmutableList.of(exp, makeZeroLiteral(exp.getType())));
      }
    }
    return super.makeCast(pos, type, exp, matchNullability, safe, format);
  }
}
