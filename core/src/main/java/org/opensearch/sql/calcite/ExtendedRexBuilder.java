/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
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
}
