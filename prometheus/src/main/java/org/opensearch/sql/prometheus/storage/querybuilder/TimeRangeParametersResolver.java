/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage.querybuilder;

import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;

import java.util.Date;
import lombok.NoArgsConstructor;
import org.apache.commons.math3.util.Pair;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

@NoArgsConstructor
public class TimeRangeParametersResolver extends ExpressionNodeVisitor<Void, Object> {

  private Long startTime;
  private Long endTime;

  /**
   * Build Range Query Parameters from filter expression. If the filter condition consists
   * of @timestamp, startTime and endTime are derived. or else it will be defaulted to now() and
   * now()-1hr. If one of starttime and endtime are provided, the other will be derived from them by
   * fixing the time range duration to 1hr.
   *
   * @param filterCondition expression.
   * @return query string
   */
  public Pair<Long, Long> resolve(Expression filterCondition) {
    if (filterCondition == null) {
      long endTime = new Date().getTime() / 1000;
      return Pair.create(endTime - 3600, endTime);
    }
    filterCondition.accept(this, null);
    if (startTime == null && endTime == null) {
      long endTime = new Date().getTime() / 1000;
      return Pair.create(endTime - 3600, endTime);
    } else if (startTime == null) {
      return Pair.create(endTime - 3600, endTime);
    } else if (endTime == null) {
      return Pair.create(startTime, startTime + 3600);
    } else {
      return Pair.create(startTime, endTime);
    }
  }

  @Override
  public Void visitFunction(FunctionExpression func, Object context) {
    if ((BuiltinFunctionName.LTE.getName().equals(func.getFunctionName())
        || BuiltinFunctionName.GTE.getName().equals(func.getFunctionName())
        || BuiltinFunctionName.LESS.getName().equals(func.getFunctionName())
        || BuiltinFunctionName.GREATER.getName().equals(func.getFunctionName()))) {
      ReferenceExpression ref = (ReferenceExpression) func.getArguments().get(0);
      Expression rightExpr = func.getArguments().get(1);
      if (ref.getAttr().equals(TIMESTAMP)) {
        ExprValue literalValue = rightExpr.valueOf();
        if (func.getFunctionName().getFunctionName().contains(">")) {
          startTime = literalValue.timestampValue().toEpochMilli() / 1000;
        }
        if (func.getFunctionName().getFunctionName().contains("<")) {
          endTime = literalValue.timestampValue().toEpochMilli() / 1000;
        }
      }
    } else {
      func.getArguments().stream()
          .filter(arg -> arg instanceof FunctionExpression)
          .forEach(arg -> visitFunction((FunctionExpression) arg, context));
    }
    return null;
  }
}
