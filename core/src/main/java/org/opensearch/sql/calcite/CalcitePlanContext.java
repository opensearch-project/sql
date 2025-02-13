/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.function.BiFunction;
import lombok.Getter;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

public class CalcitePlanContext {

  public FrameworkConfig config;
  public CalciteConnection connection;
  public final RelBuilder relBuilder;
  public final ExtendedRexBuilder rexBuilder;

  @Getter private boolean isResolvingJoinCondition = false;

  public CalcitePlanContext(FrameworkConfig config, CalciteConnection connection) {
    this.config = config;
    this.connection = connection;
    this.relBuilder = RelBuilder.create(config);
    this.rexBuilder = new ExtendedRexBuilder(relBuilder.getRexBuilder());
  }

  public RexNode resolveJoinCondition(
      UnresolvedExpression expr,
      BiFunction<UnresolvedExpression, CalcitePlanContext, RexNode> transformFunction) {
    isResolvingJoinCondition = true;
    RexNode result = transformFunction.apply(expr, this);
    isResolvingJoinCondition = false;
    return result;
  }

  // for testing only
  public static CalcitePlanContext create(FrameworkConfig config) {
    return new CalcitePlanContext(config, null);
  }
}
