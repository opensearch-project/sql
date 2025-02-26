/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.sql.Connection;
import java.util.function.BiFunction;
import lombok.Getter;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;

public class CalcitePlanContext {

  public FrameworkConfig config;
  public final Connection connection;
  public final RelBuilder relBuilder;
  public final ExtendedRexBuilder rexBuilder;

  @Getter private boolean isResolvingJoinCondition = false;

  private CalcitePlanContext(FrameworkConfig config, JavaTypeFactory typeFactory) {
    this.config = config;
    this.connection = CalciteToolsHelper.connect(config, typeFactory);
    this.relBuilder = CalciteToolsHelper.create(config, typeFactory, connection);
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

  public static CalcitePlanContext create(FrameworkConfig config) {
    return new CalcitePlanContext(config, null);
  }

  public static CalcitePlanContext create(FrameworkConfig config, JavaTypeFactory typeFactory) {
    return new CalcitePlanContext(config, typeFactory);
  }
}
