/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.function.BiFunction;
import lombok.Getter;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

public class CalcitePlanContext {

  public static class OSRelBuilder extends RelBuilder {

    protected OSRelBuilder(
        @Nullable Context context, RelOptCluster cluster, @Nullable RelOptSchema relOptSchema) {
      super(context, cluster, relOptSchema);
    }
  }

  public FrameworkConfig config;
  public CalciteConnection connection;
  public CalciteServerStatement statement;
  public final CalcitePrepareImpl prepare;
  public final RelBuilder relBuilder;
  public final ExtendedRexBuilder rexBuilder;

  @Getter private boolean isResolvingJoinCondition = false;

  public CalcitePlanContext(FrameworkConfig config, CalciteConnection connection) {
    this.config = config;
    this.connection = connection;
    try {
      this.statement = connection.createStatement().unwrap(CalciteServerStatement.class);
    } catch (Exception e) {
      throw new RuntimeException("create statement failed", e);
    }
    this.prepare = new CalcitePrepareImpl();
    this.relBuilder =
        prepare.perform(
            statement,
            config,
            (cluster, relOptSchema, rootSchema, statement) ->
                new OSRelBuilder(config.getContext(), cluster, relOptSchema));
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
}
