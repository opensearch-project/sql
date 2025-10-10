/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.expression.function.FunctionProperties;

public class CalcitePlanContext {

  public FrameworkConfig config;
  public final Connection connection;
  public final RelBuilder relBuilder;
  public final ExtendedRexBuilder rexBuilder;
  public final FunctionProperties functionProperties;
  public final QueryType queryType;
  public final Integer querySizeLimit;

  /** This thread local variable is only used to skip script encoding in script pushdown. */
  public static final ThreadLocal<Boolean> skipEncoding = ThreadLocal.withInitial(() -> false);

  @Getter @Setter private boolean isResolvingJoinCondition = false;
  @Getter @Setter private boolean isResolvingSubquery = false;
  @Getter @Setter private boolean inCoalesceFunction = false;

  /**
   * The flag used to determine whether we do metadata field projection for user 1. If a project is
   * never visited, we will do metadata field projection for user 2. Else not because user may
   * intend to show the metadata field themselves. // TODO: use stack here if we want to do similar
   * projection for subquery.
   */
  @Getter @Setter private boolean isProjectVisited = false;

  private final Stack<RexCorrelVariable> correlVar = new Stack<>();
  private final Stack<List<RexNode>> windowPartitions = new Stack<>();

  @Getter public Map<String, RexLambdaRef> rexLambdaRefMap;

  private CalcitePlanContext(FrameworkConfig config, Integer querySizeLimit, QueryType queryType) {
    this.config = config;
    this.querySizeLimit = querySizeLimit;
    this.queryType = queryType;
    this.connection = CalciteToolsHelper.connect(config, TYPE_FACTORY);
    this.relBuilder = CalciteToolsHelper.create(config, TYPE_FACTORY, connection);
    this.rexBuilder = new ExtendedRexBuilder(relBuilder.getRexBuilder());
    this.functionProperties = new FunctionProperties(QueryType.PPL);
    this.rexLambdaRefMap = new HashMap<>();
  }

  private CalcitePlanContext(
      FrameworkConfig config,
      Integer querySizeLimit,
      QueryType queryType,
      Connection connection,
      RelBuilder relBuilder,
      ExtendedRexBuilder rexBuilder,
      FunctionProperties functionProperties,
      Map<String, RexLambdaRef> rexLambdaRefMap) {
    this.config = config;
    this.querySizeLimit = querySizeLimit;
    this.queryType = queryType;
    this.connection = connection;
    this.relBuilder = relBuilder;
    this.rexBuilder = rexBuilder;
    this.functionProperties = functionProperties;
    this.rexLambdaRefMap = rexLambdaRefMap;
  }

  public RexNode resolveJoinCondition(
      UnresolvedExpression expr,
      BiFunction<UnresolvedExpression, CalcitePlanContext, RexNode> transformFunction) {
    isResolvingJoinCondition = true;
    RexNode result = transformFunction.apply(expr, this);
    isResolvingJoinCondition = false;
    return result;
  }

  public Optional<RexCorrelVariable> popCorrelVar() {
    if (!correlVar.empty()) {
      return Optional.of(correlVar.pop());
    } else {
      return Optional.empty();
    }
  }

  public void pushCorrelVar(RexCorrelVariable v) {
    correlVar.push(v);
  }

  public Optional<RexCorrelVariable> peekCorrelVar() {
    if (!correlVar.empty()) {
      return Optional.of(correlVar.peek());
    } else {
      return Optional.empty();
    }
  }

  public CalcitePlanContext clone() {
    return new CalcitePlanContext(config, querySizeLimit, queryType);
  }

  /**
   * A deep copy to create a totally same one calciteContext
   *
   * @return a deep clone calcite context and current context
   */
  public CalcitePlanContext deepClone() {
    RelOptCluster cluster = this.relBuilder.getCluster();
    RelBuilderFactory factory = RelBuilder.proto(config.getContext());
    RelOptSchema schema =
        this.relBuilder.getCluster().getPlanner().getContext().unwrap(RelOptSchema.class);
    RelBuilder siblingRelBuilder = factory.create(cluster, schema);
    siblingRelBuilder.push(this.relBuilder.peek()); // Add current logical plan as base
    CalcitePlanContext clone =
        new CalcitePlanContext(
            config,
            querySizeLimit,
            queryType,
            connection,
            siblingRelBuilder,
            new ExtendedRexBuilder(siblingRelBuilder.getRexBuilder()),
            functionProperties,
            rexLambdaRefMap);
    clone.inCoalesceFunction = this.inCoalesceFunction;
    clone.isProjectVisited = this.isProjectVisited;
    clone.isResolvingJoinCondition = this.isResolvingJoinCondition;
    clone.isResolvingSubquery = this.isResolvingSubquery;
    return clone;
  }

  public static CalcitePlanContext create(
      FrameworkConfig config, Integer querySizeLimit, QueryType queryType) {
    return new CalcitePlanContext(config, querySizeLimit, queryType);
  }

  public void putRexLambdaRefMap(Map<String, RexLambdaRef> candidateMap) {
    this.rexLambdaRefMap.putAll(candidateMap);
  }
}
