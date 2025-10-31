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
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.expression.function.FunctionProperties;

public class CalcitePlanContext {

  public FrameworkConfig config;
  public final Connection connection;
  public final RelBuilder relBuilder;
  public final ExtendedRexBuilder rexBuilder;
  public final FunctionProperties functionProperties;
  public final QueryType queryType;
  public final SysLimit sysLimit;

  /** This thread local variable is only used to skip script encoding in script pushdown. */
  public static final ThreadLocal<Boolean> skipEncoding = ThreadLocal.withInitial(() -> false);

  /** Thread-local switch that tells whether the current query prefers legacy behavior. */
  private static final ThreadLocal<Boolean> legacyPreferredFlag =
      ThreadLocal.withInitial(() -> true);

  @Getter @Setter private boolean isResolvingJoinCondition = false;
  @Getter @Setter private boolean isResolvingSubquery = false;
  @Getter @Setter private boolean inCoalesceFunction = false;

  /** Fields that are being grouped by in aggregation (for bin operations to preserve originals) */
  @Getter @Setter
  private java.util.Set<String> aggregationGroupByFields = new java.util.HashSet<>();

  /** Total number of group-by fields in current aggregation */
  @Getter @Setter private int aggregationGroupByCount = 0;

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

  private CalcitePlanContext(FrameworkConfig config, SysLimit sysLimit, QueryType queryType) {
    this.config = config;
    this.sysLimit = sysLimit;
    this.queryType = queryType;
    this.connection = CalciteToolsHelper.connect(config, TYPE_FACTORY);
    this.relBuilder = CalciteToolsHelper.create(config, TYPE_FACTORY, connection);
    this.rexBuilder = new ExtendedRexBuilder(relBuilder.getRexBuilder());
    this.functionProperties = new FunctionProperties(QueryType.PPL);
    this.rexLambdaRefMap = new HashMap<>();
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
    return new CalcitePlanContext(config, sysLimit, queryType);
  }

  public static CalcitePlanContext create(
      FrameworkConfig config, SysLimit sysLimit, QueryType queryType) {
    return new CalcitePlanContext(config, sysLimit, queryType);
  }

  /**
   * Executes {@code action} with the thread-local legacy flag set according to the supplied
   * settings.
   */
  public static void run(Runnable action, Settings settings) {
    Boolean preferred = settings.getSettingValue(Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED);
    legacyPreferredFlag.set(preferred);
    try {
      action.run();
    } finally {
      legacyPreferredFlag.remove();
    }
  }

  /**
   * @return {@code true} when the current planning prefer legacy behavior.
   */
  public static boolean isLegacyPreferred() {
    return legacyPreferredFlag.get();
  }

  public void putRexLambdaRefMap(Map<String, RexLambdaRef> candidateMap) {
    this.rexLambdaRefMap.putAll(candidateMap);
  }
}
