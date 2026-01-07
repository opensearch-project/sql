/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.sql.Connection;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.type.RelDataType;
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

  /**
   * The flag used to determine whether we do metadata field projection for user 1. If a project is
   * never visited, we will do metadata field projection for user 2. Else not because user may
   * intend to show the metadata field themselves. // TODO: use stack here if we want to do similar
   * projection for subquery.
   */
  @Getter @Setter private boolean isProjectVisited = false;

  private final Stack<RexCorrelVariable> correlVar = new Stack<>();
  private final Stack<List<RexNode>> windowPartitions = new Stack<>();
  private final Deque<RecursiveRelationInfo> recursiveRelations = new ArrayDeque<>();

  @Getter public Map<String, RexLambdaRef> rexLambdaRefMap;

  /**
   * List of captured variables from outer scope for lambda functions. When a lambda body references
   * a field that is not a lambda parameter, it gets captured and stored here. The captured
   * variables are passed as additional arguments to the transform function.
   */
  @Getter private List<RexNode> capturedVariables;

  /** Whether we're currently inside a lambda context. */
  @Getter @Setter private boolean inLambdaContext = false;

  private CalcitePlanContext(FrameworkConfig config, SysLimit sysLimit, QueryType queryType) {
    this.config = config;
    this.sysLimit = sysLimit;
    this.queryType = queryType;
    this.connection = CalciteToolsHelper.connect(config, TYPE_FACTORY);
    this.relBuilder = CalciteToolsHelper.create(config, TYPE_FACTORY, connection);
    this.rexBuilder = new ExtendedRexBuilder(relBuilder.getRexBuilder());
    this.functionProperties = new FunctionProperties(QueryType.PPL);
    this.rexLambdaRefMap = new HashMap<>();
    this.capturedVariables = new ArrayList<>();
  }

  /**
   * Private constructor for creating a context that shares relBuilder with parent. Used by clone()
   * to create lambda contexts that can resolve fields from the parent context.
   */
  private CalcitePlanContext(CalcitePlanContext parent) {
    this.config = parent.config;
    this.sysLimit = parent.sysLimit;
    this.queryType = parent.queryType;
    this.connection = parent.connection;
    this.relBuilder = parent.relBuilder; // Share the same relBuilder
    this.rexBuilder = parent.rexBuilder; // Share the same rexBuilder
    this.functionProperties = parent.functionProperties;
    this.rexLambdaRefMap = new HashMap<>(); // New map for lambda variables
    this.capturedVariables = new ArrayList<>(); // New list for captured variables
    this.inLambdaContext = true; // Mark that we're inside a lambda
    this.recursiveRelations.addAll(parent.recursiveRelations);
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

  public void pushRecursiveRelation(String relationName, RelDataType rowType) {
    recursiveRelations.push(new RecursiveRelationInfo(relationName, rowType));
  }

  public Optional<RecursiveRelationInfo> findRecursiveRelation(String relationName) {
    if (relationName == null) {
      return Optional.empty();
    }
    String relationNameLower = relationName.toLowerCase(java.util.Locale.ROOT);
    for (RecursiveRelationInfo info : recursiveRelations) {
      if (info.nameLower.equals(relationNameLower)) {
        return Optional.of(info);
      }
    }
    return Optional.empty();
  }

  public void popRecursiveRelation() {
    if (!recursiveRelations.isEmpty()) {
      recursiveRelations.pop();
    }
  }

  /**
   * Creates a clone of this context that shares the relBuilder with the parent. This allows lambda
   * expressions to reference fields from the current row while having their own lambda variable
   * mappings.
   */
  public CalcitePlanContext clone() {
    return new CalcitePlanContext(this);
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

  /**
   * Captures an external variable for use inside a lambda. Returns a RexLambdaRef that references
   * the captured variable by its index in the captured variables list. The actual RexNode value is
   * stored in capturedVariables and will be passed as additional arguments to the transform
   * function.
   *
   * @param fieldRef The RexInputRef representing the external field
   * @param fieldName The name of the field being captured
   * @return A RexLambdaRef that can be used inside the lambda to reference the captured value
   */
  public RexLambdaRef captureVariable(RexNode fieldRef, String fieldName) {
    // Check if this variable is already captured
    for (int i = 0; i < capturedVariables.size(); i++) {
      if (capturedVariables.get(i).equals(fieldRef)) {
        // Return existing reference - offset by number of lambda params (1 for array element)
        return rexLambdaRefMap.get("__captured_" + i);
      }
    }

    // Add to captured variables list
    int captureIndex = capturedVariables.size();
    capturedVariables.add(fieldRef);

    // Create a lambda ref for this captured variable
    // The index is offset by the number of lambda parameters (1 for single-param lambda)
    // Count only actual lambda parameters, not captured variables
    int lambdaParamCount =
        (int)
            rexLambdaRefMap.keySet().stream().filter(key -> !key.startsWith("__captured_")).count();
    RexLambdaRef lambdaRef =
        new RexLambdaRef(lambdaParamCount + captureIndex, fieldName, fieldRef.getType());

    // Store it so we can find it again if the same field is referenced multiple times
    rexLambdaRefMap.put("__captured_" + captureIndex, lambdaRef);

    return lambdaRef;
  }

  @Getter
  public static final class RecursiveRelationInfo {
    private final String name;
    private final String nameLower;
    private final RelDataType rowType;

    private RecursiveRelationInfo(String name, RelDataType rowType) {
      this.name = name;
      this.nameLower = name.toLowerCase(java.util.Locale.ROOT);
      this.rowType = rowType;
    }
  }
}
