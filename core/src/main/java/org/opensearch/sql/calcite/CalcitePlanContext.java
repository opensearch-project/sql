/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.sql.Connection;
import java.util.ArrayList;
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
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.HighlightConfig;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.expression.function.FunctionProperties;

public class CalcitePlanContext {

  public FrameworkConfig config;
  public final Connection connection;
  public final OpenSearchRelBuilder relBuilder;
  public final ExtendedRexBuilder rexBuilder;
  public final FunctionProperties functionProperties;
  public final QueryType queryType;
  public final SysLimit sysLimit;

  /** This thread local variable is only used to skip script encoding in script pushdown. */
  public static final ThreadLocal<Boolean> skipEncoding = ThreadLocal.withInitial(() -> false);

  /** When true, the execution engine strips all-null columns from the result (used by timewrap). */
  public static final ThreadLocal<Boolean> stripNullColumns = ThreadLocal.withInitial(() -> false);

  /**
   * Timewrap span unit name for column renaming in the execution engine. When set, the execution
   * engine uses __base_offset__ to compute absolute period names (e.g., "501days_before").
   */
  public static final ThreadLocal<String> timewrapUnitName = new ThreadLocal<>();

  /** Timewrap series mode: "relative", "short", or "exact". */
  public static final ThreadLocal<String> timewrapSeries = new ThreadLocal<>();

  /** Thread-local switch that tells whether the current query prefers legacy behavior. */
  private static final ThreadLocal<Boolean> legacyPreferredFlag =
      ThreadLocal.withInitial(() -> true);

  @Getter @Setter private HighlightConfig highlightConfig;
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

  /**
   * Foreach placeholder bindings active in this context, keyed by upper-cased placeholder name.
   * Multifield mode activates bindings directly (placeholders resolve against the current row);
   * collection modes stage bindings via {@link #stageForeachLambdaBindings} instead, so they only
   * become active inside the lambda context cloned for the generated {@code reduce} call.
   */
  @Getter private Map<String, ForeachBinding> foreachBindings = new HashMap<>();

  /** Bare identifiers enabled by explicit options such as {@code itemstr=ITEM}. */
  @Getter private Map<String, ForeachBinding> foreachIdentifierBindings = new HashMap<>();

  /** Bindings that become active in lambda contexts cloned from this one. */
  private Map<String, ForeachBinding> stagedForeachLambdaBindings = new HashMap<>();

  /** Bare identifier bindings staged for a generated foreach lambda. */
  private Map<String, ForeachBinding> stagedForeachIdentifierBindings = new HashMap<>();

  /** Expressions computed by earlier assignments in the same foreach eval iteration. */
  @Getter private Map<String, RexNode> foreachComputedBindings = new HashMap<>();

  /**
   * Maps AggregateFunction AST nodes to their output field index for HAVING/post-aggregate
   * resolution.
   */
  @Getter private final Map<AggregateFunction, Integer> aggregateOutputIndex = new HashMap<>();

  /** Maps GROUP BY Function AST nodes to their output field index for post-aggregate resolution. */
  @Getter private final Map<Function, Integer> groupKeyOutputIndex = new HashMap<>();

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
    this.highlightConfig = parent.highlightConfig;
    this.rexLambdaRefMap = new HashMap<>(); // New map for lambda variables
    this.capturedVariables = new ArrayList<>(); // New list for captured variables
    this.inLambdaContext = true; // Mark that we're inside a lambda
    // Active bindings carry over; staged bindings become active inside the lambda.
    this.foreachBindings = new HashMap<>(parent.foreachBindings);
    this.foreachBindings.putAll(parent.stagedForeachLambdaBindings);
    this.foreachIdentifierBindings = new HashMap<>(parent.foreachIdentifierBindings);
    this.foreachIdentifierBindings.putAll(parent.stagedForeachIdentifierBindings);
    this.foreachComputedBindings = new HashMap<>(parent.foreachComputedBindings);
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
      clearTimewrapSignals();
    }
  }

  /**
   * @return {@code true} when the current planning prefer legacy behavior.
   */
  public static boolean isLegacyPreferred() {
    return legacyPreferredFlag.get();
  }

  /**
   * Resets the timewrap thread-locals set by {@code CalciteRelNodeVisitor.visitTimewrap}. Called
   * from the query lifecycle's {@code finally} on every path (execute, explain, and exceptions) so
   * the signals never leak onto the next query that reuses this pooled worker thread.
   */
  public static void clearTimewrapSignals() {
    stripNullColumns.set(false);
    timewrapUnitName.set(null);
    timewrapSeries.set(null);
  }

  public void pushForeachBindings(
      Map<String, ForeachBinding> bindings, Map<String, ForeachBinding> identifierBindings) {
    foreachBindings = new HashMap<>(bindings);
    foreachIdentifierBindings = new HashMap<>(identifierBindings);
  }

  public void stageForeachLambdaBindings(
      Map<String, ForeachBinding> bindings, Map<String, ForeachBinding> identifierBindings) {
    stagedForeachLambdaBindings = new HashMap<>(bindings);
    stagedForeachIdentifierBindings = new HashMap<>(identifierBindings);
  }

  public void putForeachComputedBinding(String name, RexNode expression) {
    foreachComputedBindings.put(name.toUpperCase(java.util.Locale.ROOT), expression);
  }

  public void clearForeachBindings() {
    foreachBindings.clear();
    foreachIdentifierBindings.clear();
    stagedForeachLambdaBindings.clear();
    stagedForeachIdentifierBindings.clear();
    foreachComputedBindings.clear();
  }

  /**
   * A foreach placeholder binding. {@code FIELD} resolves to the named row field, {@code LITERAL}
   * to a string literal, and {@code PAIR_SLOT} to slot {@code pairIndex} (typed {@code pairType})
   * of the named lambda pair variable.
   */
  public record ForeachBinding(
      String value, ForeachBindingType type, int pairIndex, @Nullable RelDataType pairType) {
    public ForeachBinding(String value, ForeachBindingType type) {
      this(value, type, -1, null);
    }
  }

  public enum ForeachBindingType {
    FIELD,
    LITERAL,
    PAIR_SLOT
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
}
