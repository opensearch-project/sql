/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.ast.tree.HighlightConfig;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.calcite.OpenSearchSchema;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.SystemLimitType;
import org.opensearch.sql.calcite.utils.CalciteClassLoaderHelper;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.error.QueryProcessingStage;
import org.opensearch.sql.common.error.StageErrorHandler;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.exception.NonFallbackCalciteException;
import org.opensearch.sql.monitor.profile.MetricName;
import org.opensearch.sql.monitor.profile.ProfileContext;
import org.opensearch.sql.monitor.profile.ProfileMetric;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.planner.PlanContext;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.protocol.response.format.Format;

/** The low level interface of core engine. */
@RequiredArgsConstructor
@AllArgsConstructor
@Log4j2
public class QueryService {
  private final Analyzer analyzer;
  private final ExecutionEngine executionEngine;
  private final Planner planner;
  private DataSourceService dataSourceService;
  private Settings settings;

  @Getter(lazy = true)
  private final CalciteRelNodeVisitor relNodeVisitor = new CalciteRelNodeVisitor(dataSourceService);

  /** Helper: depending on the type of error, either re-raise or propagate to the listener. */
  private void propagateCalciteError(Throwable t, ResponseListener<?> listener)
      throws VirtualMachineError {
    if (t instanceof VirtualMachineError) {
      // throw and fast fail the VM errors such as OOM (same with v2).
      throw (VirtualMachineError) t;
    }
    if (t instanceof Exception) {
      listener.onFailure((Exception) t);
    } else if (t instanceof ExceptionInInitializerError
        && ((ExceptionInInitializerError) t).getException() instanceof Exception) {
      listener.onFailure((Exception) ((ExceptionInInitializerError) t).getException());
    } else {
      // Calcite may throw AssertError during query execution.
      listener.onFailure(new CalciteUnsupportedException(t.getMessage(), t));
    }
  }

  /** Execute the {@link UnresolvedPlan}, using {@link ResponseListener} to get response.<br> */
  public void execute(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    execute(plan, queryType, null, listener);
  }

  /** Execute with optional highlight config. */
  public void execute(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    execute(plan, queryType, highlightConfig, false, listener);
  }

  /** Execute with optional highlight config and include metadata flag. */
  public void execute(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      boolean includeMetadata,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    if (shouldUseCalcite(queryType)) {
      executeWithCalcite(plan, queryType, highlightConfig, includeMetadata, listener);
    } else {
      // Legacy engine always includes basic metadata (schema information)
      // The includeMetadata flag doesn't affect legacy engine behavior since
      // it already provides column names, types, and aliases in the schema
      executeWithLegacy(plan, queryType, listener, Optional.empty());
    }
  }

  /** Explain the {@link UnresolvedPlan}, using {@link ResponseListener} to get response.<br> */
  public void explain(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode) {
    explain(plan, queryType, null, listener, mode);
  }

  /** Explain with optional highlight config. */
  public void explain(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode) {
    explain(plan, queryType, highlightConfig, false, listener, mode, null);
  }

  /** Explain with optional highlight config, include metadata flag, and format. */
  public void explain(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      boolean includeMetadata,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode,
      Format format) {
    if (shouldUseCalcite(queryType)) {
      explainWithCalcite(plan, queryType, highlightConfig, includeMetadata, listener, mode, format);
    } else {
      // Legacy engine provides basic explain information
      // The includeMetadata flag doesn't significantly affect legacy explain behavior
      explainWithLegacy(plan, queryType, listener, mode, Optional.empty());
    }
  }

  public void executeWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    executeWithCalcite(plan, queryType, highlightConfig, false, listener);
  }

  public void executeWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      boolean includeMetadata,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    CalcitePlanContext.run(
        () -> {
          try {
            ProfileContext profileContext =
                QueryProfiling.activate(QueryContext.isProfileEnabled());
            ProfileMetric analyzeMetric = profileContext.getOrCreateMetric(MetricName.ANALYZE);
            long analyzeStart = System.nanoTime();
            CalciteClassLoaderHelper.withCalciteClassLoader(
                () -> {
                  CalcitePlanContext context =
                      CalcitePlanContext.create(
                          buildFrameworkConfig(),
                          SysLimit.fromSettings(settings),
                          queryType,
                          includeMetadata);

                  context.setHighlightConfig(highlightConfig);

                  // Wrap analyze with ANALYZING stage tracking
                  RelNode relNode =
                      StageErrorHandler.executeStage(
                          QueryProcessingStage.ANALYZING,
                          () -> analyze(plan, context),
                          "while preparing and validating the query plan");

                  // Wrap plan conversion with PLAN_CONVERSION stage tracking
                  RelNode calcitePlan =
                      StageErrorHandler.executeStage(
                          QueryProcessingStage.PLAN_CONVERSION,
                          () ->
                              withCheckedArithmetic(
                                  convertToCalcitePlan(relNode, context), context),
                          "while converting the query to an executable plan");

                  analyzeMetric.set(System.nanoTime() - analyzeStart);

                  executeCalcitePlan(calcitePlan, context, listener);
                },
                QueryService.class);
          } catch (Throwable t) {
            if (isCalciteFallbackAllowed(t) && !(t instanceof NonFallbackCalciteException)) {
              log.warn("Fallback to V2 query engine since got exception", t);
              // Legacy engine provides basic metadata support, so fallback is acceptable
              executeWithLegacy(plan, queryType, listener, Optional.of(t));
            } else {
              propagateCalciteError(t, listener);
            }
          }
        },
        settings);
  }

  private void executeCalcitePlan(
      RelNode calcitePlan,
      CalcitePlanContext context,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      StageErrorHandler.executeStageVoid(
          QueryProcessingStage.EXECUTING,
          () -> executionEngine.execute(calcitePlan, context, listener),
          "while running the query");
    } catch (RuntimeException e) {
      ArithmeticException overflow = findArithmeticOverflow(e);
      if (overflow != null) {
        throw new NonFallbackCalciteException(
            "Arithmetic overflow: " + overflow.getMessage(), overflow);
      }
      throw e;
    }
  }

  public void explainWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode) {
    explainWithCalcite(plan, queryType, highlightConfig, false, listener, mode, null);
  }

  public void explainWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      HighlightConfig highlightConfig,
      boolean includeMetadata,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode,
      Format format) {
    CalcitePlanContext.run(
        () -> {
          try {
            QueryProfiling.noop();
            CalciteClassLoaderHelper.withCalciteClassLoader(
                () -> {
                  CalcitePlanContext context =
                      CalcitePlanContext.create(
                          buildFrameworkConfig(),
                          SysLimit.fromSettings(settings),
                          queryType,
                          includeMetadata);
                  context.setHighlightConfig(highlightConfig);
                  context.run(
                      () -> {
                        RelNode relNode = analyze(plan, context);
                        RelNode calcitePlan =
                            withCheckedArithmetic(convertToCalcitePlan(relNode, context), context);
                        if (format != null) {
                          executionEngine.explain(calcitePlan, mode, format, context, listener);
                        } else {
                          executionEngine.explain(calcitePlan, mode, context, listener);
                        }
                      },
                      settings);
                },
                QueryService.class);
          } catch (Throwable t) {
            if (isCalciteFallbackAllowed(t)) {
              log.warn("Fallback to V2 query engine since got exception", t);
              explainWithLegacy(plan, queryType, listener, mode, Optional.of(t));
            } else {
              propagateCalciteError(t, listener);
            }
          }
        },
        settings);
  }

  public void executeWithLegacy(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> listener,
      Optional<Throwable> calciteFailure) {
    try {
      executePlan(analyze(plan, queryType), PlanContext.emptyPlanContext(), listener);
    } catch (Exception e) {
      if (calciteFailure.isPresent()) {
        // This happens if Calcite fell back to V2 due to some issue, and then V2 also failed.
        // Prefer the Calcite error.
        // https://github.com/opensearch-project/sql/issues/5060
        propagateCalciteError(calciteFailure.get(), listener);
      } else {
        listener.onFailure(e);
      }
    }
  }

  /**
   * Explain the query in {@link UnresolvedPlan} using {@link ResponseListener} to get and format
   * explain response.
   *
   * @param plan {@link UnresolvedPlan}
   * @param queryType {@link QueryType}
   * @param listener {@link ResponseListener} for explain response
   * @param calciteFailure Optional failure thrown from calcite
   */
  public void explainWithLegacy(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      ExplainMode mode,
      Optional<Throwable> calciteFailure) {
    try {
      if (mode != null && (mode != ExplainMode.STANDARD)) {
        throw new UnsupportedOperationException(
            "Explain mode " + mode.name() + " is not supported in v2 engine");
      }
      executionEngine.explain(plan(analyze(plan, queryType)), listener);
    } catch (Exception e) {
      if (calciteFailure.isPresent()) {
        // This happens if Calcite fell back to V2 due to some issue, and then V2 also failed.
        // Prefer the Calcite error.
        // https://github.com/opensearch-project/sql/issues/5060
        propagateCalciteError(calciteFailure.get(), listener);
      } else {
        listener.onFailure(e);
      }
    }
  }

  /**
   * Execute the {@link LogicalPlan}, with {@link PlanContext} and using {@link ResponseListener} to
   * get response.<br>
   * Todo. Pass split from PlanContext to ExecutionEngine in following PR.
   *
   * @param plan {@link LogicalPlan}
   * @param planContext {@link PlanContext}
   * @param listener {@link ResponseListener}
   */
  public void executePlan(
      LogicalPlan plan,
      PlanContext planContext,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      planContext
          .getSplit()
          .ifPresentOrElse(
              split -> executionEngine.execute(plan(plan), new ExecutionContext(split), listener),
              () ->
                  executionEngine.execute(
                      plan(plan),
                      ExecutionContext.querySizeLimit(
                          // For pagination, querySizeLimit shouldn't take effect.
                          // See {@link PaginationWindowIT::testQuerySizeLimitDoesNotEffectPageSize}
                          plan instanceof LogicalPaginate
                              ? null
                              : SysLimit.fromSettings(settings).querySizeLimit()),
                      listener));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  public RelNode analyze(UnresolvedPlan plan, CalcitePlanContext context) {
    return getRelNodeVisitor().analyze(plan, context);
  }

  /** Analyze {@link UnresolvedPlan}. */
  public LogicalPlan analyze(UnresolvedPlan plan, QueryType queryType) {
    return analyzer.analyze(plan, new AnalysisContext(queryType));
  }

  /** Translate {@link LogicalPlan} to {@link PhysicalPlan}. */
  public PhysicalPlan plan(LogicalPlan plan) {
    return planner.plan(plan);
  }

  private boolean isCalciteUnsupportedError(@Nullable Throwable t) {
    return switch (t) {
      case null -> false;
      case CalciteUnsupportedException calciteUnsupportedException -> true;
      case ErrorReport errorReport when t.getCause() instanceof CalciteUnsupportedException -> true;
      default -> false;
    };
  }

  private boolean isCalciteFallbackAllowed(@Nullable Throwable t) {
    // We always allow fallback the query failed with CalciteUnsupportedException.
    // This is for avoiding breaking changes when enable Calcite by default.
    if (isCalciteUnsupportedError(t)) {
      return true;
    }

    if (settings != null) {
      Boolean fallback_allowed = settings.getSettingValue(Settings.Key.CALCITE_FALLBACK_ALLOWED);
      if (fallback_allowed == null) {
        return false;
      }
      return fallback_allowed;
    }

    return true;
  }

  private boolean isCalciteEnabled(Settings settings) {
    if (settings != null) {
      return settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
    } else {
      return false;
    }
  }

  /**
   * Rewrite {@code +}/{@code -}/{@code *} to their overflow-checked variants ({@code CHECKED_PLUS}
   * / {@code CHECKED_MINUS} / {@code CHECKED_MULTIPLY}) so integer and long arithmetic overflow
   * throws {@link ArithmeticException} (via {@code Math.addExact} etc.) instead of silently
   * wrapping. Applied before pushdown so both coordinator-executed and pushed-down (script)
   * arithmetic are checked. Floating-point arithmetic is unchanged (IEEE 754).
   *
   * <p>This does the same rewrite as Calcite's {@code ConvertToChecked} but preserves each call's
   * originally inferred type (via {@code makeCall(type, op, operands)}) and touches only the three
   * arithmetic operators, so it does not re-derive the types of unrelated calls (e.g. {@code
   * CEIL}/{@code DIVIDE}) the way {@code ConvertToChecked} does.
   */
  private static RelNode withCheckedArithmetic(RelNode calcitePlan, CalcitePlanContext context) {
    RexShuttle checkedShuttle =
        new RexShuttle() {
          @Override
          public RexNode visitCall(RexCall call) {
            RexNode visited = super.visitCall(call);
            if (!(visited instanceof RexCall rexCall)) {
              return visited;
            }
            SqlOperator checked =
                switch (rexCall.getOperator().getKind()) {
                  case PLUS -> SqlStdOperatorTable.CHECKED_PLUS;
                  case MINUS -> SqlStdOperatorTable.CHECKED_MINUS;
                  case TIMES -> SqlStdOperatorTable.CHECKED_MULTIPLY;
                  default -> null;
                };
            // Only integer/long arithmetic can overflow silently and has a checked
            // implementation (Math.addExact etc.). Float/double/decimal have no checked variant
            // (SqlFunctions.checkedMultiply(double,double) does not exist) and follow IEEE 754, so
            // leave them untouched.
            if (checked == null || !isCheckableIntegerArithmetic(rexCall)) {
              return visited;
            }
            return context.rexBuilder.makeCall(rexCall.getType(), checked, rexCall.getOperands());
          }
        };
    return calcitePlan.accept(
        new RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visitChildren(other);
            return visited.accept(checkedShuttle);
          }
        });
  }

  /** Returns whether the result and every operand are BIGINT. */
  private static boolean isCheckableIntegerArithmetic(RexCall call) {
    if (!isCheckableLongType(call.getType())) {
      return false;
    }
    return call.getOperands().stream().allMatch(op -> isCheckableLongType(op.getType()));
  }

  private static boolean isCheckableLongType(org.apache.calcite.rel.type.RelDataType type) {
    return type.getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.BIGINT;
  }

  /**
   * Walk the cause chain to find an {@link ArithmeticException} raised by checked arithmetic. Row-
   * level overflow surfaces wrapped (SQLException -&gt; RuntimeException -&gt; ErrorReport), so a
   * top-level {@code catch (ArithmeticException)} is insufficient.
   */
  private static ArithmeticException findArithmeticOverflow(@Nullable Throwable t) {
    for (Throwable cause = t;
        cause != null && cause != cause.getCause();
        cause = cause.getCause()) {
      if (cause instanceof ArithmeticException arithmeticException) {
        return arithmeticException;
      }
    }
    return null;
  }

  // TODO https://github.com/opensearch-project/sql/issues/3457
  // Calcite is not available for SQL query now. Maybe release in 3.1.0?
  private boolean shouldUseCalcite(QueryType queryType) {
    return isCalciteEnabled(settings) && queryType == QueryType.PPL;
  }

  private FrameworkConfig buildFrameworkConfig() {
    // Use simple calcite schema since we don't compute tables in advance of the query.
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(true, false).plus();
    final SchemaPlus opensearchSchema =
        rootSchema.add(
            OpenSearchSchema.OPEN_SEARCH_SCHEMA_NAME, new OpenSearchSchema(dataSourceService));
    Frameworks.ConfigBuilder configBuilder =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT) // TODO check
            .defaultSchema(opensearchSchema)
            .traitDefs((List<RelTraitDef>) null)
            .programs(Programs.standard())
            .typeSystem(OpenSearchTypeSystem.INSTANCE);
    return configBuilder.build();
  }

  /**
   * Convert OpenSearch Plan to Calcite Plan. Although both plans consist of Calcite RelNodes, there
   * are some differences in the topological structures or semantics between them.
   *
   * @param osPlan Logical Plan derived from OpenSearch PPL
   * @param context Calcite context
   */
  private static RelNode convertToCalcitePlan(RelNode osPlan, CalcitePlanContext context) {
    // Explicitly add a limit operator to enforce query size limit
    RelNode calcitePlan =
        LogicalSystemLimit.create(
            SystemLimitType.QUERY_SIZE_LIMIT,
            osPlan,
            context.relBuilder.literal(context.sysLimit.querySizeLimit()));
    /* Calcite only ensures collation of the final result produced from the root sort operator.
     * While we expect that the collation can be preserved through the pipes over PPL, we need to
     * explicitly add a sort operator on top of the original plan
     * to ensure the correct collation of the final result.
     * See logic in ${@link CalcitePrepareImpl}
     * For the redundant sort, we rely on Calcite optimizer to eliminate
     */
    RelCollation collation = calcitePlan.getTraitSet().getCollation();
    if (!(calcitePlan instanceof Sort) && collation != RelCollations.EMPTY) {
      calcitePlan = LogicalSort.create(calcitePlan, collation, null, null);
    }
    return calcitePlan;
  }
}
