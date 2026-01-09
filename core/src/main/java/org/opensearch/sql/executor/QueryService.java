/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.calcite.OpenSearchSchema;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.SystemLimitType;
import org.opensearch.sql.calcite.validate.OpenSearchSparkSqlDialect;
import org.opensearch.sql.calcite.validate.PplConvertletTable;
import org.opensearch.sql.calcite.validate.ValidationUtils;
import org.opensearch.sql.calcite.validate.converters.PplRelToSqlNodeConverter;
import org.opensearch.sql.calcite.validate.converters.PplSqlToRelConverter;
import org.opensearch.sql.calcite.validate.shuttles.PplRelToSqlRelShuttle;
import org.opensearch.sql.calcite.validate.shuttles.SkipRelValidationShuttle;
import org.opensearch.sql.calcite.validate.shuttles.SqlRewriteShuttle;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.exception.ExpressionEvaluationException;
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

  /** Execute the {@link UnresolvedPlan}, using {@link ResponseListener} to get response.<br> */
  public void execute(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    if (shouldUseCalcite(queryType)) {
      executeWithCalcite(plan, queryType, listener);
    } else {
      executeWithLegacy(plan, queryType, listener, Optional.empty());
    }
  }

  /** Explain the {@link UnresolvedPlan}, using {@link ResponseListener} to get response.<br> */
  public void explain(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      Explain.ExplainFormat format) {
    if (shouldUseCalcite(queryType)) {
      explainWithCalcite(plan, queryType, listener, format);
    } else {
      explainWithLegacy(plan, queryType, listener, format, Optional.empty());
    }
  }

  public void executeWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    CalcitePlanContext.run(
        () -> {
          try {
            ProfileContext profileContext =
                QueryProfiling.activate(QueryContext.isProfileEnabled());
            ProfileMetric analyzeMetric = profileContext.getOrCreateMetric(MetricName.ANALYZE);
            long analyzeStart = System.nanoTime();
            CalcitePlanContext context =
                CalcitePlanContext.create(
                    buildFrameworkConfig(), SysLimit.fromSettings(settings), queryType);
            RelNode relNode = analyze(plan, context);
            RelNode validated = validate(relNode, context);
            RelNode calcitePlan = convertToCalcitePlan(validated, context);
            analyzeMetric.set(System.nanoTime() - analyzeStart);
            executionEngine.execute(calcitePlan, context, listener);
          } catch (Throwable t) {
            if (isCalciteFallbackAllowed(t) && !(t instanceof NonFallbackCalciteException)) {
              log.warn("Fallback to V2 query engine since got exception", t);
              executeWithLegacy(plan, queryType, listener, Optional.of(t));
            } else {
              if (t instanceof Exception) {
                listener.onFailure((Exception) t);
              } else if (t instanceof ExceptionInInitializerError
                  && ((ExceptionInInitializerError) t).getException() instanceof Exception) {
                listener.onFailure((Exception) ((ExceptionInInitializerError) t).getException());
              } else if (t instanceof VirtualMachineError) {
                // throw and fast fail the VM errors such as OOM (same with v2).
                throw t;
              } else {
                // Calcite may throw AssertError during query execution.
                listener.onFailure(new CalciteUnsupportedException(t.getMessage(), t));
              }
            }
          }
        },
        settings);
  }

  public void explainWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      Explain.ExplainFormat format) {
    CalcitePlanContext.run(
        () -> {
          try {
            QueryProfiling.noop();
            CalcitePlanContext context =
                CalcitePlanContext.create(
                    buildFrameworkConfig(), SysLimit.fromSettings(settings), queryType);
            context.run(
                () -> {
                  RelNode relNode = analyze(plan, context);
                  RelNode validated = validate(relNode, context);
                  RelNode calcitePlan = convertToCalcitePlan(validated, context);
                  executionEngine.explain(calcitePlan, format, context, listener);
                },
                settings);
          } catch (Throwable t) {
            if (isCalciteFallbackAllowed(t)) {
              log.warn("Fallback to V2 query engine since got exception", t);
              explainWithLegacy(plan, queryType, listener, format, Optional.of(t));
            } else {
              if (t instanceof Error) {
                // Calcite may throw AssertError during query execution.
                listener.onFailure(new CalciteUnsupportedException(t.getMessage(), t));
              } else {
                listener.onFailure((Exception) t);
              }
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
      if (shouldUseCalcite(queryType) && isCalciteFallbackAllowed(null)) {
        // if there is a failure thrown from Calcite and execution after fallback V2
        // keeps failure, we should throw the failure from Calcite.
        calciteFailure.ifPresentOrElse(
            t -> listener.onFailure(new RuntimeException(t)), () -> listener.onFailure(e));
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
      Explain.ExplainFormat format,
      Optional<Throwable> calciteFailure) {
    try {
      if (format != null
          && (format != Explain.ExplainFormat.STANDARD && format != Explain.ExplainFormat.YAML)) {
        throw new UnsupportedOperationException(
            "Explain mode " + format.name() + " is not supported in v2 engine");
      }
      executionEngine.explain(plan(analyze(plan, queryType)), listener);
    } catch (Exception e) {
      if (shouldUseCalcite(queryType) && isCalciteFallbackAllowed(null)) {
        // if there is a failure thrown from Calcite and execution after fallback V2
        // keeps failure, we should throw the failure from Calcite.
        calciteFailure.ifPresentOrElse(
            t -> listener.onFailure(new RuntimeException(t)), () -> listener.onFailure(e));
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

  /**
   * Validates a RelNode by converting it to SqlNode, performing validation, and converting back.
   *
   * <p>This process enables Calcite's type validation and implicit casting mechanisms to work on
   * PPL queries.
   *
   * @param relNode the relation node to validate
   * @param context the Calcite plan context containing the validator
   * @return the validated (and potentially modified) relation node
   */
  private RelNode validate(RelNode relNode, CalcitePlanContext context) {
    SkipRelValidationShuttle skipShuttle = new SkipRelValidationShuttle();
    relNode.accept(skipShuttle);
    if (skipShuttle.shouldSkipValidation()) {
      return relNode;
    }
    // Fix interval literals before conversion to SQL
    RelNode sqlRelNode = relNode.accept(new PplRelToSqlRelShuttle(context.rexBuilder, true));

    // Convert RelNode to SqlNode for validation
    RelToSqlConverter rel2sql = new PplRelToSqlNodeConverter(OpenSearchSparkSqlDialect.DEFAULT);
    SqlImplementor.Result result = rel2sql.visitRoot(sqlRelNode);
    SqlNode root = result.asStatement();

    // Rewrite SqlNode to remove database qualifiers
    SqlNode rewritten = root.accept(new SqlRewriteShuttle());
    SqlValidator validator = context.getValidator();
    try {
      validator.validate(Objects.requireNonNull(rewritten));
    } catch (CalciteContextException e) {
      if (ValidationUtils.tolerantValidationException(e)) {
        return relNode;
      }
      throw new ExpressionEvaluationException(e.getMessage(), e);
    }

    SqlToRelConverter.Config sql2relConfig =
        SqlToRelConverter.config()
            // Do not remove sort in subqueries so that the orders for queries like `... | sort a
            // | fields b` is preserved
            .withRemoveSortInSubQuery(false)
            // Disable automatic JSON_TYPE_OPERATOR wrapping for nested JSON functions.
            // See CALCITE-4989: Calcite wraps nested JSON functions with JSON_TYPE by default
            .withAddJsonTypeOperatorEnabled(false)
            // Set hint strategy so that hints can be properly propagated.
            // See SqlToRelConverter.java#convertSelectImpl
            .withHintStrategyTable(context.relBuilder.getCluster().getHintStrategies());
    SqlToRelConverter sql2rel =
        new PplSqlToRelConverter(
            context.config.getViewExpander(),
            validator,
            validator.getCatalogReader().unwrap(CalciteCatalogReader.class),
            context.relBuilder.getCluster(),
            PplConvertletTable.INSTANCE,
            sql2relConfig);
    // Convert the validated SqlNode back to RelNode
    RelNode validatedRel = sql2rel.convertQuery(rewritten, false, true).project();
    return validatedRel.accept(new PplRelToSqlRelShuttle(context.rexBuilder, false));
  }

  /** Translate {@link LogicalPlan} to {@link PhysicalPlan}. */
  public PhysicalPlan plan(LogicalPlan plan) {
    return planner.plan(plan);
  }

  private boolean isCalciteFallbackAllowed(@Nullable Throwable t) {
    // We always allow fallback the query failed with CalciteUnsupportedException.
    // This is for avoiding breaking changes when enable Calcite by default.
    if (t instanceof CalciteUnsupportedException) {
      return true;
    } else {
      if (settings != null) {
        Boolean fallback_allowed = settings.getSettingValue(Settings.Key.CALCITE_FALLBACK_ALLOWED);
        if (fallback_allowed == null) {
          return false;
        }
        return fallback_allowed;
      } else {
        return true;
      }
    }
  }

  private boolean isCalciteEnabled(Settings settings) {
    if (settings != null) {
      return settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
    } else {
      return false;
    }
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
