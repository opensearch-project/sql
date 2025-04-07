/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.calcite.OpenSearchSchema;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.planner.PlanContext;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/** The low level interface of core engine. */
@RequiredArgsConstructor
@AllArgsConstructor
public class QueryService {
  private static final Logger LOG = LogManager.getLogger();

  private final Analyzer analyzer;

  private final ExecutionEngine executionEngine;

  private final Planner planner;

  private CalciteRelNodeVisitor relNodeVisitor;

  private DataSourceService dataSourceService;

  private Settings settings;

  private boolean isCalciteEnabled() {
    if (settings != null) {
      return settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
    } else {
      return false;
    }
  }

  private boolean isCalciteFallbackAllowed() {
    if (settings != null) {
      return settings.getSettingValue(Settings.Key.CALCITE_FALLBACK_ALLOWED);
    } else {
      return true;
    }
  }

  // TODO https://github.com/opensearch-project/sql/issues/3457
  // Calcite is not available for SQL query now. Maybe release in 3.1.0?
  private boolean shouldUseCalcite(QueryType queryType) {
    return isCalciteEnabled() && queryType == QueryType.PPL && relNodeVisitor != null;
  }

  /**
   * Execute the {@link UnresolvedPlan}, using {@link ResponseListener} to get response.<br>
   * Todo. deprecated this interface after finalize {@link PlanContext}.
   *
   * @param plan {@link UnresolvedPlan}
   * @param listener {@link ResponseListener}
   */
  public void execute(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      if (shouldUseCalcite(queryType)) {
        try {
          AccessController.doPrivileged(
              (PrivilegedAction<Void>)
                  () -> {
                    final FrameworkConfig config = buildFrameworkConfig();
                    final CalcitePlanContext context = CalcitePlanContext.create(config, queryType);
                    executePlanByCalcite(analyze(plan, context), context, listener);
                    return null;
                  });
        } catch (Throwable t) {
          if (isCalciteFallbackAllowed()) {
            LOG.warn("Fallback to V2 query engine since got exception", t);
            executePlan(analyze(plan, queryType), PlanContext.emptyPlanContext(), listener);
          } else {
            if (t instanceof Error) {
              // Calcite may throw AssertError during query execution.
              // Convert them to CalciteUnsupportedException.
              listener.onFailure(new CalciteUnsupportedException(t.getMessage()));
            } else {
              listener.onFailure((Exception) t);
            }
          }
        }
      } else {
        executePlan(analyze(plan, queryType), PlanContext.emptyPlanContext(), listener);
      }
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Execute the {@link UnresolvedPlan}, with {@link PlanContext} and using {@link ResponseListener}
   * to get response.<br>
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
                      plan(plan), ExecutionContext.emptyExecutionContext(), listener));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  public void executePlanByCalcite(
      RelNode plan,
      CalcitePlanContext context,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    executionEngine.execute(convertToCalcitePlan(optimize(plan)), context, listener);
  }

  public void explainPlanByCalcite(
      RelNode plan,
      Explain.ExplainFormat format,
      CalcitePlanContext context,
      ResponseListener<ExecutionEngine.ExplainResponse> listener) {
    executionEngine.explain(convertToCalcitePlan(optimize(plan)), format, context, listener);
  }

  /**
   * Convert OpenSearch Plan to Calcite Plan. Although both plans consist of Calcite RelNodes, there
   * are some differences in the topological structures or semantics between them.
   *
   * @param osPlan Logical Plan derived from OpenSearch PPL
   */
  private static RelNode convertToCalcitePlan(RelNode osPlan) {
    RelNode calcitePlan = osPlan;

    /* Calcite only ensures collation of the final result produced from the root sort operator.
     * While we expect that the collation can be preserved through the pipes over PPL, we need to
     * explicitly add a sort operator on top of the original plan
     * to ensure the correct collation of the final result.
     * See logic in ${@link CalcitePrepareImpl}
     * For the redundant sort, we rely on Calcite optimizer to eliminate
     */
    RelCollation collation = osPlan.getTraitSet().getCollation();
    if (!(osPlan instanceof Sort) && collation != RelCollations.EMPTY) {
      calcitePlan = LogicalSort.create(osPlan, collation, null, null);
    }

    return calcitePlan;
  }

  public void explain(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.ExplainResponse> listener) {
    explain(plan, queryType, listener, Explain.ExplainFormat.STANDARD);
  }

  /**
   * Explain the query in {@link UnresolvedPlan} using {@link ResponseListener} to get and format
   * explain response.
   *
   * @param plan {@link UnresolvedPlan}
   * @param listener {@link ResponseListener} for explain response
   */
  public void explain(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      Explain.ExplainFormat format) {
    try {
      if (shouldUseCalcite(queryType)) {
        try {
          AccessController.doPrivileged(
              (PrivilegedAction<Void>)
                  () -> {
                    final FrameworkConfig config = buildFrameworkConfig();
                    final CalcitePlanContext context = CalcitePlanContext.create(config, queryType);
                    explainPlanByCalcite(analyze(plan, context), format, context, listener);
                    return null;
                  });
        } catch (Throwable t) {
          if (isCalciteFallbackAllowed()) {
            LOG.warn("Fallback to V2 query engine since got exception", t);
            executionEngine.explain(plan(analyze(plan, queryType)), listener);
          } else {
            if (t instanceof Error) {
              // Calcite may throw AssertError during query execution.
              // Convert them to CalciteUnsupportedException.
              listener.onFailure(new CalciteUnsupportedException(t.getMessage()));
            } else {
              listener.onFailure((Exception) t);
            }
          }
        }
      } else {
        executionEngine.explain(plan(analyze(plan, queryType)), listener);
      }
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /** Analyze {@link UnresolvedPlan}. */
  public LogicalPlan analyze(UnresolvedPlan plan, QueryType queryType) {
    return analyzer.analyze(plan, new AnalysisContext(queryType));
  }

  public RelNode analyze(UnresolvedPlan plan, CalcitePlanContext context) {
    return relNodeVisitor.analyze(plan, context);
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
            .programs(Programs.calc(DefaultRelMetadataProvider.INSTANCE))
            .typeSystem(OpenSearchTypeSystem.INSTANCE);
    return configBuilder.build();
  }

  /** Translate {@link LogicalPlan} to {@link PhysicalPlan}. */
  public PhysicalPlan plan(LogicalPlan plan) {
    return planner.plan(plan);
  }

  public RelNode optimize(RelNode plan) {
    return planner.customOptimize(plan);
  }
}
