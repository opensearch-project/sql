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
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
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
@Log4j2
public class QueryService {
  private final Analyzer analyzer;
  private final ExecutionEngine executionEngine;
  private final Planner planner;

  @Getter(lazy = true)
  private final CalciteRelNodeVisitor relNodeVisitor = new CalciteRelNodeVisitor();

  private DataSourceService dataSourceService;
  private Settings settings;

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
      explainWithLegacy(plan, queryType, listener, Optional.empty());
    }
  }

  public void executeWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      AccessController.doPrivileged(
          (PrivilegedAction<Void>)
              () -> {
                CalcitePlanContext context =
                    CalcitePlanContext.create(buildFrameworkConfig(), queryType);
                RelNode relNode = analyze(plan, context);
                RelNode optimized = optimize(relNode);
                RelNode calcitePlan = convertToCalcitePlan(optimized);
                executionEngine.execute(calcitePlan, context, listener);
                return null;
              });
    } catch (Throwable t) {
      if (isCalciteFallbackAllowed()) {
        log.warn("Fallback to V2 query engine since got exception", t);
        executeWithLegacy(plan, queryType, listener, Optional.of(t));
      } else {
        if (t instanceof Error) {
          // Calcite may throw AssertError during query execution.
          listener.onFailure(new CalciteUnsupportedException(t.getMessage()));
        } else {
          listener.onFailure((Exception) t);
        }
      }
    }
  }

  public void explainWithCalcite(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.ExplainResponse> listener,
      Explain.ExplainFormat format) {
    try {
      AccessController.doPrivileged(
          (PrivilegedAction<Void>)
              () -> {
                CalcitePlanContext context =
                    CalcitePlanContext.create(buildFrameworkConfig(), queryType);
                RelNode relNode = analyze(plan, context);
                RelNode optimized = optimize(relNode);
                RelNode calcitePlan = convertToCalcitePlan(optimized);
                executionEngine.explain(calcitePlan, format, context, listener);
                return null;
              });
    } catch (Throwable t) {
      if (isCalciteFallbackAllowed()) {
        log.warn("Fallback to V2 query engine since got exception", t);
        explainWithLegacy(plan, queryType, listener, Optional.of(t));
      } else {
        if (t instanceof Error) {
          // Calcite may throw AssertError during query execution.
          listener.onFailure(new CalciteUnsupportedException(t.getMessage()));
        } else {
          listener.onFailure((Exception) t);
        }
      }
    }
  }

  public void executeWithLegacy(
      UnresolvedPlan plan,
      QueryType queryType,
      ResponseListener<ExecutionEngine.QueryResponse> listener,
      Optional<Throwable> calciteFailure) {
    try {
      executePlan(analyze(plan, queryType), PlanContext.emptyPlanContext(), listener);
    } catch (Exception e) {
      if (shouldUseCalcite(queryType) && isCalciteFallbackAllowed()) {
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
      Optional<Throwable> calciteFailure) {
    try {
      executionEngine.explain(plan(analyze(plan, queryType)), listener);
    } catch (Exception e) {
      if (shouldUseCalcite(queryType) && isCalciteFallbackAllowed()) {
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
                      plan(plan), ExecutionContext.emptyExecutionContext(), listener));
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

  public RelNode optimize(RelNode plan) {
    return planner.customOptimize(plan);
  }

  private boolean isCalciteFallbackAllowed() {
    if (settings != null) {
      return settings.getSettingValue(Settings.Key.CALCITE_FALLBACK_ALLOWED);
    } else {
      return true;
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
            .programs(Programs.calc(DefaultRelMetadataProvider.INSTANCE))
            .typeSystem(OpenSearchTypeSystem.INSTANCE);
    return configBuilder.build();
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
}
