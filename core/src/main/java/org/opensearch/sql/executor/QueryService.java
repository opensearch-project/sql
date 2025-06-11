/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.analysis.AnalysisContext;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.calcite.OpenSearchSchema;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.datasource.DataSourceService;
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

  /**
   * Execute the {@link UnresolvedPlan}, using {@link ResponseListener} to get response.<br>
   * Todo. deprecated this interface after finalize {@link PlanContext}.
   *
   * @param plan {@link UnresolvedPlan}
   * @param listener {@link ResponseListener}
   */
  public void execute(
      UnresolvedPlan plan, ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      try {
        final FrameworkConfig config = buildFrameworkConfig();
        final CalcitePlanContext context = new CalcitePlanContext(config);
        executePlanByCalcite(analyze(plan, context), context, listener);
      } catch (Exception e) {
        LOG.warn("Fallback to V2 query engine since got exception", e);
        executePlan(analyze(plan), PlanContext.emptyPlanContext(), listener);
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
    try {
      executionEngine.execute(optimize(plan), context, listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Explain the query in {@link UnresolvedPlan} using {@link ResponseListener} to get and format
   * explain response.
   *
   * @param plan {@link UnresolvedPlan}
   * @param listener {@link ResponseListener} for explain response
   */
  public void explain(
      UnresolvedPlan plan, ResponseListener<ExecutionEngine.ExplainResponse> listener) {
    try {
      executionEngine.explain(plan(analyze(plan)), listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /** Analyze {@link UnresolvedPlan}. */
  public LogicalPlan analyze(UnresolvedPlan plan) {
    return analyzer.analyze(plan, new AnalysisContext());
  }

  public RelNode analyze(UnresolvedPlan plan, CalcitePlanContext context) {
    return relNodeVisitor.analyze(plan, context);
  }

  private FrameworkConfig buildFrameworkConfig() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus opensearchSchema =
        rootSchema.add(
            OpenSearchSchema.OPEN_SEARCH_SCHEMA_NAME, new OpenSearchSchema(dataSourceService));
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT) // TODO check
        .defaultSchema(opensearchSchema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
        .build();
  }

  /** Translate {@link LogicalPlan} to {@link PhysicalPlan}. */
  public PhysicalPlan plan(LogicalPlan plan) {
    return planner.plan(plan);
  }

  public RelNode optimize(RelNode plan) {
    return planner.customOptimize(plan);
  }
}
