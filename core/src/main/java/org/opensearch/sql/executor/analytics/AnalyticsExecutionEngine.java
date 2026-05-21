/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.analytics;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.monitor.profile.MetricName;
import org.opensearch.sql.monitor.profile.ProfileMetric;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Execution engine adapter for the analytics engine (Project Mustang).
 *
 * <p>Bridges the analytics engine's {@link QueryPlanExecutor} with the SQL plugin's {@link
 * ExecutionEngine} response pipeline. Takes a Calcite {@link RelNode}, delegates execution to the
 * analytics engine, and converts the raw results into {@link QueryResponse}.
 */
public class AnalyticsExecutionEngine implements ExecutionEngine {

  private final QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor;

  public AnalyticsExecutionEngine(QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor) {
    this.planExecutor = planExecutor;
  }

  /** Not supported. Analytics queries use the RelNode path exclusively. */
  @Override
  public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    listener.onFailure(
        new UnsupportedOperationException("Analytics engine only supports RelNode execution"));
  }

  /** Not supported. Analytics queries use the RelNode path exclusively. */
  @Override
  public void execute(
      PhysicalPlan plan, ExecutionContext context, ResponseListener<QueryResponse> listener) {
    listener.onFailure(
        new UnsupportedOperationException("Analytics engine only supports RelNode execution"));
  }

  /** Not supported. Analytics queries use the RelNode path exclusively. */
  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    listener.onFailure(
        new UnsupportedOperationException("Analytics engine only supports RelNode execution"));
  }

  @Override
  public void execute(
      RelNode plan, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
    // QueryPlanExecutor became asynchronous in analytics-framework 3.7 — execution is dispatched
    // to a worker pool and results arrive on the listener. Record the execute metric in the
    // listener callback, before delegating to the user-supplied listener, so the metric snapshot
    // taken by SimpleJsonResponseFormatter sees the correct value.
    ProfileMetric execMetric = QueryProfiling.current().getOrCreateMetric(MetricName.EXECUTE);
    long execStart = System.nanoTime();
    final RelNode executablePlan = removeSubQueries(plan, context);

    planExecutor.execute(
        executablePlan,
        null,
        new ActionListener<>() {
          @Override
          public void onResponse(Iterable<Object[]> rows) {
            try {
              List<RelDataTypeField> fields = executablePlan.getRowType().getFieldList();
              List<ExprValue> results = convertRows(rows, fields);
              Schema schema = buildSchema(fields);
              execMetric.set(System.nanoTime() - execStart);
              listener.onResponse(new QueryResponse(schema, results, Cursor.None));
            } catch (Exception e) {
              listener.onFailure(e);
            }
          }

          @Override
          public void onFailure(Exception e) {
            listener.onFailure(e);
          }
        });
  }

  @Override
  public void explain(
      RelNode plan,
      ExplainMode mode,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    try {
      plan = removeSubQueries(plan, context);
      String logical = RelOptUtil.toString(plan, mode.toExplainLevel());
      ExplainResponse response =
          new ExplainResponse(new ExplainResponseNodeV2(logical, null, null));
      listener.onResponse(ExplainResponse.normalizeLf(response));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  private List<ExprValue> convertRows(Iterable<Object[]> rows, List<RelDataTypeField> fields) {
    List<ExprValue> results = new ArrayList<>();
    for (Object[] row : rows) {
      Map<String, ExprValue> valueMap = new LinkedHashMap<>();
      for (int i = 0; i < fields.size(); i++) {
        String columnName = fields.get(i).getName();
        Object value = (i < row.length) ? row[i] : null;
        valueMap.put(columnName, ExprValueUtils.fromObjectValue(value));
      }
      results.add(ExprTupleValue.fromExprValueMap(valueMap));
    }
    return results;
  }

  private Schema buildSchema(List<RelDataTypeField> fields) {
    List<Schema.Column> columns = new ArrayList<>();
    for (RelDataTypeField field : fields) {
      ExprType exprType = convertType(field.getType());
      columns.add(new Schema.Column(field.getName(), null, exprType));
    }
    return new Schema(columns);
  }

  private ExprType convertType(RelDataType type) {
    try {
      return OpenSearchTypeFactory.convertRelDataTypeToExprType(type);
    } catch (IllegalArgumentException e) {
      return org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;
    }
  }

  /**
   * Converts every {@link org.apache.calcite.rex.RexSubQuery} in the plan to a {@code
   * LogicalCorrelate}, then decorrelates back to standard join shapes (LEFT_SEMI / LEFT_ANTI /
   * INNER). The analytics-engine planner has no RexSubQuery handler — it routes filter / project
   * expressions through a {@code ScalarFunction} table that doesn't include EXISTS / IN / SOME /
   * ANY operators, so an un-removed subquery surfaces as {@code "Unrecognized filter operator
   * [EXISTS / EXISTS]"} at the filter rule. The legacy engine path picks this up implicitly inside
   * Calcite's {@code RelRunner.prepareStatement}, which is skipped on the analytics route.
   *
   * <p>Scoped to the analytics path only — placing this on {@link AnalyticsExecutionEngine} (rather
   * than on the central {@code UnifiedQueryPlanner}) keeps the legacy path's RelNode pipeline
   * untouched.
   */
  private static final HepProgram SUBQUERY_REMOVE_PROGRAM =
      new HepProgramBuilder()
          .addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
          .addRuleInstance(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE)
          .addRuleInstance(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE)
          .build();

  private static RelNode removeSubQueries(RelNode plan, CalcitePlanContext context) {
    HepPlanner planner = new HepPlanner(SUBQUERY_REMOVE_PROGRAM);
    planner.setRoot(plan);
    RelNode withCorrelates = planner.findBestExp();
    return RelDecorrelator.decorrelateQuery(withCorrelates, context.relBuilder);
  }
}
