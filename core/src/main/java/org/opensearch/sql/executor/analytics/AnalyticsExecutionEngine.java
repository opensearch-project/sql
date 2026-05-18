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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
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
    // Record EXECUTE inside the callback so the formatter's metric snapshot sees the final value.
    ProfileMetric execMetric = QueryProfiling.current().getOrCreateMetric(MetricName.EXECUTE);
    long execStart = System.nanoTime();

    planExecutor.execute(
        plan,
        null,
        new ActionListener<>() {
          @Override
          public void onResponse(Iterable<Object[]> rows) {
            try {
              List<RelDataTypeField> fields = plan.getRowType().getFieldList();
              List<ExprValue> results = convertRows(rows, fields);
              Schema schema = buildSchema(plan);
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

  /**
   * Recovers pre-cast datetime types stripped by {@code DatetimeOutputCastRule}. The wire format
   * keeps {@code timestamp}/{@code date}/{@code time} labels even though values render as VARCHAR.
   *
   * <p>Detection is structural: the rule's output Project has only identity {@link RexInputRef}
   * slots and {@code CAST(<datetime InputRef> AS VARCHAR)} slots — no user-authored expressions.
   * That shape uniquely identifies the rule's emit and avoids unwrapping user CASTs (which can
   * legitimately VARCHAR-wrap a datetime expression and should round-trip as VARCHAR).
   */
  private Schema buildSchema(RelNode plan) {
    List<RelDataTypeField> fields = plan.getRowType().getFieldList();
    Project castProject = findOutputCastProject(plan);
    List<RexNode> projects = castProject == null ? null : castProject.getProjects();
    List<Schema.Column> columns = new ArrayList<>();
    for (int i = 0; i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      RelDataType labelType = field.getType();
      if (projects != null && i < projects.size()) {
        labelType = unwrapDatetimeCast(projects.get(i), labelType);
      }
      columns.add(new Schema.Column(field.getName(), null, convertType(labelType)));
    }
    return new Schema(columns);
  }

  /**
   * Returns the Project emitted by {@code DatetimeOutputCastRule} — root, or sitting under a single
   * {@code Sort} (system query-size limit). Detection is by structural shape: every slot is either
   * an identity {@link RexInputRef} or {@code CAST(<datetime InputRef> AS VARCHAR)}, and at least
   * one slot is the cast form. Anything else (user-authored expression, function call) means this
   * is not the rule's emit.
   */
  private static Project findOutputCastProject(RelNode plan) {
    RelNode current = plan;
    while (current instanceof Sort) {
      current = current.getInput(0);
    }
    if (!(current instanceof Project project)) {
      return null;
    }
    boolean sawDatetimeCast = false;
    for (RexNode slot : project.getProjects()) {
      if (slot instanceof RexInputRef) {
        continue;
      }
      if (isDatetimeToVarcharCast(slot)) {
        sawDatetimeCast = true;
        continue;
      }
      return null;
    }
    return sawDatetimeCast ? project : null;
  }

  private static boolean isDatetimeToVarcharCast(RexNode expr) {
    if (!(expr instanceof RexCall call) || call.getKind() != SqlKind.CAST) {
      return false;
    }
    if (call.getOperands().size() != 1) {
      return false;
    }
    RexNode source = call.getOperands().get(0);
    if (!(source instanceof RexInputRef)) {
      return false;
    }
    if (call.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
      return false;
    }
    return isDatetime(source.getType().getSqlTypeName());
  }

  private static boolean isDatetime(SqlTypeName type) {
    return type == SqlTypeName.DATE
        || type == SqlTypeName.TIME
        || type == SqlTypeName.TIMESTAMP
        || type == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
  }

  private static RelDataType unwrapDatetimeCast(RexNode projection, RelDataType fallback) {
    if (!isDatetimeToVarcharCast(projection)) {
      return fallback;
    }
    return ((RexCall) projection).getOperands().get(0).getType();
  }

  private ExprType convertType(RelDataType type) {
    try {
      return OpenSearchTypeFactory.convertRelDataTypeToExprType(type);
    } catch (IllegalArgumentException e) {
      return org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;
    }
  }
}
