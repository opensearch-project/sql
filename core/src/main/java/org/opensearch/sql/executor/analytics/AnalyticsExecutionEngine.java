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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
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
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Execution engine adapter for the analytics engine (Project Mustang).
 *
 * <p>Bridges the analytics engine's {@link QueryPlanExecutor} with the SQL plugin's {@link
 * ExecutionEngine} response pipeline. Takes a Calcite {@link RelNode}, delegates execution to the
 * analytics engine, and converts the raw results into {@link QueryResponse}.
 */
public class AnalyticsExecutionEngine implements ExecutionEngine {

  private final QueryPlanExecutor planExecutor;

  public AnalyticsExecutionEngine(QueryPlanExecutor planExecutor) {
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
    try {
      Iterable<Object[]> rows = planExecutor.execute(plan, null);

      List<RelDataTypeField> fields = plan.getRowType().getFieldList();
      List<ExprValue> results = convertRows(rows, fields);
      Schema schema = buildSchema(fields);

      listener.onResponse(new QueryResponse(schema, results, Cursor.None));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  @Override
  public void explain(
      RelNode plan,
      ExplainMode mode,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    try {
      String logical = RelOptUtil.toString(plan, mode.toExplainLevel());
      listener.onResponse(new ExplainResponse(new ExplainResponseNodeV2(logical, null, null)));
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
}
