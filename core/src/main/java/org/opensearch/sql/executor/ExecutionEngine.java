/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/** Execution engine that encapsulates execution details. */
public interface ExecutionEngine {

  /**
   * Execute physical plan and call back response listener.<br>
   * Todo. deprecated this interface after finalize {@link ExecutionContext}.
   *
   * @param plan executable physical plan
   * @param listener response listener
   */
  void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener);

  /** Execute physical plan with {@link ExecutionContext} and call back response listener. */
  void execute(
      PhysicalPlan plan, ExecutionContext context, ResponseListener<QueryResponse> listener);

  /**
   * Explain physical plan and call back response listener. The reason why this has to be part of
   * execution engine interface is that the physical plan probably needs to be executed to get more
   * info for profiling, such as actual execution time, rows fetched etc.
   *
   * @param plan physical plan to explain
   * @param listener response listener
   */
  void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener);

  /** Execute calcite RelNode plan with {@link ExecutionContext} and call back response listener. */
  default void execute(
      RelNode plan, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {}

  /** Data class that encapsulates ExprValue. */
  @Data
  class QueryResponse {
    private final Schema schema;
    private final List<ExprValue> results;
    private final Cursor cursor;
  }

  @Data
  class Schema {
    private final List<Column> columns;

    @Data
    public static class Column {
      private final String name;
      private final String alias;
      private final ExprType exprType;
    }
  }

  /**
   * Data class that encapsulates explain result. This can help decouple core engine from concrete
   * explain response format.
   */
  @Data
  class ExplainResponse {
    private final ExplainResponseNode root;
  }

  @AllArgsConstructor
  @Data
  @RequiredArgsConstructor
  class ExplainResponseNode {
    private final String name;
    private Map<String, Object> description;
    private List<ExplainResponseNode> children;
  }
}
