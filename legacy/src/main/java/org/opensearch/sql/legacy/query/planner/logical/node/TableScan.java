/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.logical.node;

import java.util.Map;

import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.query.join.TableInJoinRequestBuilder;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.logical.LogicalOperator;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;
import org.opensearch.sql.legacy.query.planner.physical.node.pointInTime.PointInTime;
import org.opensearch.sql.legacy.query.planner.physical.node.scroll.Scroll;

import static org.opensearch.sql.common.setting.Settings.Key.SQL_PAGINATION_API_SEARCH_AFTER;

/** Table scan */
public class TableScan implements LogicalOperator {

  /** Request builder for the table */
  private final TableInJoinRequestBuilder request;

  /** Page size for physical operator */
  private final int pageSize;

  public TableScan(TableInJoinRequestBuilder request, int pageSize) {
    this.request = request;
    this.pageSize = pageSize;
  }

  @Override
  public PlanNode[] children() {
    return new PlanNode[0];
  }

  @Override
  public <T> PhysicalOperator[] toPhysical(Map<LogicalOperator, PhysicalOperator<T>> optimalOps) {
    if (LocalClusterState.state().getSettingValue(SQL_PAGINATION_API_SEARCH_AFTER)) {
      return new PhysicalOperator[] {new PointInTime(request, pageSize)};
    }
    return new PhysicalOperator[] {new Scroll(request, pageSize)};
  }

  @Override
  public String toString() {
    return "TableScan";
  }

  /*********************************************
   *          Getters for Explain
   *********************************************/

  public String getTableAlias() {
    return request.getAlias();
  }

  public String getTableName() {
    return request.getOriginalSelect().getFrom().get(0).getIndex();
  }
}
