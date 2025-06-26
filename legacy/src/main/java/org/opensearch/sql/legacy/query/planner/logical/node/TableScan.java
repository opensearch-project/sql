/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.logical.node;

import java.util.Map;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.legacy.query.join.TableInJoinRequestBuilder;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.logical.LogicalOperator;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;
import org.opensearch.sql.legacy.query.planner.physical.node.pointInTime.PointInTime;

/** Table scan */
public class TableScan implements LogicalOperator {

  /** Request builder for the table */
  private final TableInJoinRequestBuilder request;

  /** Page size for physical operator */
  private final int pageSize;

  /** Custom PIT keepalive timeout */
  private final TimeValue customPitKeepAlive;

  public TableScan(TableInJoinRequestBuilder request, int pageSize) {
    this.request = request;
    this.pageSize = pageSize;
    this.customPitKeepAlive = null; // Default constructor - no custom timeout
  }

  // Enhanced constructor with custom PIT keepalive
  public TableScan(TableInJoinRequestBuilder request, int pageSize, TimeValue customPitKeepAlive) {
    this.request = request;
    this.pageSize = pageSize;
    this.customPitKeepAlive = customPitKeepAlive;
  }

  @Override
  public PlanNode[] children() {
    return new PlanNode[0];
  }

  @Override
  public <T> PhysicalOperator[] toPhysical(Map<LogicalOperator, PhysicalOperator<T>> optimalOps) {
    // Create PointInTime with custom timeout if available
    if (customPitKeepAlive != null) {
      return new PhysicalOperator[] {new PointInTime(request, pageSize, customPitKeepAlive)};
    } else {
      return new PhysicalOperator[] {new PointInTime(request, pageSize)};
    }
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
