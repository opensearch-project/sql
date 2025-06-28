/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.logical.node;

import java.util.Map;
import java.util.Optional;
import org.opensearch.sql.legacy.query.join.TableInJoinRequestBuilder;
import org.opensearch.sql.legacy.query.planner.core.Config;
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

  /** Configuration object for accessing custom settings */
  private final Optional<Config> config;

  public TableScan(TableInJoinRequestBuilder request, int pageSize) {
    this.request = request;
    this.pageSize = pageSize;
    this.config = Optional.empty();
  }

  public TableScan(TableInJoinRequestBuilder request, int pageSize, Config config) {
    this.request = request;
    this.pageSize = pageSize;
    this.config = Optional.ofNullable(config);
  }

  @Override
  public PlanNode[] children() {
    return new PlanNode[0];
  }

  @Override
  public <T> PhysicalOperator[] toPhysical(Map<LogicalOperator, PhysicalOperator<T>> optimalOps) {
    if (config.isPresent()) {
      return new PhysicalOperator[] {new PointInTime(request, pageSize, config.get())};
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
