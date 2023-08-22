/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.logical.node;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.domain.Where;
import org.opensearch.sql.legacy.query.join.TableInJoinRequestBuilder;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.logical.LogicalOperator;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;

/** Selection expression */
public class Filter implements LogicalOperator {

  private final LogicalOperator next;

  /** Alias to WHERE clause mapping */
  private final Map<String, Where> aliasWhereMap = new HashMap<>();

  public Filter(LogicalOperator next, List<TableInJoinRequestBuilder> tables) {
    this.next = next;

    for (TableInJoinRequestBuilder table : tables) {
      Select select = table.getOriginalSelect();
      if (select.getWhere() != null) {
        aliasWhereMap.put(table.getAlias(), select.getWhere());
      }
    }
  }

  public Filter(LogicalOperator next) {
    this.next = next;
  }

  @Override
  public PlanNode[] children() {
    return new PlanNode[] {next};
  }

  @Override
  public boolean isNoOp() {
    return aliasWhereMap.isEmpty();
  }

  @Override
  public <T> PhysicalOperator[] toPhysical(Map<LogicalOperator, PhysicalOperator<T>> optimalOps) {
    // Always no-op after push down, skip it by returning next
    return new PhysicalOperator[] {optimalOps.get(next)};
  }

  public void pushDown(String tableAlias, Filter pushedDownFilter) {
    Where pushedDownWhere = pushedDownFilter.aliasWhereMap.remove(tableAlias);
    if (pushedDownWhere != null) {
      aliasWhereMap.put(tableAlias, pushedDownWhere);
    }
  }

  @Override
  public String toString() {
    return "Filter [ conditions=" + aliasWhereMap.values() + " ]";
  }
}
