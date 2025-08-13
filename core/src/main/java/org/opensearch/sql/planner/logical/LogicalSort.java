/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.expression.Expression;

/** Sort Plan. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalSort extends LogicalPlan {

  /** Maximum number of results to return after sorting. */
  @Getter(AccessLevel.NONE)
  private final Integer count;

  private final List<Pair<SortOption, Expression>> sortList;

  /** Constructor of LogicalSort. */
  public LogicalSort(
      LogicalPlan child, Integer count, List<Pair<SortOption, Expression>> sortList) {
    super(Collections.singletonList(child));
    this.count = (count == null || count <= 0) ? null : count;
    this.sortList = sortList;
  }

  public Optional<Integer> getCount() {
    return Optional.ofNullable(count);
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitSort(this, context);
  }
}
