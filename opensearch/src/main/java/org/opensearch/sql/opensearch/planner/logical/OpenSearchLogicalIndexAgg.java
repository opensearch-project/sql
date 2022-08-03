/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.planner.logical;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;

/**
 * Logical Index Scan Aggregation Operation.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class OpenSearchLogicalIndexAgg extends LogicalPlan {

  private final String relationName;

  /**
   * Filter Condition.
   */
  @Setter
  private Expression filter;

  /**
   * Aggregation List.
   */
  @Setter
  private List<NamedAggregator> aggregatorList;

  /**
   * Group List.
   */
  @Setter
  private List<NamedExpression> groupByList;

  /**
   * Sort List.
   */
  @Setter
  private List<Pair<Sort.SortOption, Expression>> sortList;

  /**
   * ElasticsearchLogicalIndexAgg Constructor.
   */
  @Builder
  public OpenSearchLogicalIndexAgg(
      String relationName,
      Expression filter,
      List<NamedAggregator> aggregatorList,
      List<NamedExpression> groupByList,
      List<Pair<Sort.SortOption, Expression>> sortList) {
    super(ImmutableList.of());
    this.relationName = relationName;
    this.filter = filter;
    this.aggregatorList = aggregatorList;
    this.groupByList = groupByList;
    this.sortList = sortList;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }
}
