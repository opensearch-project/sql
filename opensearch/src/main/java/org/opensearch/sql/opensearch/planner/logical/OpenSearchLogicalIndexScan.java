/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.planner.logical;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Set;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;

/**
 * OpenSearch Logical Index Scan Operation.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class OpenSearchLogicalIndexScan extends LogicalPlan {

  /**
   * Relation Name.
   */
  private final String relationName;

  /**
   * Filter Condition.
   */
  @Setter
  private Expression filter;

  /**
   * Projection List.
   */
  @Setter
  private Set<ReferenceExpression> projectList;

  /**
   * Sort List.
   */
  @Setter
  private List<Pair<Sort.SortOption, Expression>> sortList;

  @Setter
  private Integer offset;

  @Setter
  private Integer limit;


  @Setter
  private String highlightField;

  /**
   * ElasticsearchLogicalIndexScan Constructor.
   */
  @Builder
  public OpenSearchLogicalIndexScan(
      String relationName,
      Expression filter,
      Set<ReferenceExpression> projectList,
      List<Pair<Sort.SortOption, Expression>> sortList,
      Integer limit, Integer offset) {
    super(ImmutableList.of());
    this.relationName = relationName;
    this.filter = filter;
    this.projectList = projectList;
    this.sortList = sortList;
    this.limit = limit;
    this.offset = offset;
//    this.highlightField = highlightField;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }

  public boolean hasLimit() {
    return limit != null;
  }

  /**
   * Test has projects or not.
   *
   * @return true for has projects, otherwise false.
   */
  public boolean hasProjects() {
    return projectList != null && !projectList.isEmpty();
  }
}
