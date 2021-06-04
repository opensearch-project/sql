/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
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
