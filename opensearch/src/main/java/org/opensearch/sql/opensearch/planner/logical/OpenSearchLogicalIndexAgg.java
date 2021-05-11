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
