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
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.planner;

import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalDedupe;
import org.opensearch.sql.planner.logical.LogicalEval;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRareTopN;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalRemove;
import org.opensearch.sql.planner.logical.LogicalRename;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.planner.logical.LogicalValues;
import org.opensearch.sql.planner.logical.LogicalWindow;
import org.opensearch.sql.planner.physical.AggregationOperator;
import org.opensearch.sql.planner.physical.DedupeOperator;
import org.opensearch.sql.planner.physical.EvalOperator;
import org.opensearch.sql.planner.physical.FilterOperator;
import org.opensearch.sql.planner.physical.LimitOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.planner.physical.RareTopNOperator;
import org.opensearch.sql.planner.physical.RemoveOperator;
import org.opensearch.sql.planner.physical.RenameOperator;
import org.opensearch.sql.planner.physical.SortOperator;
import org.opensearch.sql.planner.physical.ValuesOperator;
import org.opensearch.sql.planner.physical.WindowOperator;

/**
 * Default implementor for implementing logical to physical translation. "Default" here means all
 * logical operator will be translated to correspondent physical operator to pipeline operations
 * in post-processing style in memory.
 * Different storage can override methods here to optimize default pipelining operator, for example
 * a storage has the flexibility to override visitFilter and visitRelation to push down filtering
 * operation and return a single physical index scan operator.
 *
 * @param <C>   context type
 */
public class DefaultImplementor<C> extends LogicalPlanNodeVisitor<PhysicalPlan, C> {

  @Override
  public PhysicalPlan visitRareTopN(LogicalRareTopN node, C context) {
    return new RareTopNOperator(
        visitChild(node, context),
        node.getCommandType(),
        node.getNoOfResults(),
        node.getFieldList(),
        node.getGroupByList()
    );
  }

  @Override
  public PhysicalPlan visitDedupe(LogicalDedupe node, C context) {
    return new DedupeOperator(
        visitChild(node, context),
        node.getDedupeList(),
        node.getAllowedDuplication(),
        node.getKeepEmpty(),
        node.getConsecutive());
  }

  @Override
  public PhysicalPlan visitProject(LogicalProject node, C context) {
    return new ProjectOperator(visitChild(node, context), node.getProjectList());
  }

  @Override
  public PhysicalPlan visitWindow(LogicalWindow node, C context) {
    return new WindowOperator(
        visitChild(node, context),
        node.getWindowFunction(),
        node.getWindowDefinition());
  }

  @Override
  public PhysicalPlan visitRemove(LogicalRemove node, C context) {
    return new RemoveOperator(visitChild(node, context), node.getRemoveList());
  }

  @Override
  public PhysicalPlan visitEval(LogicalEval node, C context) {
    return new EvalOperator(visitChild(node, context), node.getExpressions());
  }

  @Override
  public PhysicalPlan visitSort(LogicalSort node, C context) {
    return new SortOperator(visitChild(node, context), node.getSortList());
  }

  @Override
  public PhysicalPlan visitRename(LogicalRename node, C context) {
    return new RenameOperator(visitChild(node, context), node.getRenameMap());
  }

  @Override
  public PhysicalPlan visitAggregation(LogicalAggregation node, C context) {
    return new AggregationOperator(
        visitChild(node, context), node.getAggregatorList(), node.getGroupByList());
  }

  @Override
  public PhysicalPlan visitFilter(LogicalFilter node, C context) {
    return new FilterOperator(visitChild(node, context), node.getCondition());
  }

  @Override
  public PhysicalPlan visitValues(LogicalValues node, C context) {
    return new ValuesOperator(node.getValues());
  }

  @Override
  public PhysicalPlan visitLimit(LogicalLimit node, C context) {
    return new LimitOperator(visitChild(node, context), node.getLimit(), node.getOffset());
  }

  @Override
  public PhysicalPlan visitRelation(LogicalRelation node, C context) {
    throw new UnsupportedOperationException("Storage engine is responsible for "
        + "implementing and optimizing logical plan with relation involved");
  }

  protected PhysicalPlan visitChild(LogicalPlan node, C context) {
    // Logical operators visited here must have a single child
    return node.getChild().get(0).accept(this, context);
  }

}
