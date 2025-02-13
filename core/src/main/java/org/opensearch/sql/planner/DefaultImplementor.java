/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner;

import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalCloseCursor;
import org.opensearch.sql.planner.logical.LogicalDedupe;
import org.opensearch.sql.planner.logical.LogicalEval;
import org.opensearch.sql.planner.logical.LogicalExpand;
import org.opensearch.sql.planner.logical.LogicalFetchCursor;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalFlatten;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRareTopN;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalRemove;
import org.opensearch.sql.planner.logical.LogicalRename;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.planner.logical.LogicalTrendline;
import org.opensearch.sql.planner.logical.LogicalValues;
import org.opensearch.sql.planner.logical.LogicalWindow;
import org.opensearch.sql.planner.physical.AggregationOperator;
import org.opensearch.sql.planner.physical.CursorCloseOperator;
import org.opensearch.sql.planner.physical.DedupeOperator;
import org.opensearch.sql.planner.physical.EvalOperator;
import org.opensearch.sql.planner.physical.ExpandOperator;
import org.opensearch.sql.planner.physical.FilterOperator;
import org.opensearch.sql.planner.physical.FlattenOperator;
import org.opensearch.sql.planner.physical.LimitOperator;
import org.opensearch.sql.planner.physical.NestedOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.planner.physical.RareTopNOperator;
import org.opensearch.sql.planner.physical.RemoveOperator;
import org.opensearch.sql.planner.physical.RenameOperator;
import org.opensearch.sql.planner.physical.SortOperator;
import org.opensearch.sql.planner.physical.TakeOrderedOperator;
import org.opensearch.sql.planner.physical.TrendlineOperator;
import org.opensearch.sql.planner.physical.ValuesOperator;
import org.opensearch.sql.planner.physical.WindowOperator;
import org.opensearch.sql.storage.read.TableScanBuilder;
import org.opensearch.sql.storage.write.TableWriteBuilder;

/**
 * Default implementor for implementing logical to physical translation. "Default" here means all
 * logical operator will be translated to correspondent physical operator to pipeline operations in
 * post-processing style in memory. Different storage can override methods here to optimize default
 * pipelining operator, for example a storage has the flexibility to override visitFilter and
 * visitRelation to push down filtering operation and return a single physical index scan operator.
 *
 * @param <C> context type
 */
public class DefaultImplementor<C> extends LogicalPlanNodeVisitor<PhysicalPlan, C> {

  @Override
  public PhysicalPlan visitRareTopN(LogicalRareTopN node, C context) {
    return new RareTopNOperator(
        visitChild(node, context),
        node.getCommandType(),
        node.getNoOfResults(),
        node.getFieldList(),
        node.getGroupByList());
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
    return new ProjectOperator(
        visitChild(node, context), node.getProjectList(), node.getNamedParseExpressions());
  }

  @Override
  public PhysicalPlan visitWindow(LogicalWindow node, C context) {
    return new WindowOperator(
        visitChild(node, context), node.getWindowFunction(), node.getWindowDefinition());
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
  public PhysicalPlan visitExpand(LogicalExpand node, C context) {
    return new ExpandOperator(visitChild(node, context), node.getFieldRefExp());
  }

  @Override
  public PhysicalPlan visitFlatten(LogicalFlatten node, C context) {
    return new FlattenOperator(visitChild(node, context), node.getFieldRefExp());
  }

  @Override
  public PhysicalPlan visitNested(LogicalNested node, C context) {
    return new NestedOperator(visitChild(node, context), node.getFields());
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
    PhysicalPlan child = visitChild(node, context);
    // Optimize sort + limit to take ordered operator
    if (child instanceof SortOperator sortChild) {
      return new TakeOrderedOperator(
          sortChild.getInput(), node.getLimit(), node.getOffset(), sortChild.getSortList());
    }
    return new LimitOperator(child, node.getLimit(), node.getOffset());
  }

  @Override
  public PhysicalPlan visitTableScanBuilder(TableScanBuilder plan, C context) {
    return plan.build();
  }

  @Override
  public PhysicalPlan visitTableWriteBuilder(TableWriteBuilder plan, C context) {
    return plan.build(visitChild(plan, context));
  }

  @Override
  public PhysicalPlan visitRelation(LogicalRelation node, C context) {
    throw new UnsupportedOperationException(
        "Storage engine is responsible for "
            + "implementing and optimizing logical plan with relation involved");
  }

  @Override
  public PhysicalPlan visitFetchCursor(LogicalFetchCursor plan, C context) {
    return new PlanSerializer(plan.getEngine()).convertToPlan(plan.getCursor());
  }

  @Override
  public PhysicalPlan visitCloseCursor(LogicalCloseCursor node, C context) {
    return new CursorCloseOperator(visitChild(node, context));
  }

  @Override
  public PhysicalPlan visitTrendline(LogicalTrendline plan, C context) {
    return new TrendlineOperator(visitChild(plan, context), plan.getComputations());
  }

  // Called when paging query requested without `FROM` clause only
  @Override
  public PhysicalPlan visitPaginate(LogicalPaginate plan, C context) {
    return visitChild(plan, context);
  }

  protected PhysicalPlan visitChild(LogicalPlan node, C context) {
    // Logical operators visited here must have a single child
    return node.getChild().get(0).accept(this, context);
  }
}
