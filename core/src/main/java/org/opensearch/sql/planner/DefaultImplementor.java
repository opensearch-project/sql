/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner;

import static org.opensearch.sql.planner.physical.join.JoinOperator.BuildSide.BuildLeft;
import static org.opensearch.sql.planner.physical.join.JoinOperator.BuildSide.BuildRight;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalCloseCursor;
import org.opensearch.sql.planner.logical.LogicalDedupe;
import org.opensearch.sql.planner.logical.LogicalEval;
import org.opensearch.sql.planner.logical.LogicalFetchCursor;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalJoin;
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
import org.opensearch.sql.planner.logical.LogicalValues;
import org.opensearch.sql.planner.logical.LogicalWindow;
import org.opensearch.sql.planner.physical.AggregationOperator;
import org.opensearch.sql.planner.physical.CursorCloseOperator;
import org.opensearch.sql.planner.physical.DedupeOperator;
import org.opensearch.sql.planner.physical.EvalOperator;
import org.opensearch.sql.planner.physical.FilterOperator;
import org.opensearch.sql.planner.physical.LimitOperator;
import org.opensearch.sql.planner.physical.NestedOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.planner.physical.RareTopNOperator;
import org.opensearch.sql.planner.physical.RemoveOperator;
import org.opensearch.sql.planner.physical.RenameOperator;
import org.opensearch.sql.planner.physical.SortOperator;
import org.opensearch.sql.planner.physical.TakeOrderedOperator;
import org.opensearch.sql.planner.physical.ValuesOperator;
import org.opensearch.sql.planner.physical.WindowOperator;
import org.opensearch.sql.planner.physical.join.HashJoinOperator;
import org.opensearch.sql.planner.physical.join.JoinOperator;
import org.opensearch.sql.planner.physical.join.JoinPredicatesHelper;
import org.opensearch.sql.planner.physical.join.NestedLoopJoinOperator;
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
  private static final Logger LOG = LogManager.getLogger();

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
  public PhysicalPlan visitJoin(LogicalJoin join, C ctx) {
    LOG.debug("join condition is {}", join.getCondition());
    List<Expression> predicates =
        JoinPredicatesHelper.splitConjunctivePredicates(join.getCondition());
    // Extract all equi-join key pairs
    List<Pair<Expression, Expression>> equiJoinKeys = new ArrayList<>();
    for (Expression predicate : predicates) {
      if (JoinPredicatesHelper.isEqual(predicate)) {
        Pair<Expression, Expression> pair =
            JoinPredicatesHelper.extractJoinKeys((FunctionExpression) predicate);
        if (pair.getLeft() instanceof ReferenceExpression
            && pair.getRight() instanceof ReferenceExpression) {
          if (canEvaluate((ReferenceExpression) pair.getLeft(), join.getLeft())
              && canEvaluate((ReferenceExpression) pair.getRight(), join.getRight())) {
            equiJoinKeys.add(pair);
          } else {
            throw new SemanticCheckException(
                StringUtils.format("Join key must be a field of index."));
          }
        } else {
          throw new SemanticCheckException(
              StringUtils.format(
                  "Join condition must contain field only. E.g. t1.field1 = t2.field2 AND"
                      + " t1.field3 = t2.field4. But found {}",
                  predicate.getClass().getSimpleName()));
        }
      } else {
        equiJoinKeys.clear();
        break;
      }
    }

    // 1. Determining Join with Hint and build side.
    JoinOperator.BuildSide buildSide = determineBuildSide(join.getType());
    // 2. Pick hash join if it is an equi-join and hash join supported
    if (!equiJoinKeys.isEmpty()) {
      Pair<List<Expression>, List<Expression>> unzipped = JoinPredicatesHelper.unzip(equiJoinKeys);
      List<Expression> leftKeys = unzipped.getLeft();
      List<Expression> rightKeys = unzipped.getRight();
      LOG.info("EquiJoin leftKeys are {}, rightKeys are {}", leftKeys, rightKeys);

      return new HashJoinOperator(
          leftKeys,
          rightKeys,
          join.getType(),
          buildSide,
          visitRelation((LogicalRelation) join.getLeft(), ctx),
          visitRelation((LogicalRelation) join.getRight(), ctx),
          Optional.empty());
      // 3. Pick sort merge join if the join keys are sortable. TODO
    } else {
      // 4. Pick Nested loop join if is a non-equi-join. TODO
      return new NestedLoopJoinOperator(
          visitRelation((LogicalRelation) join.getLeft(), ctx),
          visitRelation((LogicalRelation) join.getRight(), ctx),
          join.getType(),
          buildSide,
          join.getCondition());
    }
  }

  /**
   * Build side is right by default (except RightOuter). TODO set the smaller side as the build side
   * TODO set build side from hint if provided
   *
   * @param joinType Join type
   * @return Build side
   */
  private JoinOperator.BuildSide determineBuildSide(Join.JoinType joinType) {
    return joinType == Join.JoinType.RIGHT ? BuildLeft : BuildRight;
  }

  /** Return true if the reference can be evaluated in relation */
  private boolean canEvaluate(ReferenceExpression expr, LogicalPlan plan) {
    if (plan instanceof LogicalRelation relation) {
      return relation.getTable().getFieldTypes().containsKey(expr.getAttr());
    } else {
      throw new UnsupportedOperationException("Only relation can be used in join");
    }
  }

  @Override
  public PhysicalPlan visitFetchCursor(LogicalFetchCursor plan, C context) {
    return new PlanSerializer(plan.getEngine()).convertToPlan(plan.getCursor());
  }

  @Override
  public PhysicalPlan visitCloseCursor(LogicalCloseCursor node, C context) {
    return new CursorCloseOperator(visitChild(node, context));
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
