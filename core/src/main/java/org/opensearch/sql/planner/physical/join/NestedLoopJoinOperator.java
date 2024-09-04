/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.join;

import static org.opensearch.sql.planner.physical.join.JoinOperator.BuildSide.BuildRight;

import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Nested Loop Join Operator. For best performance, the build side should be set a smaller table,
 * without hint and CBO, we treat right side as a smaller table by default and the build side set to
 * right. TODO add join hint support. Best practice in PPL: source=bigger | INNER JOIN smaller ON
 * bigger.field1 = smaller.field2 AND bigger.field3 = smaller.field4 The build side is right
 * (smaller), and the streamed side is left (bigger). For RIGHT OUTER join, the build side is always
 * left. If the smaller table is left, it will get the best performance: source=smaller | RIGHT JOIN
 * bigger ON bigger.field1 = smaller.field2 AND bigger.field3 = smaller.field4 The build side is
 * left (smaller), and the streamed side is right (bigger).
 */
public class NestedLoopJoinOperator extends JoinOperator {
  private final BuildSide buildSide;
  private final Expression condition;

  public NestedLoopJoinOperator(
      PhysicalPlan left,
      PhysicalPlan right,
      Join.JoinType joinType,
      BuildSide buildSide,
      Expression condition) {
    super(left, right, joinType);
    this.buildSide = buildSide;
    this.condition = condition;
  }

  private final ImmutableList.Builder<ExprValue> joinedBuilder = ImmutableList.builder();
  private Iterator<ExprValue> joinedIterator;

  private List<ExprValue> cachedBuildSide;

  @Override
  public void open() {
    left.open();
    right.open();
    Iterator<ExprValue> streamed;
    if (buildSide == BuildRight) {
      cachedBuildSide = cacheIterator(right);
      streamed = left;
    } else {
      cachedBuildSide = cacheIterator(left);
      streamed = right;
    }

    switch (joinType) {
      case INNER -> innerJoin(streamed);
      case LEFT, RIGHT -> outerJoin(streamed);
      case SEMI -> semiJoin(streamed);
      case ANTI -> antiJoin(streamed);
      default -> throw new UnsupportedOperationException("Unsupported Join Type " + joinType);
    }
  }

  @Override
  public void close() {
    left.close();
    right.close();
    joinedIterator = null;
    cachedBuildSide = null;
  }

  /** The implementation for inner join: Inner with BuildRight */
  @Override
  public void innerJoin(Iterator<ExprValue> streamedSide) {
    while (streamedSide.hasNext()) {
      ExprValue streamedRow = streamedSide.next();

      for (ExprValue buildRow : cachedBuildSide) {
        ExprTupleValue joinedRow = combineExprTupleValue(buildSide, streamedRow, buildRow);
        ExprValue conditionValue = condition.valueOf(joinedRow.bindingTuples());
        if (!(conditionValue.isNull() || conditionValue.isMissing())
            && (conditionValue.booleanValue())) {
          joinedBuilder.add(joinedRow);
        }
      }
    }
    joinedIterator = joinedBuilder.build().iterator();
  }

  /** The implementation for outer join: LeftOuter with BuildRight RightOuter with BuildLeft */
  @Override
  public void outerJoin(Iterator<ExprValue> streamedSide) {
    while (streamedSide.hasNext()) {
      ExprValue streamedRow = streamedSide.next();
      boolean matched = false;
      for (ExprValue buildRow : cachedBuildSide) {
        ExprTupleValue joinedRow = combineExprTupleValue(buildSide, streamedRow, buildRow);
        ExprValue conditionValue = condition.valueOf(joinedRow.bindingTuples());
        if (!(conditionValue.isNull() || conditionValue.isMissing())
            && conditionValue.booleanValue()) {
          joinedBuilder.add(joinedRow);
          matched = true;
        }
      }
      if (!matched) {
        ExprTupleValue joinedRow =
            combineExprTupleValue(buildSide, streamedRow, ExprValueUtils.nullValue());
        joinedBuilder.add(joinedRow);
      }
    }

    joinedIterator = joinedBuilder.build().iterator();
  }

  /**
   * The implementation for left semi join: LeftSemi with BuildRight TODO LeftSemi with buildLeft
   */
  @Override
  public void semiJoin(Iterator<ExprValue> streamedSide) {
    Set<ExprValue> matchedRows = new HashSet<>();

    while (streamedSide.hasNext()) {
      ExprValue streamedRow = streamedSide.next();
      for (ExprValue buildRow : cachedBuildSide) {
        ExprTupleValue joinedRow = combineExprTupleValue(buildSide, streamedRow, buildRow);
        ExprValue conditionValue = condition.valueOf(joinedRow.bindingTuples());
        if (!(conditionValue.isNull() || conditionValue.isMissing())
            && conditionValue.booleanValue()) {
          matchedRows.add(streamedRow);
          break;
        }
      }
    }

    for (ExprValue row : matchedRows) {
      joinedBuilder.add(row);
    }

    joinedIterator = joinedBuilder.build().iterator();
  }

  /**
   * The implementation for left anti join: LeftAnti with BuildRight TODO LeftAnti with buildLeft
   */
  @Override
  public void antiJoin(Iterator<ExprValue> streamedSide) {
    while (streamedSide.hasNext()) {
      ExprValue streamedRow = streamedSide.next();
      boolean matched = false;
      for (ExprValue buildRow : cachedBuildSide) {
        ExprTupleValue joinedRow = combineExprTupleValue(buildSide, streamedRow, buildRow);
        ExprValue conditionValue = condition.valueOf(joinedRow.bindingTuples());
        if (!(conditionValue.isNull() || conditionValue.isMissing())
            && conditionValue.booleanValue()) {
          matched = true;
          break;
        }
      }
      if (!matched) {
        joinedBuilder.add(streamedRow);
      }
    }

    joinedIterator = joinedBuilder.build().iterator();
  }

  /** Convert iterator to a list to allow multiple iterations */
  private List<ExprValue> cacheIterator(PhysicalPlan plan) {
    ImmutableList.Builder<ExprValue> streamedBuilder = ImmutableList.builder();
    plan.forEachRemaining(streamedBuilder::add);
    return streamedBuilder.build();
  }

  @Override
  public boolean hasNext() {
    return joinedIterator != null && joinedIterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return joinedIterator.next();
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return ImmutableList.of(left, right);
  }
}
