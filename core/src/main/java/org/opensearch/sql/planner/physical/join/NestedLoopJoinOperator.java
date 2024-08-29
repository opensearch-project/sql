/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.join;

import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.physical.PhysicalPlan;

@RequiredArgsConstructor
public class NestedLoopJoinOperator extends JoinOperator {
  private final PhysicalPlan left;
  private final PhysicalPlan right;
  private final Join.JoinType joinType;
  private final Expression condition;

  private final ImmutableList.Builder<ExprValue> joinedBuilder = ImmutableList.builder();
  private Iterator<ExprValue> joinedIterator;
  // Build side is left by default, set the smaller side as the build side in future TODO
  private Iterator<ExprValue> buildSide;

  @Override
  public void open() {
    left.open();
    right.open();

    // buildSide is left plan by default
    buildSide = left;

    if (joinType == Join.JoinType.INNER) {
      List<ExprValue> cached = cacheStreamedSide(right);
      innerJoin(cached);
    } else if (joinType == Join.JoinType.LEFT) {
      // build side is right plan and streamed side is left plan in left outer join.
      buildSide = right;
      List<ExprValue> cached = cacheStreamedSide(left);
      outerJoin(cached);
    } else if (joinType == Join.JoinType.RIGHT) {
      List<ExprValue> cached = cacheStreamedSide(right);
      outerJoin(cached);
    } else if (joinType == Join.JoinType.SEMI) {
      List<ExprValue> cached = cacheStreamedSide(right);
      semiJoin(cached);
    } else if (joinType == Join.JoinType.ANTI) {
      List<ExprValue> cached = cacheStreamedSide(right);
      antiJoin(cached);
    } else {
      // LeftOuter with BuildLeft
      // RightOuter with BuildRight
      // FullOuter
      List<ExprValue> cached = cacheStreamedSide(right);
      defaultJoin(cached);
    }
  }

  /** Convert iterator to a list to allow multiple iterations */
  private List<ExprValue> cacheStreamedSide(PhysicalPlan plan) {
    ImmutableList.Builder<ExprValue> streamedBuilder = ImmutableList.builder();
    plan.forEachRemaining(streamedBuilder::add);
    return streamedBuilder.build();
  }

  @Override
  public void close() {
    joinedIterator = null;
    left.close();
    right.close();
  }

  private void innerJoin(List<ExprValue> cacheStreamedSide) {
    Iterator<ExprValue> streamed = cacheStreamedSide.iterator();
    while (streamed.hasNext()) {
      ExprValue leftRow = buildSide.next();

      for (ExprValue rightRow : cacheStreamedSide) {
        ExprTupleValue joinedRow = combineExprTupleValue(leftRow, rightRow);
        ExprValue conditionValue = condition.valueOf(joinedRow.bindingTuples());
        if (!(conditionValue.isNull() || conditionValue.isMissing())
            && (conditionValue.booleanValue())) {
          joinedBuilder.add(joinedRow);
        }
      }
    }
    joinedIterator = joinedBuilder.build().iterator();
  }

  /** The implementation for LeftOuter with BuildRight, RightOuter with BuildLeft */
  private void outerJoin(List<ExprValue> cacheStreamedSide) {
    Set<ExprValue> matchedRows = new HashSet<>();

    // Probe phase
    for (ExprValue streamedRow : cacheStreamedSide) {
      boolean matched = false;
      while (buildSide.hasNext()) {
        ExprValue buildRow = buildSide.next();
        ExprTupleValue joinedRow =
            combineExprTupleValue(
                joinType == Join.JoinType.LEFT ? streamedRow : buildRow,
                joinType == Join.JoinType.LEFT ? buildRow : streamedRow);
        ExprValue conditionValue = condition.valueOf(joinedRow.bindingTuples());
        if (!(conditionValue.isNull() || conditionValue.isMissing())
            && conditionValue.booleanValue()) {
          joinedBuilder.add(joinedRow);
          matchedRows.add(streamedRow);
          matched = true;
          break;
        }
      }
      if (!matched) {
        ExprTupleValue joinedRow =
            combineExprTupleValue(
                joinType == Join.JoinType.LEFT ? streamedRow : ExprValueUtils.nullValue(),
                joinType == Join.JoinType.LEFT ? ExprValueUtils.nullValue() : streamedRow);
        joinedBuilder.add(joinedRow);
      }
    }

    // Add unmatched rows
    if (joinType == Join.JoinType.LEFT) {
      while (buildSide.hasNext()) {
        ExprValue buildRow = buildSide.next();
        if (!matchedRows.contains(buildRow)) {
          ExprTupleValue joinedRow = combineExprTupleValue(ExprValueUtils.nullValue(), buildRow);
          joinedBuilder.add(joinedRow);
        }
      }
    } else if (joinType == Join.JoinType.RIGHT) {
      for (ExprValue streamedRow : cacheStreamedSide) {
        if (!matchedRows.contains(streamedRow)) {
          ExprTupleValue joinedRow = combineExprTupleValue(streamedRow, ExprValueUtils.nullValue());
          joinedBuilder.add(joinedRow);
        }
      }
    }

    joinedIterator = joinedBuilder.build().iterator();
  }

  private void semiJoin(List<ExprValue> cacheStreamedSide) {
    Set<ExprValue> matchedRows = new HashSet<>();

    // Probe phase
    for (ExprValue streamedRow : cacheStreamedSide) {
      while (buildSide.hasNext()) {
        ExprValue buildRow = buildSide.next();
        ExprTupleValue joinedRow = combineExprTupleValue(streamedRow, buildRow);
        ExprValue conditionValue = condition.valueOf(joinedRow.bindingTuples());
        if (!(conditionValue.isNull() || conditionValue.isMissing())
            && conditionValue.booleanValue()) {
          matchedRows.add(streamedRow);
          break;
        }
      }
    }

    // Add matched rows
    for (ExprValue row : matchedRows) {
      joinedBuilder.add(row);
    }

    joinedIterator = joinedBuilder.build().iterator();
  }

  // Java
  private void antiJoin(List<ExprValue> cacheStreamedSide) {
    Set<ExprValue> matchedRows = new HashSet<>();

    // Probe phase
    for (ExprValue streamedRow : cacheStreamedSide) {
      boolean matched = false;
      while (buildSide.hasNext()) {
        ExprValue buildRow = buildSide.next();
        ExprTupleValue joinedRow = combineExprTupleValue(streamedRow, buildRow);
        ExprValue conditionValue = condition.valueOf(joinedRow.bindingTuples());
        if (!(conditionValue.isNull() || conditionValue.isMissing())
            && conditionValue.booleanValue()) {
          matchedRows.add(streamedRow);
          matched = true;
          break;
        }
      }
      if (!matched) {
        matchedRows.add(streamedRow);
      }
    }

    // Add unmatched rows
    for (ExprValue row : cacheStreamedSide) {
      if (!matchedRows.contains(row)) {
        joinedBuilder.add(row);
      }
    }

    joinedIterator = joinedBuilder.build().iterator();
  }

  private void defaultJoin(List<ExprValue> cacheStreamedSide) {}

  @Override
  public boolean hasNext() {
    return joinedIterator != null && joinedIterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return joinedIterator.next();
  }

  private ExprTupleValue combineExprTupleValue(ExprValue left, ExprValue right) {
    Map<String, ExprValue> combinedMap = left.tupleValue();
    combinedMap.putAll(right.tupleValue());
    return ExprTupleValue.fromExprValueMap(combinedMap);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return ImmutableList.of(left, right);
  }
}
