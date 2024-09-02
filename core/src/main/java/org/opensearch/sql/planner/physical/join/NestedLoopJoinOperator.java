/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.join;

import static org.opensearch.sql.data.type.ExprCoreType.UNDEFINED;

import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
      List<ExprValue> cachedBuildSide = cacheIterator(right);
      innerJoin(cachedBuildSide, left);
    } else if (joinType == Join.JoinType.LEFT) {
      // build side is right plan and streamed side is left plan in left outer join.
      List<ExprValue> cachedBuildSide = cacheIterator(right);
      outerJoin(cachedBuildSide, left);
    } else if (joinType == Join.JoinType.RIGHT) {
      // build side is left plan and streamed side is right plan in right outer join.
      List<ExprValue> cachedBuildSide = cacheIterator(left);
      outerJoin(cachedBuildSide, right);
    } else if (joinType == Join.JoinType.SEMI) {
      // build right plan in left semi
      // TODO support buildLeft in LEFT SEMI
      List<ExprValue> cachedBuildSide = cacheIterator(right);
      semiJoin(cachedBuildSide, left);
    } else if (joinType == Join.JoinType.ANTI) {
      // build right plan in left semi
      // TODO support buildLeft in LEFT ANTI
      List<ExprValue> cachedBuildSide = cacheIterator(right);
      antiJoin(cachedBuildSide, left);
    } else {
      // FullOuter
      throw new UnsupportedOperationException("Unsupported Join Type " + joinType);
    }
  }

  /** Convert iterator to a list to allow multiple iterations */
  private List<ExprValue> cacheIterator(PhysicalPlan plan) {
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

  private void innerJoin(List<ExprValue> cachedBuildSide, Iterator<ExprValue> streamedSide) {
    while (streamedSide.hasNext()) {
      ExprValue streamedRow = streamedSide.next();

      for (ExprValue buildRow : cachedBuildSide) {
        ExprTupleValue joinedRow = combineExprTupleValue(streamedRow, buildRow);
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
  private void outerJoin(List<ExprValue> cachedBuildSide, Iterator<ExprValue> streamedSide) {
    while (streamedSide.hasNext()) {
      ExprValue streamedRow = streamedSide.next();
      boolean matched = false;
      for (ExprValue buildRow : cachedBuildSide) {
        ExprTupleValue joinedRow = combineExprTupleValue(streamedRow, buildRow);
        ExprValue conditionValue = condition.valueOf(joinedRow.bindingTuples());
        if (!(conditionValue.isNull() || conditionValue.isMissing())
            && conditionValue.booleanValue()) {
          joinedBuilder.add(joinedRow);
          matched = true;
        }
      }
      if (!matched) {
        ExprTupleValue joinedRow = combineExprTupleValue(streamedRow, ExprValueUtils.nullValue());
        joinedBuilder.add(joinedRow);
      }
    }

    joinedIterator = joinedBuilder.build().iterator();
  }

  private void semiJoin(List<ExprValue> cachedBuildSide, Iterator<ExprValue> streamedSide) {
    Set<ExprValue> matchedRows = new HashSet<>();

    while (streamedSide.hasNext()) {
      ExprValue streamedRow = streamedSide.next();
      for (ExprValue buildRow : cachedBuildSide) {
        ExprTupleValue joinedRow = combineExprTupleValue(streamedRow, buildRow);
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

  private void antiJoin(List<ExprValue> cachedBuildSide, Iterator<ExprValue> streamedSide) {
    while (streamedSide.hasNext()) {
      ExprValue streamedRow = streamedSide.next();
      boolean matched = false;
      for (ExprValue buildRow : cachedBuildSide) {
        ExprTupleValue joinedRow = combineExprTupleValue(streamedRow, buildRow);
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

  @Override
  public boolean hasNext() {
    return joinedIterator != null && joinedIterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return joinedIterator.next();
  }

  private ExprTupleValue combineExprTupleValue(ExprValue streamedRow, ExprValue buildRow) {
    ExprValue left = joinType == Join.JoinType.RIGHT ? buildRow : streamedRow;
    ExprValue right = joinType == Join.JoinType.RIGHT ? streamedRow : buildRow;
    Map<String, ExprValue> leftTuple = left.type().equals(UNDEFINED) ? Map.of() : left.tupleValue();
    Map<String, ExprValue> rightTuple =
        right.type().equals(UNDEFINED) ? Map.of() : right.tupleValue();
    Map<String, ExprValue> combinedMap = new LinkedHashMap<>(leftTuple);
    combinedMap.putAll(rightTuple);
    return ExprTupleValue.fromExprValueMap(combinedMap);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return ImmutableList.of(left, right);
  }
}
