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
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Hash Join Operator. For best performance, the build side should be set a smaller table, without
 * hint and CBO, we treat right side as a smaller table by default and the build side set to right.
 * TODO add join hint support. Best practice in PPL: source=bigger | INNER JOIN smaller ON
 * bigger.field1 = smaller.field2 AND bigger.field3 = smaller.field4 The build side is right
 * (smaller), and the streamed side is left (bigger). For RIGHT OUTER join, the build side is always
 * left. If the smaller table is left, it will get the best performance: source=smaller | RIGHT JOIN
 * bigger ON bigger.field1 = smaller.field2 AND bigger.field3 = smaller.field4 The build side is
 * left (smaller), and the streamed side is right (bigger).
 */
public class HashJoinOperator extends JoinOperator {
  private final List<Expression> leftKeys;
  private final List<Expression> rightKeys;
  private final BuildSide buildSide;
  private final Optional<Expression> nonEquiCond;

  // write the construct method
  public HashJoinOperator(
      List<Expression> leftKeys,
      List<Expression> rightKeys,
      Join.JoinType joinType,
      BuildSide buildSide,
      PhysicalPlan left,
      PhysicalPlan right,
      Optional<Expression> nonEquiCond) {
    super(left, right, joinType);
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.buildSide = buildSide;
    this.nonEquiCond = nonEquiCond;
  }

  private final ImmutableList.Builder<ExprValue> joinedBuilder = ImmutableList.builder();
  private Iterator<ExprValue> joinedIterator;

  private HashedRelation hashed;
  private List<Expression> buildKeys;
  private List<Expression> streamedKeys;

  @Override
  public void open() {
    left.open();
    right.open();
    if (!(leftKeys.size() == rightKeys.size()
        && IntStream.range(0, leftKeys.size())
            .allMatch(i -> sameType(leftKeys.get(i), rightKeys.get(i))))) {
      throw new IllegalArgumentException(
          "Join keys from two sides should have same length and types");
    }

    Iterator<ExprValue> streamed;
    if (buildSide == BuildRight) {
      hashed = buildHashed(right, rightKeys);
      streamed = left;
      buildKeys = rightKeys;
      streamedKeys = leftKeys;
    } else {
      hashed = buildHashed(left, leftKeys);
      streamed = right;
      buildKeys = leftKeys;
      streamedKeys = rightKeys;
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
    if (hashed != null) {
      hashed.close();
      hashed = null;
    }
  }

  @Override
  public void innerJoin(Iterator<ExprValue> streamed) {
    while (streamed.hasNext()) {
      ExprValue streamedRow = streamed.next();

      for (Expression streamedKey : streamedKeys) {
        ExprValue streamedRowKey = streamedKey.valueOf(streamedRow.bindingTuples());
        if (streamedRowKey != null && hashed.containsKey(streamedRowKey)) {
          List<ExprValue> matchedBuildRows = hashed.get(streamedRowKey);
          for (ExprValue matchedBuildRow : matchedBuildRows) {
            ExprValue joinedRow = combineExprTupleValue(buildSide, streamedRow, matchedBuildRow);
            if (nonEquiCond.isPresent()) {
              ExprValue conditionValue = nonEquiCond.get().valueOf(joinedRow.bindingTuples());
              if (!(conditionValue.isNull() || conditionValue.isMissing())
                  && conditionValue.booleanValue()) {
                joinedBuilder.add(joinedRow);
              }
            } else {
              joinedBuilder.add(joinedRow);
            }
          }
        }
      }
    }
    joinedIterator = joinedBuilder.build().iterator();
  }

  /** The implementation for outer join: LeftOuter with BuildRight RightOuter with BuildLeft */
  @Override
  public void outerJoin(Iterator<ExprValue> streamed) {
    while (streamed.hasNext()) {
      ExprValue streamedRow = streamed.next();
      boolean matched = false;
      for (Expression streamedKey : streamedKeys) {
        ExprValue streamedRowKey = streamedKey.valueOf(streamedRow.bindingTuples());
        if (streamedRowKey != null && hashed.containsKey(streamedRowKey)) {
          List<ExprValue> matchedBuildRows = hashed.get(streamedRowKey);
          for (ExprValue matchedBuildRow : matchedBuildRows) {
            ExprValue joinedRow = combineExprTupleValue(buildSide, streamedRow, matchedBuildRow);
            if (nonEquiCond.isPresent()) {
              ExprValue conditionValue = nonEquiCond.get().valueOf(joinedRow.bindingTuples());
              if (!(conditionValue.isNull() || conditionValue.isMissing())
                  && conditionValue.booleanValue()) {
                joinedBuilder.add(joinedRow);
                matched = true;
              }
            } else {
              joinedBuilder.add(joinedRow);
              matched = true;
            }
          }
        } else {
          // if any streamedRowKey does not match, the remaining keys are not checked.
          matched = false;
          break;
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

  @Override
  public void semiJoin(Iterator<ExprValue> streamed) {
    Set<ExprValue> matchedRows = new HashSet<>();

    while (streamed.hasNext()) {
      ExprValue streamedRow = streamed.next();
      for (Expression streamedKey : streamedKeys) {
        ExprValue streamedRowKey = streamedKey.valueOf(streamedRow.bindingTuples());
        if (streamedRowKey != null && hashed.containsKey(streamedRowKey)) {
          List<ExprValue> matchedBuildRows = hashed.get(streamedRowKey);
          for (ExprValue matchedBuildRow : matchedBuildRows) {
            ExprValue joinedRow = combineExprTupleValue(buildSide, streamedRow, matchedBuildRow);
            if (nonEquiCond.isPresent()) {
              ExprValue conditionValue = nonEquiCond.get().valueOf(joinedRow.bindingTuples());
              if (!(conditionValue.isNull() || conditionValue.isMissing())
                  && conditionValue.booleanValue()) {
                matchedRows.add(streamedRow);
              }
            } else {
              matchedRows.add(streamedRow);
            }
          }
        } else {
          // if any streamedRowKey does not match, the remaining keys are not checked.
          break;
        }
      }
    }

    for (ExprValue row : matchedRows) {
      joinedBuilder.add(row);
    }

    joinedIterator = joinedBuilder.build().iterator();
  }

  @Override
  public void antiJoin(Iterator<ExprValue> streamed) {
    while (streamed.hasNext()) {
      ExprValue streamedRow = streamed.next();
      boolean matched = false;
      for (Expression streamedKey : streamedKeys) {
        ExprValue streamedRowKey = streamedKey.valueOf(streamedRow.bindingTuples());
        if (streamedRowKey != null && hashed.containsKey(streamedRowKey)) {
          List<ExprValue> matchedBuildRows = hashed.get(streamedRowKey);
          for (ExprValue matchedBuildRow : matchedBuildRows) {
            if (nonEquiCond.isPresent()) {
              ExprValue joinedRow = combineExprTupleValue(buildSide, streamedRow, matchedBuildRow);
              ExprValue conditionValue = nonEquiCond.get().valueOf(joinedRow.bindingTuples());
              if (!(conditionValue.isNull() || conditionValue.isMissing())
                  && conditionValue.booleanValue()) {
                matched = true;
              }
            } else {
              matched = true;
            }
          }
        } else {
          // if any streamedRowKey does not match, the remaining keys are not checked.
          matched = false;
          break;
        }
      }
      if (!matched) {
        joinedBuilder.add(streamedRow);
      }
    }

    joinedIterator = joinedBuilder.build().iterator();
  }

  private HashedRelation buildHashed(PhysicalPlan buildSide, List<Expression> buildKeys) {
    HashedRelation hashedRelation = new DefaultHashedRelation();
    while (buildSide.hasNext()) {
      ExprValue row = buildSide.next();
      for (Expression buildKey : buildKeys) {
        ExprValue rowKey = buildKey.valueOf(row.bindingTuples());
        if (rowKey != null) {
          hashedRelation.put(rowKey, row);
          break;
        }
      }
    }
    return hashedRelation;
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
