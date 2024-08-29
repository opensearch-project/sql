/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.join;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.physical.PhysicalPlan;

@RequiredArgsConstructor
public class HashJoinOperator extends JoinOperator {
  private final List<Expression> leftKeys;
  private final List<Expression> rightKeys;
  private final Join.JoinType joinType;
  private final PhysicalPlan left;
  private final PhysicalPlan right;
  private final Optional<Expression> nonEquiCond;

  private final ImmutableList.Builder<ExprValue> joinedBuilder = ImmutableList.builder();
  private Iterator<ExprValue> joinedIterator;

  @Override
  public void open() {
    // Build hash table from left
    left.open();
    Map<ExprValue, ExprValue> hashed = buildHashed();
    // Set streamed side to right
    right.open();
    Iterator<ExprValue> streamed = right;

    if (joinType == Join.JoinType.INNER) {
      innerJoin(streamed, hashed);
    } else if (joinType == Join.JoinType.LEFT) {
      leftOuterJoin(streamed, hashed);
    } else if (joinType == Join.JoinType.SEMI) {
      semiJoin(streamed, hashed);
    } else if (joinType == Join.JoinType.ANTI) {
      antiJoin(streamed, hashed);
    } else {
      throw new IllegalArgumentException("Unsupported join type: " + joinType);
    }
  }

  @Override
  public void close() {
    joinedIterator = null;
    left.close();
    right.close();
  }

  private void innerJoin(Iterator<ExprValue> streamed, Map<ExprValue, ExprValue> hashed) {
    while (streamed.hasNext()) {
      ExprValue rightRow = streamed.next();
      for (Expression rightKey : rightKeys) {
        ExprValue rightRowKey = rightKey.valueOf(rightRow.bindingTuples());
        if (rightRowKey != null && hashed.containsKey(rightRowKey)) {
          ExprValue leftRow = hashed.get(rightRowKey);
          ExprValue joinedRow = combineExprTupleValue(leftRow, rightRow);
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
    joinedIterator = joinedBuilder.build().iterator();
  }

  private void leftOuterJoin(Iterator<ExprValue> streamed, Map<ExprValue, ExprValue> hashed) {
    // Track matched keys to identify unmatched left rows later
    Set<ExprValue> matchedKeys = new HashSet<>();

    while (streamed.hasNext()) {
      ExprValue rightRow = streamed.next();
      for (Expression rightKey : rightKeys) {
        ExprValue rightRowKey = rightKey.valueOf(rightRow.bindingTuples());
        if (rightRowKey != null && hashed.containsKey(rightRowKey)) {
          ExprValue leftRow = hashed.get(rightRowKey);
          ExprValue joinedRow = combineExprTupleValue(leftRow, rightRow);
          if (nonEquiCond.isPresent()) {
            ExprValue conditionValue = nonEquiCond.get().valueOf(joinedRow.bindingTuples());
            if (!(conditionValue.isNull() || conditionValue.isMissing())
                && conditionValue.booleanValue()) {
              joinedBuilder.add(joinedRow);
              matchedKeys.add(rightRowKey);
            }
          } else {
            joinedBuilder.add(joinedRow);
            matchedKeys.add(rightRowKey);
          }
        }
      }
    }

    // Add unmatched left rows with nulls for the right side
    for (Map.Entry<ExprValue, ExprValue> entry : hashed.entrySet()) {
      if (!matchedKeys.contains(entry.getKey())) {
        ExprValue leftRow = entry.getValue();
        ExprValue joinedRow = combineExprTupleValue(leftRow, ExprValueUtils.nullValue());
        joinedBuilder.add(joinedRow);
      }
    }

    joinedIterator = joinedBuilder.build().iterator();
  }

  private void semiJoin(Iterator<ExprValue> streamed, Map<ExprValue, ExprValue> hashed) {
    Set<ExprValue> matchedKeys = new HashSet<>();

    while (streamed.hasNext()) {
      ExprValue rightRow = streamed.next();
      for (Expression rightKey : rightKeys) {
        ExprValue rightRowKey = rightKey.valueOf(rightRow.bindingTuples());
        if (rightRowKey != null && hashed.containsKey(rightRowKey)) {
          ExprValue leftRow = hashed.get(rightRowKey);
          if (nonEquiCond.isPresent()) {
            ExprValue joinedRow = combineExprTupleValue(leftRow, rightRow);
            ExprValue conditionValue = nonEquiCond.get().valueOf(joinedRow.bindingTuples());
            if (!(conditionValue.isNull() || conditionValue.isMissing())
                && conditionValue.booleanValue()) {
              matchedKeys.add(rightRowKey);
            }
          } else {
            matchedKeys.add(rightRowKey);
          }
        }
      }
    }

    // Add matched left rows to the result
    for (ExprValue key : matchedKeys) {
      joinedBuilder.add(hashed.get(key));
    }

    joinedIterator = joinedBuilder.build().iterator();
  }

  private void antiJoin(Iterator<ExprValue> streamed, Map<ExprValue, ExprValue> hashed) {
    Set<ExprValue> matchedKeys = new HashSet<>();

    while (streamed.hasNext()) {
      ExprValue rightRow = streamed.next();
      for (Expression rightKey : rightKeys) {
        ExprValue rightRowKey = rightKey.valueOf(rightRow.bindingTuples());
        if (rightRowKey != null && hashed.containsKey(rightRowKey)) {
          ExprValue leftRow = hashed.get(rightRowKey);
          if (nonEquiCond.isPresent()) {
            ExprValue joinedRow = combineExprTupleValue(leftRow, rightRow);
            ExprValue conditionValue = nonEquiCond.get().valueOf(joinedRow.bindingTuples());
            if (!(conditionValue.isNull() || conditionValue.isMissing())
                && conditionValue.booleanValue()) {
              matchedKeys.add(rightRowKey);
            }
          } else {
            matchedKeys.add(rightRowKey);
          }
        }
      }
    }

    // Add unmatched left rows to the result
    for (Map.Entry<ExprValue, ExprValue> entry : hashed.entrySet()) {
      if (!matchedKeys.contains(entry.getKey())) {
        joinedBuilder.add(entry.getValue());
      }
    }

    joinedIterator = joinedBuilder.build().iterator();
  }

  private Map<ExprValue, ExprValue> buildHashed() {
    ImmutableMap.Builder<ExprValue, ExprValue> leftTableBuilder = ImmutableMap.builder();
    while (left.hasNext()) {
      ExprValue row = left.next();
      for (Expression leftKey : leftKeys) {
        ExprValue rowKey = leftKey.valueOf(row.bindingTuples());
        if (rowKey != null) {
          leftTableBuilder.put(rowKey, row);
          break;
        }
      }
    }
    return leftTableBuilder.build();
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

  private ExprTupleValue combineExprTupleValue(ExprValue left, ExprValue right) {
    Map<String, ExprValue> combinedMap = left.tupleValue();
    combinedMap.putAll(right.tupleValue());
    return ExprTupleValue.fromExprValueMap(combinedMap);
  }
}
