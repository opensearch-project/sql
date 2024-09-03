/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.join;

import static org.opensearch.sql.data.type.ExprCoreType.UNDEFINED;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

public abstract class JoinOperator extends PhysicalPlan {

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitJoin(this, context);
  }

  @Override
  public abstract List<PhysicalPlan> getChild();

  public abstract void innerJoin(Iterator<ExprValue> streamedSide);

  public abstract void outerJoin(Iterator<ExprValue> streamedSide);

  public abstract void semiJoin(Iterator<ExprValue> streamedSide);

  public abstract void antiJoin(Iterator<ExprValue> streamedSide);

  protected ExprTupleValue combineExprTupleValue(
      BuildSide buildSide, ExprValue streamedRow, ExprValue buildRow) {
    ExprValue left = buildSide == BuildSide.BuildLeft ? buildRow : streamedRow;
    ExprValue right = buildSide == BuildSide.BuildLeft ? streamedRow : buildRow;
    Map<String, ExprValue> leftTuple = left.type().equals(UNDEFINED) ? Map.of() : left.tupleValue();
    Map<String, ExprValue> rightTuple =
        right.type().equals(UNDEFINED) ? Map.of() : right.tupleValue();
    Map<String, ExprValue> combinedMap = new LinkedHashMap<>(leftTuple);
    combinedMap.putAll(rightTuple);
    return ExprTupleValue.fromExprValueMap(combinedMap);
  }

  protected boolean sameType(Expression expr1, Expression expr2) {
    ExprType type1 = expr1.type();
    ExprType type2 = expr2.type();
    return type1.isCompatible(type2);
  }

  public enum BuildSide {
    BuildLeft,
    BuildRight
  }
}
