/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.join;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

public abstract class JoinOperator extends PhysicalPlan {
  protected PhysicalPlan left;
  protected PhysicalPlan right;
  protected Join.JoinType joinType;

  protected ExecutionEngine.Schema leftSchema;
  protected ExecutionEngine.Schema rightSchema;
  protected ExecutionEngine.Schema outputSchema;

  JoinOperator(PhysicalPlan left, PhysicalPlan right, Join.JoinType joinType) {
    this.left = left;
    this.right = right;
    this.joinType = joinType;
    this.leftSchema = left.schema();
    this.rightSchema = right.schema();
    getOutputSchema();
  }

  private void getOutputSchema() {
    switch (joinType) {
      case INNER, LEFT, RIGHT, FULL -> { // merge left and right schemas
        List<ExecutionEngine.Schema.Column> columns =
            Stream.of(left.schema().getColumns(), right.schema().getColumns())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        this.outputSchema = new ExecutionEngine.Schema(columns);
      }
      case SEMI, ANTI -> outputSchema = left.schema(); // left schema only
      default -> throw new UnsupportedOperationException("Unsupported Join Type " + joinType);
    }
  }

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
    Map<String, ExprValue> leftTuple = getExprTupleMapFromSchema(left, leftSchema);
    Map<String, ExprValue> rightTuple = getExprTupleMapFromSchema(right, rightSchema);
    Map<String, ExprValue> combinedMap = new LinkedHashMap<>(leftTuple);
    combinedMap.putAll(rightTuple);
    return ExprTupleValue.fromExprValueMap(combinedMap);
  }

  private Map<String, ExprValue> getExprTupleMapFromSchema(
      ExprValue row, ExecutionEngine.Schema schema) {
    Map<String, ExprValue> map = new LinkedHashMap<>();
    if (row.isNull()) {
      schema.getColumns().forEach(col -> map.put(col.getAlias(), ExprNullValue.of()));
    } else {
      // replace to indexName.fieldName as tupleMap key in case the field names are same in join
      // tables.
      schema
          .getColumns()
          .forEach(col -> map.put(col.getAlias(), row.tupleValue().get(col.getName())));
    }
    return map;
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
