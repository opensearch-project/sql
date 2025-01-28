/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;

@Getter
@ToString
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class FlattenOperator extends PhysicalPlan {

  private final PhysicalPlan input;
  private final ReferenceExpression field;

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitFlatten(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public boolean hasNext() {
    return input.hasNext();
  }

  @Override
  public ExprValue next() {

    ExprValue inputExprValue = input.next();
    Map<String, ExprValue> fieldsMap = ExprValueUtils.getTupleValue(inputExprValue);

    // Build the flattened field map.
    String fieldName = field.getAttr();
    ExprValue exprValue = fieldsMap.get(fieldName);

    Map<String, ExprValue> flattenedFieldsMap = exprValue.tupleValue();

    // Update field map.
    fieldsMap.putAll(flattenedFieldsMap);
    fieldsMap.remove(fieldName);

    // Update the environment.
    Environment<Expression, ExprValue> env = inputExprValue.bindingTuples();

    for (Entry<String, ExprValue> entry : flattenedFieldsMap.entrySet()) {
      ExprValue fieldValue = entry.getValue();
      Expression fieldRefExp = new ReferenceExpression(entry.getKey(), fieldValue.type());
      Environment.extendEnv(env, fieldRefExp, fieldValue);
    }

    return ExprTupleValue.fromExprValueMap(fieldsMap);
  }
}
