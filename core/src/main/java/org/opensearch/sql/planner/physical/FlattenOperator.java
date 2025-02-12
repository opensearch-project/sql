/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.commons.math3.analysis.function.Exp;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.ReferenceExpression;

/** Flattens the specified field from the input and returns the result. */
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
    return flattenNestedExprValue(input.next(), field.getAttr());
  }

  /**
   * Flattens the nested {@link ExprTupleValue} with the specified qualified name within the given
   * root value, and returns the result. If the root value does not contain a nested value with the
   * qualified name, or if the nested value is null or missing, returns the unmodified root value.
   * Raises {@link org.opensearch.sql.exception.SemanticCheckException} if the root value or nested
   * value is not an {@link ExprTupleValue}.
   */
  private static ExprValue flattenNestedExprValue(ExprValue rootExprValue, String qualifiedName) {

    // Get current field name.
    List<String> components = ExprValueUtils.splitQualifiedName(qualifiedName);
    String fieldName = components.getFirst();

    // Check if the child value is undefined.
    Map<String, ExprValue> fieldsMap = rootExprValue.tupleValue();
    if (!fieldsMap.containsKey(fieldName)) {
      return rootExprValue;
    }

    // Check if the child value is null or missing.
    ExprValue childExprValue = fieldsMap.get(fieldName);
    if (childExprValue.isNull() || childExprValue.isMissing()) {
      return rootExprValue;
    }

    // Flatten the child value.
    Map<String, ExprValue> flattenedChildFieldMap;

    if (components.size() == 1) {
      flattenedChildFieldMap = childExprValue.tupleValue();
    } else {
      String remainingQualifiedName =
          ExprValueUtils.joinQualifiedName(components.subList(1, components.size()));
      ExprValue flattenedChildExprValue =
          flattenNestedExprValue(childExprValue, remainingQualifiedName);
      flattenedChildFieldMap = Map.of(fieldName, flattenedChildExprValue);
    }

    // Build flattened value.
    Map<String, ExprValue> newFieldsMap = new  HashMap<>(fieldsMap);
    newFieldsMap.putAll(flattenedChildFieldMap);

    return ExprTupleValue.fromExprValueMap(newFieldsMap);
  }
}
