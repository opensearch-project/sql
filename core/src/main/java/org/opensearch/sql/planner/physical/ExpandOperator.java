/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.ReferenceExpression;

/** Expands the specified field from the input and returns the result. */
@Getter
@ToString
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class ExpandOperator extends PhysicalPlan {

  private final PhysicalPlan input;
  private final ReferenceExpression field;

  private Queue<ExprValue> expandedRows = new LinkedList<>();

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitExpand(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public boolean hasNext() {
    while (expandedRows.isEmpty() && input.hasNext()) {
      expandedRows = new LinkedList<>(expandNestedExprValue(input.next(), field.getAttr()));
    }

    return !expandedRows.isEmpty();
  }

  @Override
  public ExprValue next() {
    return expandedRows.remove();
  }

  /**
   * Expands the nested {@link ExprCollectionValue} with the specified qualified name within the
   * given root value, and returns the results. If the root value does not contain a nested value
   * with the qualified name, if the nested value is null or missing, or if the nested value is not
   * an {@link ExprCollectionValue}, returns the unmodified root value.
   *
   * @throws SemanticCheckException if the root value is not an {@link ExprTupleValue}.
   */
  private static List<ExprValue> expandNestedExprValue(
      ExprValue rootExprValue, String qualifiedName) {

    // Get current field name.
    List<String> components = ExprValueUtils.splitQualifiedName(qualifiedName);
    String fieldName = components.getFirst();

    // Check if the child value is undefined.
    Map<String, ExprValue> fieldsMap = rootExprValue.tupleValue();
    if (!fieldsMap.containsKey(fieldName)) {
      return List.of(rootExprValue);
    }

    // Check if the child value is null or missing.
    ExprValue childExprValue = fieldsMap.get(fieldName);
    if (childExprValue.isNull() || childExprValue.isMissing()) {
      return List.of(rootExprValue);
    }

    // Expand the child value.
    List<ExprValue> expandedChildExprValues;
    if (components.size() == 1) {
      expandedChildExprValues = expandExprValue(childExprValue);
    } else {
      String remainingQualifiedName =
          ExprValueUtils.joinQualifiedName(components.subList(1, components.size()));
      expandedChildExprValues = expandNestedExprValue(childExprValue, remainingQualifiedName);
    }

    // Build expanded values.
    List<ExprValue> expandedExprValues = new LinkedList<>();

    for (ExprValue expanded : expandedChildExprValues) {
      Map<String, ExprValue> newFieldsMap = new HashMap<>(fieldsMap);
      newFieldsMap.put(fieldName, expanded);
      expandedExprValues.add(ExprTupleValue.fromExprValueMap(newFieldsMap));
    }

    return expandedExprValues;
  }

  /** Expands the given value and returns the results. */
  private static List<ExprValue> expandExprValue(ExprValue exprValue) {
    if (!exprValue.type().equals(ARRAY)) {
      return List.of(exprValue);
    }

    List<ExprValue> collectionExprValues = exprValue.collectionValue();

    // Expand an empyt collection expands to a null value.
    if (collectionExprValues.isEmpty()) {
      return List.of(ExprValueUtils.nullValue());
    }

    return collectionExprValues;
  }
}
