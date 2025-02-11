/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
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
    ExprValue rootExprValue = input.next();
    String qualifiedName = field.getAttr();

    if (!ExprValueUtils.containsNestedExprValue(rootExprValue, qualifiedName)) {
      return rootExprValue;
    }

    ExprValue flattenExprValue = ExprValueUtils.getNestedExprValue(rootExprValue, qualifiedName);
    if (flattenExprValue.isNull() || flattenExprValue.isMissing()) {
      return rootExprValue;
    }

    return flattenNestedExprValue(rootExprValue, qualifiedName);
  }

  /**
   * Flattens the nested {@link ExprTupleValue} with the specified qualified name within the given
   * root value and returns the result. Requires that the root value contain a nested value with the
   * qualified name - see {@link ExprValueUtils#containsNestedExprValue}.
   */
  private static ExprValue flattenNestedExprValue(ExprValue rootExprValue, String qualifiedName) {

    Map<String, ExprValue> exprValueMap = ExprValueUtils.getTupleValue(rootExprValue);

    List<String> qualifiedNameComponents = ExprValueUtils.splitQualifiedName(qualifiedName);
    String currentQualifiedNameComponent = qualifiedNameComponents.getFirst();
    ExprValue childExprValue = exprValueMap.get(currentQualifiedNameComponent);

    // Get flattened values and add them to the field map.
    Map<String, ExprValue> flattenedExprValueMap;
    if (qualifiedNameComponents.size() > 1) {
      String remainingQualifiedName =
          ExprValueUtils.joinQualifiedName(
              qualifiedNameComponents.subList(1, qualifiedNameComponents.size()));

      flattenedExprValueMap =
          Map.of(
              currentQualifiedNameComponent,
              flattenNestedExprValue(childExprValue, remainingQualifiedName));
    } else {
      flattenedExprValueMap = ExprValueUtils.getTupleValue(childExprValue);
    }

    exprValueMap.putAll(flattenedExprValueMap);
    return ExprTupleValue.fromExprValueMap(exprValueMap);
  }
}
