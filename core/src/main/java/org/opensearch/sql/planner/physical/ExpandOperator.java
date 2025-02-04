/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.utils.PathUtils;

/** Flattens the specified field from the input and returns the result. */
@Getter
@ToString
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class ExpandOperator extends PhysicalPlan {

  private final PhysicalPlan input;
  private final ReferenceExpression field;

  private List<ExprValue> expandedRows = List.of();

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
      expandedRows = expandValue(input.next(), field.getAttr());
    }

    return expandedRows.isEmpty();
  }

  @Override
  public ExprValue next() {
    return expandedRows.removeFirst();
  }

  /**
   * Expands the {@link org.opensearch.sql.data.model.ExprCollectionValue} at the specified path and
   * returns the resulting value. If the value is null or missing, the unmodified value is returned.
   */
  private static List<ExprValue> expandValue(ExprValue rootExprValue, String path) {
    ExprValue targetExprValue = PathUtils.getExprValueAtPath(rootExprValue, path);
    return targetExprValue.collectionValue().stream()
        .map(v -> PathUtils.setExprValueAtPath(rootExprValue, path, v))
        .collect(Collectors.toCollection(LinkedList::new));
  }
}
