/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
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
    return flattenExprValueAtPath(input.next(), field.getAttr());
  }

  /**
   * Flattens the {@link ExprTupleValue} at the specified path within the given root value and
   * returns the result. Returns the unmodified root value if it does not contain a value at the
   * specified path. rootExprValue is expected to be an {@link ExprTupleValue}.
   */
  private static ExprValue flattenExprValueAtPath(ExprValue rootExprValue, String path) {

    Map<String, ExprValue> exprValueMap = ExprValueUtils.getTupleValue(rootExprValue);

    // Get current path component.
    Matcher matcher = ExprValueUtils.QUALIFIED_NAME_SEPARATOR_PATTERN.matcher(path);
    boolean fieldIsNested = matcher.find();
    String currentPathComponent = fieldIsNested ? path.substring(0, matcher.start()) : path;

    // Check for undefined, null, or missing values.
    if (!exprValueMap.containsKey(currentPathComponent)) {
      return rootExprValue;
    }

    ExprValue childExprValue = exprValueMap.get(currentPathComponent);
    if (childExprValue.isNull() || childExprValue.isMissing()) {
      return rootExprValue;
    }

    // Get flattened values and add them to the field map.
    Map<String, ExprValue> flattenedExprValueMap;
    if (fieldIsNested) {
      String remainingPath = path.substring(matcher.end());
      flattenedExprValueMap =
          Map.of(
              currentPathComponent,
              flattenExprValueAtPath(exprValueMap.get(currentPathComponent), remainingPath));
    } else {
      flattenedExprValueMap = ExprValueUtils.getTupleValue(childExprValue);
    }

    exprValueMap.putAll(flattenedExprValueMap);
    return ExprTupleValue.fromExprValueMap(exprValueMap);
  }
}
