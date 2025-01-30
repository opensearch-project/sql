/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.ReferenceExpression;

@Getter
@ToString
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class FlattenOperator extends PhysicalPlan {

  private final PhysicalPlan input;
  private final ReferenceExpression field;

  private static final String PATH_SEPARATOR = ".";
  private static final Pattern PATH_SEPARATOR_PATTERN =
      Pattern.compile(PATH_SEPARATOR, Pattern.LITERAL);

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

  /** Flattens the value at the specified path and returns the result. */
  private static ExprValue flattenExprValueAtPath(ExprValue value, String path) {

    Matcher matcher = PATH_SEPARATOR_PATTERN.matcher(path);
    Map<String, ExprValue> exprValueMap = ExprValueUtils.getTupleValue(value);

    if (matcher.find()) {
      String currentPathComponent = path.substring(0, matcher.start());
      String remainingPath = path.substring(matcher.end());

      ExprValue flattenedExprValue =
          flattenExprValueAtPath(exprValueMap.get(currentPathComponent), remainingPath);
      exprValueMap.put(currentPathComponent, flattenedExprValue);
    } else {
      exprValueMap.putAll(ExprValueUtils.getTupleValue(exprValueMap.get(path)));
      exprValueMap.remove(path);
    }

    return ExprTupleValue.fromExprValueMap(exprValueMap);
  }
}
