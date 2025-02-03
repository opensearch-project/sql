/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.ReferenceExpression;

/** Flattens the specified field from the input and returns the result. */
@Getter
@ToString
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class ExpandOperator extends PhysicalPlan {

  private final PhysicalPlan input;
  private final ReferenceExpression field;

  private static final Pattern PATH_SEPARATOR_PATTERN = Pattern.compile(".", Pattern.LITERAL);

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

    // TODO #3016: Implement expand command
    return false;
  }

  @Override
  public ExprValue next() {

    // TODO #3016: Implement expand command
    return ExprValueUtils.nullValue();
  }
}
