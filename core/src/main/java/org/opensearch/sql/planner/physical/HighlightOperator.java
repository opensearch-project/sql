/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;

@EqualsAndHashCode(callSuper = false)
@Getter
public class HighlightOperator extends PhysicalPlan {
  @NonNull
  private final PhysicalPlan input;
  private final Expression highlightField;

  public HighlightOperator(PhysicalPlan input, Expression highlightField) {
    this.input = input;
    this.highlightField = highlightField;
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitHighlight(this, context);
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
    ExprValue inputValue = input.next();
    ImmutableMap.Builder<String, ExprValue> mapBuilder = new ImmutableMap.Builder<>();
    inputValue.tupleValue().forEach(mapBuilder::put);

    return inputValue;
  }
}
