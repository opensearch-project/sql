/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Interval extends UnresolvedExpression {

  private final UnresolvedExpression value;
  private final IntervalUnit unit;

  public Interval(UnresolvedExpression value, String unit) {
    this.value = value;
    this.unit = IntervalUnit.of(unit);
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return List.of(value);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitInterval(this, context);
  }
}
