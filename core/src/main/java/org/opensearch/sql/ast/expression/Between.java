/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Arrays;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

/** Unresolved expression for BETWEEN. */
@Data
@EqualsAndHashCode(callSuper = false)
public class Between extends UnresolvedExpression {

  /** Value for range check. */
  private final UnresolvedExpression value;

  /** Lower bound of the range (inclusive). */
  private final UnresolvedExpression lowerBound;

  /** Upper bound of the range (inclusive). */
  private final UnresolvedExpression upperBound;

  @Override
  public List<? extends Node> getChild() {
    return Arrays.asList(value, lowerBound, upperBound);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitBetween(this, context);
  }
}
