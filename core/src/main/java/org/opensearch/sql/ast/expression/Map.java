/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Arrays;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/** Expression node of one-to-one mapping relation. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Map extends UnresolvedExpression {
  private final UnresolvedExpression origin;
  private final UnresolvedExpression target;

  @Override
  public List<UnresolvedExpression> getChild() {
    return Arrays.asList(origin, target);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitMap(this, context);
  }
}
