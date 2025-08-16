/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Marker expression to indicate that the wrapped expression should be treated as search text rather
 * than a field reference or other expression type. This helps distinguish between: - Search text:
 * "search Street" where Street is text to search for - Field reference: "search field=value" where
 * field is an actual field name
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class SearchMarkerExpression extends UnresolvedExpression {

  private final UnresolvedExpression expression;

  @Override
  public List<UnresolvedExpression> getChild() {
    return Collections.singletonList(expression);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitSearchMarkerExpression(this, context);
  }
}
