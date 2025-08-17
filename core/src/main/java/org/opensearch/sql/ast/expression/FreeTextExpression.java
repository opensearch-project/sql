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
 * Expression representing free-text search terms that should be processed as search queries rather
 * than structured field references or comparisons. This helps distinguish between: - Free text:
 * "search holmes" where "holmes" is text to search for across all fields - Field reference: "search
 * status=404" where "status" is an actual field name
 *
 * <p>Free text expressions are typically converted to query_string function calls during the
 * translation phase, preserving boolean operators and search syntax.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class FreeTextExpression extends UnresolvedExpression {

  private final UnresolvedExpression expression;

  @Override
  public List<UnresolvedExpression> getChild() {
    return Collections.singletonList(expression);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitFreeTextExpression(this, context);
  }
}
