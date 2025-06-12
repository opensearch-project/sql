/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression.subquery;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.utils.StringUtils;

@Getter
@EqualsAndHashCode(callSuper = true)
@RequiredArgsConstructor
public class InSubquery extends SubqueryExpression {
  private final List<UnresolvedExpression> value;
  private final UnresolvedPlan query;

  @Override
  public List<UnresolvedExpression> getChild() {
    return value;
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitInSubquery(this, context);
  }

  @Override
  public String toString() {
    return StringUtils.format("%s in ( %s )", value, query);
  }
}
