/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.ast.expression;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.opensearch.analysis.OpenSearchAbstractNodeVisitor;

@EqualsAndHashCode(callSuper = false)
@ToString
public abstract class OpenSearchUnresolvedExpression extends UnresolvedExpression {

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    if (nodeVisitor instanceof OpenSearchAbstractNodeVisitor) {
      return accept((OpenSearchAbstractNodeVisitor<T, C>) nodeVisitor, context);
    }
    throw new RuntimeException();
  }

  public <T, C> T accept(OpenSearchAbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitChildren(this, context);
  }
}

