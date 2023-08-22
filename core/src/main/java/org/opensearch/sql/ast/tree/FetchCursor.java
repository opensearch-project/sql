/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/** An unresolved plan that represents fetching the next batch in paginated plan. */

@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class FetchCursor extends UnresolvedPlan {
  @Getter final String cursor;

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitFetchCursor(this, context);
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    throw new UnsupportedOperationException("Cursor unresolved plan does not support children");
  }
}
