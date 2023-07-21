/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.statement;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/**
 * Query Statement.
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Query extends Statement {

  protected final UnresolvedPlan plan;
  protected final int fetchSize;

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> visitor, C context) {
    return visitor.visitQuery(this, context);
  }
}
