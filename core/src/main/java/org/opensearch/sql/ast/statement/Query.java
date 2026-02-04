/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.statement;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.executor.QueryType;

/** Query Statement. */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Query extends Statement {

  protected final UnresolvedPlan plan;
  protected final int fetchSize;
  private final QueryType queryType;

  public Query(UnresolvedPlan plan, int fetchSize, QueryType queryType) {
    this.plan = plan;
    this.fetchSize = fetchSize;
    this.queryType = queryType;
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> visitor, C context) {
    return visitor.visitQuery(this, context);
  }
}
