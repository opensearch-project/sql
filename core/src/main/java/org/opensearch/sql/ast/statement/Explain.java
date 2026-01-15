/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.ast.statement;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.executor.QueryType;

/** Explain Statement. */
@Getter
@EqualsAndHashCode(callSuper = false)
public class Explain extends Statement {

  private final Statement statement;
  private final QueryType queryType;
  private final ExplainMode mode;

  public Explain(Statement statement, QueryType queryType) {
    this(statement, queryType, null);
  }

  public Explain(Statement statement, QueryType queryType, String mode) {
    this.statement = statement;
    this.queryType = queryType;
    this.mode = ExplainMode.of(mode);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> visitor, C context) {
    return visitor.visitExplain(this, context);
  }
}
