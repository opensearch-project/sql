/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.statement;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.protocol.response.format.Format;

/** Explain Statement. */
@Getter
@EqualsAndHashCode(callSuper = false)
public class Explain extends Statement {

  private final Statement statement;
  private final QueryType queryType;
  private final ExplainMode mode;
  private final Format format;

  public Explain(Statement statement, QueryType queryType) {
    this(statement, queryType, null, null);
  }

  public Explain(Statement statement, QueryType queryType, String mode) {
    this(statement, queryType, mode, null);
  }

  public Explain(Statement statement, QueryType queryType, String mode, Format format) {
    this.statement = statement;
    this.queryType = queryType;
    this.mode = ExplainMode.of(mode);
    this.format = format;
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> visitor, C context) {
    return visitor.visitExplain(this, context);
  }
}
