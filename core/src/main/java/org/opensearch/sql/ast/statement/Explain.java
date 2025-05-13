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

import java.util.Locale;

/** Explain Statement. */
@Getter
@EqualsAndHashCode(callSuper = false)
public class Explain extends Statement {

  private final Statement statement;
  private final QueryType queryType;
  private final ExplainFormat format;

  public Explain(Statement statement, QueryType queryType) {
    this(statement, queryType, null);
  }

  public Explain(Statement statement, QueryType queryType, String format) {
    this.statement = statement;
    this.queryType = queryType;
    this.format = Explain.format(format);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> visitor, C context) {
    return visitor.visitExplain(this, context);
  }

  public enum ExplainFormat {
    SIMPLE,
    STANDARD,
    EXTENDED,
    COST
  }

  public static ExplainFormat format(String format) {
    try {
      return ExplainFormat.valueOf(format.toUpperCase(Locale.ROOT));
    } catch (Exception e) {
      return ExplainFormat.STANDARD;
    }
  }
}
