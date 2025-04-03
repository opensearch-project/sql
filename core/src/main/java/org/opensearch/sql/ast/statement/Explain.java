/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.ast.statement;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.executor.QueryType;

/** Explain Statement. */
@Data
@EqualsAndHashCode(callSuper = false)
public class Explain extends Statement {

  private final Statement statement;
  private final QueryType queryType;
  private final boolean codegen;

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> visitor, C context) {
    return visitor.visitExplain(this, context);
  }
}
