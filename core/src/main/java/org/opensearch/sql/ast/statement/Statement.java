/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.statement;

import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

/**
 * Statement is the high interface of core engine.
 */
public abstract class Statement extends Node {
  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> visitor, C context) {
    return visitor.visitStatement(this, context);
  }
}
