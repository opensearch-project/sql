/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * AST node.
 */
@EqualsAndHashCode
@ToString
public abstract class Node {

  public <R, C> R accept(AbstractNodeVisitor<R, C> visitor, C context) {
    return visitor.visitChildren(this, context);
  }

  public List<? extends Node> getChild() {
    return null;
  }
}
