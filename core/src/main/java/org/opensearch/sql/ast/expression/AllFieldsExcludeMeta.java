/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

/**
 * Represent the All fields but excluding metadata fields if user never uses them in the previous
 * fields command
 */
@ToString
@EqualsAndHashCode(callSuper = false)
public class AllFieldsExcludeMeta extends AllFields {
  public static final AllFieldsExcludeMeta INSTANCE = new AllFieldsExcludeMeta();

  private AllFieldsExcludeMeta() {
    super();
  }

  public static AllFieldsExcludeMeta of() {
    return INSTANCE;
  }

  @Override
  public List<? extends Node> getChild() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitAllFieldsExcludeMeta(this, context);
  }
}
