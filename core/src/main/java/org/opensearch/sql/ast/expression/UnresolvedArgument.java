/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;

import java.util.Arrays;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Argument.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class UnresolvedArgument extends UnresolvedExpression {
  private final String argName;
  private final UnresolvedExpression value;

  public UnresolvedArgument(String argName, UnresolvedExpression value) {
    this.argName = argName;
    this.value = value;
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return Arrays.asList(value);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitUnresolvedArgument(this, context);
  }
}
