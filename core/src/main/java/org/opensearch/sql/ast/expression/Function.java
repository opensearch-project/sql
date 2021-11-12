/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;


/**
 * Expression node of scalar function.
 * Params include function name (@funcName) and function arguments (@funcArgs)
 */
@Getter
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Function extends UnresolvedExpression {
  private final String funcName;
  private final List<UnresolvedExpression> funcArgs;

  @Override
  public List<UnresolvedExpression> getChild() {
    return Collections.unmodifiableList(funcArgs);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitFunction(this, context);
  }

  @Override
  public String toString() {
    return String.format("%s(%s)", funcName,
        funcArgs.stream()
            .map(Object::toString)
            .collect(Collectors.joining(", ")));
  }
}
