/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Expression node of lambda function. Params include function name (@funcName) and function
 * arguments (@funcArgs)
 */
@Getter
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class LambdaFunction extends UnresolvedExpression {
  private final UnresolvedExpression function;
  private final List<QualifiedName> funcArgs;

  @Override
  public List<UnresolvedExpression> getChild() {
    List<UnresolvedExpression> children = new ArrayList<>();
    children.add(function);
    children.addAll(funcArgs);
    return children;
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitLambdaFunction(this, context);
  }

  @Override
  public String toString() {
    return String.format(
        "(%s) -> %s",
        funcArgs.stream().map(Object::toString).collect(Collectors.joining(", ")),
        function.toString());
  }
}
