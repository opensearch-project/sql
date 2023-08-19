/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.ast.expression;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.opensearch.analysis.OpenSearchAbstractNodeVisitor;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Expression node of a scalar function. Params include function name and function arguments. */
@Getter
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class OpenSearchFunction extends OpenSearchUnresolvedExpression {
  private final String funcName;
  private final List<UnresolvedExpression> funcArgs;

  @Override
  public List<UnresolvedExpression> getChild() {
    return Collections.unmodifiableList(funcArgs);
  }

  @Override
  public <R, C> R accept(OpenSearchAbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitOpenSearchFunction(this, context);
  }

  @Override
  public String toString() {
    return String.format(
        "%s (%s)",
        funcName, funcArgs.stream().map(Object::toString).collect(Collectors.joining(", ")));
  }
}
