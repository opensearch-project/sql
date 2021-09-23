/*
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Span expression node.
 * Params include field expression and the span value.
 */
@Getter
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@ToString
public class Span extends UnresolvedExpression {
  private final UnresolvedExpression field;
  private final UnresolvedExpression value;
  private final SpanUnit unit;

  @Override
  public List<UnresolvedExpression> getChild() {
    return ImmutableList.of(field, value);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitSpan(this, context);
  }

}
