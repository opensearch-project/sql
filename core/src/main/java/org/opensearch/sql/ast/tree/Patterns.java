/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.PatternMethod;
import org.opensearch.sql.ast.expression.PatternMode;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * Logical plan node of Patterns command to represent complex nested function calling than single
 * window function.
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@AllArgsConstructor
public class Patterns extends UnresolvedPlan {

  private final UnresolvedExpression sourceField;
  private final List<UnresolvedExpression> partitionByList;
  private final String alias;
  private final PatternMethod patternMethod;
  private final PatternMode patternMode;
  private final UnresolvedExpression patternMaxSampleCount;
  private final UnresolvedExpression patternBufferLimit;
  private final Map<String, Literal> arguments;
  private UnresolvedPlan child;

  @Override
  public Patterns attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitPatterns(this, context);
  }
}
