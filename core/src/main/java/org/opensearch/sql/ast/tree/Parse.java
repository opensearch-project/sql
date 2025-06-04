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
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** AST node represent Parse with regex operation. */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@AllArgsConstructor
public class Parse extends UnresolvedPlan {
  /** Method used to parse a field. */
  private final ParseMethod parseMethod;

  /** Field. */
  private final UnresolvedExpression sourceField;

  /** Pattern. */
  private final Literal pattern;

  /** Optional arguments. */
  private final Map<String, Literal> arguments;

  /** Child Plan. */
  private UnresolvedPlan child;

  @Override
  public Parse attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitParse(this, context);
  }
}
