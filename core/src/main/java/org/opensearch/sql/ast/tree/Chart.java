/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** AST node represent chart command. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
@lombok.Builder(toBuilder = true)
public class Chart extends UnresolvedPlan {
  public static final Literal DEFAULT_USE_OTHER = Literal.TRUE;
  public static final Literal DEFAULT_OTHER_STR = AstDSL.stringLiteral("OTHER");
  public static final Literal DEFAULT_LIMIT = AstDSL.intLiteral(10);
  public static final Literal DEFAULT_USE_NULL = Literal.TRUE;
  public static final Literal DEFAULT_NULL_STR = AstDSL.stringLiteral("NULL");
  public static final Literal DEFAULT_TOP = Literal.TRUE;

  private UnresolvedPlan child;
  private UnresolvedExpression rowSplit;
  private UnresolvedExpression columnSplit;
  private List<UnresolvedExpression> aggregationFunctions;
  private List<Argument> arguments;

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitChart(this, context);
  }
}
