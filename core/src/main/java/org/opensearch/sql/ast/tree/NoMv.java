/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;

/**
 * AST node for the NOMV command. Converts multi-value fields to single-value fields by joining
 * array elements with newline delimiter.
 */
@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class NoMv extends UnresolvedPlan {

  private final Field field;
  @Nullable private UnresolvedPlan child;

  public NoMv(Field field) {
    this.field = field;
  }

  public NoMv attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return child == null ? ImmutableList.of() : ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitNoMv(this, context);
  }

  /**
   * Rewrites the nomv command as an eval command using mvjoin function. nomv <field> is rewritten
   * to: eval <field> = mvjoin(<field>, "\n")
   *
   * @return an Eval node representing the equivalent mvjoin operation
   */
  public UnresolvedPlan rewriteAsEval() {
    // Create mvjoin function call: mvjoin(field, "\n")
    Function mvjoinFunc =
        new Function("mvjoin", ImmutableList.of(field, new Literal("\n", DataType.STRING)));

    // Create eval expression: field = mvjoin(field, "\n")
    Let letExpr = new Let(field, mvjoinFunc);

    // Create eval node and attach the child from this NoMv node
    Eval eval = new Eval(ImmutableList.of(letExpr));
    if (this.child != null) {
      eval.attach(this.child);
    }
    return eval;
  }
}
