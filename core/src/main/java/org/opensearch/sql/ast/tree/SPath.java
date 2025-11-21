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
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.dsl.AstDSL;

@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@AllArgsConstructor
@Getter
public class SPath extends UnresolvedPlan {
  private UnresolvedPlan child;

  private final String inField;

  @Nullable private final String outField;

  @Nullable private final String path;

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
    return nodeVisitor.visitSpath(this, context);
  }

  public Eval rewriteAsEval() {
    String outField = this.outField;
    if (outField == null) {
      outField = this.path;
    }

    return AstDSL.eval(
        this.child,
        AstDSL.let(
            AstDSL.field(outField),
            AstDSL.function(
                "json_extract", AstDSL.field(inField), AstDSL.stringLiteral(this.path))));
  }
}
