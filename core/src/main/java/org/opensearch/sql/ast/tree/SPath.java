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
    if (this.path == null) {
      return rewriteAsDynamicColumns();
    } else {
      return rewriteAsSpecificPath();
    }
  }

  private Eval rewriteAsDynamicColumns() {
    // For the first spath command, use json_extract_all directly
    // For subsequent spath commands, use map_merge to combine with existing _dynamic_columns
    // This matches the expected test behavior where the first spath creates _dynamic_columns
    // and subsequent ones merge with it
    return AstDSL.eval(
        this.child,
        AstDSL.let(
            AstDSL.field("_dynamic_columns"),
            AstDSL.function(
                "coalesce",
                AstDSL.function(
                    "map_merge",
                    AstDSL.field("_dynamic_columns"),
                    AstDSL.function("json_extract_all", AstDSL.field(inField))),
                AstDSL.function("json_extract_all", AstDSL.field(inField)))));
  }

  private Eval rewriteAsSpecificPath() {
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

  public boolean isDynamicColumns() {
    return this.path == null;
  }
}
