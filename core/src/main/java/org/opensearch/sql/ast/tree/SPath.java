/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import static org.opensearch.sql.common.utils.StringUtils.unquoteText;

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

/**
 * AST node for the PPL {@code spath} command. Supports two modes:
 *
 * <ul>
 *   <li>Path-based extraction ({@code path} is non-null): rewrites to {@code eval output =
 *       json_extract(input, path)} via {@link #rewriteAsEval()}.
 *   <li>Extract-all mode ({@code path} is null): rewrites to {@code eval output =
 *       json_extract_all(input)} via {@link #rewriteAsExtractAllEval()}, returning a {@code
 *       map<string, any>} with flattened keys (dotted for nested objects, {@code {}} suffix for
 *       arrays).
 * </ul>
 *
 * <p>The {@code input} parameter is always required. When {@code output} is omitted, it defaults to
 * the path value (path mode) or the input field name (extract-all mode).
 */
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@AllArgsConstructor
@Getter
public class SPath extends UnresolvedPlan {
  private UnresolvedPlan child;

  private final String inField;

  @Nullable private final String outField;

  private final String path;

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
    String unquotedPath = unquoteText(this.path);
    if (outField == null) {
      outField = unquotedPath;
    }

    return AstDSL.eval(
        this.child,
        AstDSL.let(
            AstDSL.field(outField),
            AstDSL.function(
                "json_extract", AstDSL.field(inField), AstDSL.stringLiteral(unquotedPath))));
  }

  public Eval rewriteAsExtractAllEval() {
    String outField = this.outField != null ? this.outField : this.inField;
    return AstDSL.eval(
        this.child,
        AstDSL.let(
            AstDSL.field(outField), AstDSL.function("json_extract_all", AstDSL.field(inField))));
  }
}
