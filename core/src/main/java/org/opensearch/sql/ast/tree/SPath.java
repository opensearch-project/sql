/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.calcite.CalcitePlanContext;

import static org.opensearch.sql.common.utils.StringUtils.unquoteIdentifier;

@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@AllArgsConstructor
public class SPath extends UnresolvedPlan {
    private final char DOT = '.';
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

  private String fullPath() {
    return this.inField + DOT + this.path;
  }

  /**
   * We want input=outer, path=inner.data to match records like `{ "outer": { "inner": "{\"data\":
   * 0}" }}`. To rewrite this as eval, that means we need to detect the longest prefix match in the
   * fields (`outer.inner`) and parse `data` out of it. We need to match on segments, so
   * `outer.inner` shouldn't match `outer.inner_other`.
   *
   * @return The field from the RelBuilder with the most overlap, or inField if none exists.
   */
  private String computePathField(RelBuilder builder) {
    RelDataType rowType = builder.peek().getRowType();
    List<String> rowFieldNames = rowType.getFieldNames();

    String result = this.inField;
    String matchField = this.fullPath() + DOT; // Trailing '.' simplifies checking for segments

    for (String name : rowFieldNames) {
      if (!matchField.startsWith(name)) {
        continue;
      }
      if (name.length() > result.length() && matchField.charAt(name.length()) == '.') {
        result = name;
      }
    }

    return result;
  }

  /**
   * Convert this `spath` expression to an equivalent `json_extract` eval.
   *
   * @param context The planning context for the rewrite, which has access to the available fields.
   * @return The rewritten expression.
   */
  public Eval rewriteAsEval(CalcitePlanContext context) {
    String outField = this.outField;
    if (outField == null) {
      outField = unquoteIdentifier(this.path);
    }

    String pathField = computePathField(context.relBuilder);
    String reducedPath = this.fullPath().substring(pathField.length());

    String[] pathFieldParts = unquoteIdentifier(pathField).split("\\.");

    if (reducedPath.isEmpty()) {
      // Special case: We're spath-extracting a path that already exists in the data. This is just a
      // rename.
      return AstDSL.eval(
          this.child,
          AstDSL.let(AstDSL.field(outField), AstDSL.field(AstDSL.qualifiedName(pathFieldParts))));
    }
    // Since pathField must be on a segment line, there's a leftover leading dot if we didn't match
    // the whole path.
    reducedPath = reducedPath.substring(1);

    return AstDSL.eval(
        this.child,
        AstDSL.let(
            AstDSL.field(outField),
            AstDSL.function(
                "json_extract",
                AstDSL.field(AstDSL.qualifiedName(pathFieldParts)),
                AstDSL.stringLiteral(unquoteIdentifier(reducedPath)))));
  }
}
