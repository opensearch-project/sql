/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.AddTotals;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.MvCombine;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Replace;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/**
 * Pre-materializes MAP dotted paths for commands that match fields by name. When a command like
 * {@code rename}, {@code fillnull}, or {@code replace} references a dotted path such as {@code
 * doc.user.name}, this class resolves it to an {@code ITEM()} expression and projects it as a flat
 * named column so the command's string-matching logic can find it in the schema.
 *
 * <p>Extracts operands directly from known AST node types instead of relying on {@link
 * UnresolvedPlan#getOperands()}, to avoid adding engine-specific contracts to the shared AST layer.
 */
public class MapPathMaterializer {

  private final CalciteRexNodeVisitor rexVisitor;

  public MapPathMaterializer(CalciteRexNodeVisitor rexVisitor) {
    this.rexVisitor = rexVisitor;
  }

  /**
   * Materializes MAP dotted paths referenced by the given AST node, then returns the result
   * RelNode. For each field operand that resolves to an {@code ITEM()} access on a MAP column,
   * projects it as a flat named column so the command's string-matching logic can find it.
   *
   * @param result the RelNode produced by visiting children
   * @param node the AST node being visited
   * @param context the current plan context with relBuilder state
   * @return the result RelNode (unchanged, but schema may have been enriched via relBuilder)
   */
  public RelNode materializePaths(RelNode result, Node node, CalcitePlanContext context) {
    if (node instanceof UnresolvedPlan plan) {
      List<UnresolvedExpression> operands = extractOperands(plan);
      if (!operands.isEmpty() && context.relBuilder.size() > 0) {
        resolveAndProject(operands, context);
      }
    }
    return result;
  }

  private void resolveAndProject(List<UnresolvedExpression> operands, CalcitePlanContext context) {
    List<String> currentFields = context.relBuilder.peek().getRowType().getFieldNames();
    List<RexNode> toMaterialize = new ArrayList<>();
    List<String> newNames = new ArrayList<>();

    for (UnresolvedExpression operand : operands) {
      UnresolvedExpression inner = operand;
      if (inner instanceof Alias alias) {
        inner = alias.getDelegated();
      }
      if (!(inner instanceof Field)) continue;
      String fieldName = ((Field) inner).getField().toString();
      if (currentFields.contains(fieldName)) continue;
      try {
        RexNode resolved = rexVisitor.analyze(inner, context);
        if (resolved.getKind() == SqlKind.ITEM) {
          toMaterialize.add(resolved);
          newNames.add(fieldName);
        }
      } catch (Exception e) {
        // Field can't be resolved — skip silently, let the command handle the error
      }
    }

    if (!toMaterialize.isEmpty()) {
      // Use projectPlus (not projectPlusOverriding) to preserve the MAP column.
      // projectPlusOverriding would remove the MAP column "doc" when adding "doc.user.name"
      // because shouldOverrideField detects "doc.user.name".startsWith("doc.").
      context.relBuilder.projectPlus(toMaterialize);
      List<String> currentFieldsAfter = context.relBuilder.peek().getRowType().getFieldNames();
      int total = currentFieldsAfter.size();
      List<String> renamed =
          new ArrayList<>(currentFieldsAfter.subList(0, total - newNames.size()));
      renamed.addAll(newNames);
      context.relBuilder.rename(renamed);
    }
  }

  /**
   * Extracts field operands from known AST node types that do symbol-based field matching. This
   * avoids adding a {@code getOperands()} contract to the shared AST layer while still enabling
   * centralized MAP path materialization for affected commands.
   *
   * <p>Only covers Category A commands (symbol-based field name matching). Category B (missing
   * alias wrapping) and Category C (fragmented resolve API) will be addressed in follow-up PRs.
   */
  private static List<UnresolvedExpression> extractOperands(UnresolvedPlan node) {
    return switch (node) {
      case Rename rename ->
          rename.getRenameList().stream()
              .map(m -> (UnresolvedExpression) m.getOrigin())
              .collect(Collectors.toList());
      case FillNull fillNull ->
          fillNull.getReplacementPairs().stream()
              .map(p -> (UnresolvedExpression) p.getLeft())
              .collect(Collectors.toList());
      case Replace replace ->
          replace.getFieldList().stream()
              .map(f -> (UnresolvedExpression) f)
              .collect(Collectors.toList());
      case Project project ->
          project.isExcluded()
              ? new ArrayList<>(project.getProjectList())
              : Collections.emptyList();
      case AddTotals addTotals ->
          addTotals.getFieldList().stream()
              .map(f -> (UnresolvedExpression) f)
              .collect(Collectors.toList());
      case MvCombine mvCombine -> List.of(mvCombine.getField());

      default -> Collections.emptyList();
    };
  }
}
