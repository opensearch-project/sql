/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.sql.ast.Node;
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
 * Pre-materializes MAP dotted paths for symbol-based commands (Category A). When a command like
 * {@code rename}, {@code fillnull}, or {@code replace} references a dotted path such as {@code
 * doc.user.name}, this class resolves it to an {@code ITEM()} expression and projects it as a flat
 * named column so the command's string-matching logic can find it in the schema.
 *
 * <p>The materialization pipeline has three steps:
 *
 * <ol>
 *   <li>{@link #extractOperands} — extract field operands from the AST node
 *   <li>{@link #collectMapPaths} — resolve each operand, keep only MAP paths (ITEM access)
 *   <li>{@link #materialize} — project collected paths as flat named columns
 * </ol>
 */
public class MapPathPreMaterializer {

  private final CalciteRexNodeVisitor rexVisitor;

  public MapPathPreMaterializer(CalciteRexNodeVisitor rexVisitor) {
    this.rexVisitor = rexVisitor;
  }

  /**
   * Entry point: materializes MAP dotted paths referenced by the given AST node. Called from {@link
   * CalciteRelNodeVisitor#visitChildren} after children are visited.
   */
  public void materializePaths(Node node, CalcitePlanContext context) {
    if (!(node instanceof UnresolvedPlan plan) || context.relBuilder.size() == 0) return;
    projectMapPaths(resolveMapPaths(extractFieldOperands(plan), context), context);
  }

  /**
   * Step 1: Extract field operands from known AST node types that do symbol-based field matching.
   *
   * <p>Only covers Category A commands. Category B (missing alias wrapping) and Category C
   * (fragmented resolve API) will be addressed in follow-up PRs.
   */
  private List<Field> extractFieldOperands(UnresolvedPlan node) {
    List<UnresolvedExpression> raw =
        switch (node) {
          case Rename rename ->
              rename.getRenameList().stream()
                  .map(m -> (UnresolvedExpression) m.getOrigin())
                  .toList();
          case FillNull fillNull ->
              fillNull.getReplacementPairs().stream()
                  .map(p -> (UnresolvedExpression) p.getLeft())
                  .toList();
          case Replace replace -> new ArrayList<>(replace.getFieldList());
          case Project project ->
              project.isExcluded() ? new ArrayList<>(project.getProjectList()) : List.of();
          case AddTotals addTotals -> new ArrayList<>(addTotals.getFieldList());
          case MvCombine mvCombine -> List.of(mvCombine.getField());
          default -> Collections.emptyList();
        };
    return raw.stream().filter(Field.class::isInstance).map(Field.class::cast).toList();
  }

  /**
   * Step 2: Resolve each field via rexVisitor and collect those that resolve to MAP path access
   * (ITEM). Fields that resolve to regular schema columns are filtered out.
   */
  private Map<String, RexNode> resolveMapPaths(List<Field> fields, CalcitePlanContext context) {
    Map<String, RexNode> paths = new LinkedHashMap<>();
    for (Field f : fields) {
      RexNode resolved = rexVisitor.analyze(f, context);
      if (resolved.getKind() == SqlKind.ITEM) {
        paths.put(f.getField().toString(), resolved);
      }
    }
    return paths;
  }

  /**
   * Step 3: Project each MAP path as a flat named column via {@code projectPlus} + alias. Uses
   * {@code projectPlus} (not {@code projectPlusOverriding}) to preserve the original MAP column.
   */
  private void projectMapPaths(Map<String, RexNode> mapPaths, CalcitePlanContext context) {
    if (mapPaths.isEmpty()) return;
    List<RexNode> aliased = new ArrayList<>();
    for (var entry : mapPaths.entrySet()) {
      aliased.add(context.relBuilder.alias(entry.getValue(), entry.getKey()));
    }
    context.relBuilder.projectPlus(aliased);
  }
}
