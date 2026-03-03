/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.tree.AddTotals;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.MvCombine;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Replace;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/**
 * Pre-materializes MAP dotted paths for symbol-based commands (Category A) before the command's own
 * visitor logic runs. When a command like {@code rename} or {@code fillnull} references a dotted
 * path such as {@code doc.user.name}, this class resolves it to an {@code ITEM()} expression and
 * projects it as a flat named column so the command's string-matching logic can find it.
 *
 * <p>Called from {@link CalciteRelNodeVisitor#visitChildren} after children are visited. The
 * pipeline is: {@code extractFieldOperands → resolveMapPaths → projectMapPaths}.
 */
@RequiredArgsConstructor
public class MapPathPreMaterializer {

  /** Visitor used to resolve field expressions to Calcite {@link RexNode}. */
  private final CalciteRexNodeVisitor rexVisitor;

  /**
   * Materializes MAP dotted paths referenced by the given command. For each field operand that
   * resolves to an {@code ITEM()} access on a MAP column, projects it as a flat named column via
   * {@code relBuilder.projectPlus()}.
   *
   * @param plan the AST command being visited
   * @param context the current plan context with relBuilder state
   */
  public void materializePaths(UnresolvedPlan plan, CalcitePlanContext context) {
    if (context.relBuilder.size() == 0) {
      return;
    }

    List<Field> fields = extractFieldOperands(plan);
    if (!fields.isEmpty()) {
      projectMapPaths(resolveMapPaths(fields, context), context);
    }
  }

  private List<Field> extractFieldOperands(UnresolvedPlan node) {
    return switch (node) {
      case Rename rename -> toFields(rename.getRenameList(), m -> m.getOrigin());
      case FillNull fillNull -> toFields(fillNull.getReplacementPairs(), Pair::getLeft);
      case Replace replace -> toFields(replace.getFieldList());
      case Project project -> project.isExcluded() ? toFields(project.getProjectList()) : List.of();
      case AddTotals addTotals -> toFields(addTotals.getFieldList());
      case MvCombine mvCombine -> List.of(mvCombine.getField());
      default -> List.of();
    };
  }

  private Map<String, RexNode> resolveMapPaths(List<Field> fields, CalcitePlanContext context) {
    Map<String, RexNode> paths = new LinkedHashMap<>();
    for (Field f : fields) {
      try {
        RexNode resolved = rexVisitor.analyze(f, context);
        if (resolved.getKind() == SqlKind.ITEM) {
          paths.put(f.getField().toString(), resolved);
        }
      } catch (RuntimeException e) {
        // Field cannot be resolved (e.g., not in schema) — skip silently
      }
    }
    return paths;
  }

  private void projectMapPaths(Map<String, RexNode> mapPaths, CalcitePlanContext context) {
    if (mapPaths.isEmpty()) {
      return;
    }

    List<String> existingFields = context.relBuilder.peek().getRowType().getFieldNames();
    List<RexNode> newColumns = new ArrayList<>();
    mapPaths.forEach(
        (name, itemAccess) -> {
          if (!existingFields.contains(name)) {
            newColumns.add(context.relBuilder.alias(itemAccess, name));
          }
        });
    if (!newColumns.isEmpty()) {
      context.relBuilder.projectPlus(newColumns);
    }
  }

  private static <T> List<Field> toFields(Collection<T> items) {
    return toFields(items, Function.identity());
  }

  private static <T> List<Field> toFields(Collection<T> items, Function<T, ?> mapper) {
    return items.stream()
        .map(mapper)
        .filter(Field.class::isInstance)
        .map(Field.class::cast)
        .toList();
  }
}
