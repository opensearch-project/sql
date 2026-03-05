/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.tree.AddTotals;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.MvCombine;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Replace;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/**
 * Resolves MAP dotted paths (e.g. {@code doc.user.name}) referenced by a PPL command and projects
 * them as flat named columns. Each dotted path that resolves to an {@code ITEM()} expression is
 * added to the current row type so downstream command logic can reference it by name.
 */
@RequiredArgsConstructor
public class MapPathPreMaterializer {

  private static final Logger log = LogManager.getLogger(MapPathPreMaterializer.class);

  /** Visitor used to resolve field expressions to Calcite {@link RexNode}. */
  private final CalciteRexNodeVisitor rexVisitor;

  /**
   * Resolves and projects MAP dotted paths referenced by the given command as flat named columns.
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
      doMaterializeMapPaths(fields, context);
    }
  }

  private List<Field> extractFieldOperands(UnresolvedPlan node) {
    return switch (node) {
      case Rename rename -> toFields(rename.getRenameList(), m -> m.getOrigin());
      case FillNull fillNull -> toFields(fillNull.getReplacementPairs(), Pair::getLeft);
      case Replace replace -> toFields(replace.getFieldList());
      // Only fields-exclusion needs pre-materialization; inclusion resolves paths via visitProject
      case Project project -> project.isExcluded() ? toFields(project.getProjectList()) : List.of();
      case AddTotals addTotals -> toFields(addTotals.getFieldList());
      case MvCombine mvCombine -> List.of(mvCombine.getField());
      default -> List.of();
    };
  }

  private void doMaterializeMapPaths(List<Field> fields, CalcitePlanContext context) {
    Set<String> existingFields =
        new HashSet<>(context.relBuilder.peek().getRowType().getFieldNames());
    List<RexNode> newColumns = new ArrayList<>();
    for (Field field : fields) {
      try {
        RexNode resolved = rexVisitor.analyze(field, context);
        String name = field.getField().toString();
        if (resolved.getKind() == SqlKind.ITEM && !existingFields.contains(name)) {
          newColumns.add(context.relBuilder.alias(resolved, name));
          existingFields.add(name);
        }
      } catch (RuntimeException e) {
        // Skip unresolvable fields (e.g. wildcards); let the command itself handle them
        log.debug("Skipping field resolution for '{}': {}", field.getField(), e.getMessage(), e);
      }
    }

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
