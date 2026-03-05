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
import java.util.function.Predicate;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.tree.AddTotals;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.MvCombine;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Replace;
import org.opensearch.sql.ast.tree.StreamWindow;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/**
 * Resolves dotted field paths (e.g. {@code doc.user.name}) referenced by a PPL command and projects
 * them as flat named columns. Each dotted path that passes the {@link #shouldMaterialize} predicate
 * is added to the current row type so downstream command logic can reference it by name.
 */
public class FieldPathPreMaterializer {

  private static final Logger log = LogManager.getLogger(FieldPathPreMaterializer.class);

  /** Visitor used to resolve field expressions to Calcite {@link RexNode}. */
  private final CalciteRexNodeVisitor rexVisitor;

  /** Predicate that determines whether a resolved field path should be materialized. */
  private final Predicate<RexNode> shouldMaterialize;

  public FieldPathPreMaterializer(CalciteRexNodeVisitor rexVisitor) {
    this(rexVisitor, rex -> rex.getKind() == SqlKind.ITEM);
  }

  public FieldPathPreMaterializer(
      CalciteRexNodeVisitor rexVisitor, Predicate<RexNode> shouldMaterialize) {
    this.rexVisitor = rexVisitor;
    this.shouldMaterialize = shouldMaterialize;
  }

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
      doMaterializeFieldPaths(fields, context);
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
      // The following commands use string-based field resolution internally (e.g.,
      // relBuilder.field(name)). Pre-materializing MAP paths here lets the existing
      // string-matching logic find them without modifying each command's visitor code.
      case StreamWindow streamWindow -> toFields(streamWindow.getGroupList());
      case Lookup lookup ->
          lookup.getMappingAliasMap().values().stream()
              .map(name -> new Field(QualifiedName.of(List.of(name.split("\\.")))))
              .toList();
      case Join join ->
          join.getJoinFields().isPresent() ? toFields(join.getJoinFields().get()) : List.of();
      default -> List.of();
    };
  }

  private void doMaterializeFieldPaths(List<Field> fields, CalcitePlanContext context) {
    Set<String> existingFields =
        new HashSet<>(context.relBuilder.peek().getRowType().getFieldNames());

    List<RexNode> newColumns = new ArrayList<>();
    for (Field field : fields) {
      try {
        RexNode resolved = rexVisitor.analyze(field, context);
        String name = field.getField().toString();

        if (shouldMaterialize.test(resolved) && !existingFields.contains(name)) {
          newColumns.add(context.relBuilder.alias(resolved, name));
          existingFields.add(name);
        }
      } catch (Throwable e) {
        // Skip unresolvable fields (e.g. wildcards, dotted paths on non-MAP types);
        // let the command itself handle them and throw its own error.
        log.debug("Skipping field resolution for '{}': {}", field.getField(), e.getMessage());
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
