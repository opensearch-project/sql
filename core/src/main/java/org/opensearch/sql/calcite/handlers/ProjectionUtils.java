/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.handlers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;

/** Utility class for common projection operations used across multiple handlers and visitors. */
public class ProjectionUtils {

  /**
   * Try to remove nested fields to avoid duplication issues. See logic in {@link
   * org.opensearch.sql.analysis.symbol.SymbolTable#lookupAllFields}
   */
  public static void tryToRemoveNestedFields(CalcitePlanContext context) {
    Set<String> allFields = new HashSet<>(context.relBuilder.peek().getRowType().getFieldNames());
    List<RexNode> duplicatedNestedFields =
        allFields.stream()
            .filter(
                field -> {
                  int lastDot = field.lastIndexOf(".");
                  return -1 != lastDot && allFields.contains(field.substring(0, lastDot));
                })
            .map(field -> (RexNode) context.relBuilder.field(field))
            .toList();
    if (!duplicatedNestedFields.isEmpty()) {
      forceProjectExcept(context.relBuilder, duplicatedNestedFields);
    }
  }

  /**
   * Project except with force. This method is copied from {@link
   * org.apache.calcite.tools.RelBuilder#projectExcept(Iterable)} and modified with the force flag
   * in project set to true.
   */
  public static void forceProjectExcept(RelBuilder relBuilder, Iterable<RexNode> expressions) {
    List<RexNode> allExpressions = new ArrayList<>(relBuilder.fields());
    Set<RexNode> excludeExpressions = new HashSet<>();
    for (RexNode excludeExp : expressions) {
      if (!excludeExpressions.add(excludeExp)) {
        throw new IllegalArgumentException(
            "Input list contains duplicates. Expression " + excludeExp + " exists multiple times.");
      }
      if (!allExpressions.remove(excludeExp)) {
        throw new IllegalArgumentException("Expression " + excludeExp.toString() + " not found.");
      }
    }
    relBuilder.project(allExpressions, com.google.common.collect.ImmutableList.of(), true);
  }

  /**
   * Try to remove metadata fields in two cases: 1. It's explicitly specified excluding by force,
   * usually for join or subquery. 2. There is no other project ever visited in the main query
   *
   * @param context CalcitePlanContext
   * @param excludeByForce whether exclude metadata fields by force
   */
  public static void tryToRemoveMetaFields(CalcitePlanContext context, boolean excludeByForce) {
    if (excludeByForce || !context.isProjectVisited()) {
      List<String> originalFields = context.relBuilder.peek().getRowType().getFieldNames();
      List<RexNode> metaFieldsRef =
          originalFields.stream()
              .filter(OpenSearchConstants.METADATAFIELD_TYPE_MAP::containsKey)
              .map(metaField -> (RexNode) context.relBuilder.field(metaField))
              .toList();
      // Remove metadata fields if there is and ensure there are other fields.
      if (!metaFieldsRef.isEmpty() && metaFieldsRef.size() != originalFields.size()) {
        context.relBuilder.projectExcept(metaFieldsRef);
      }
    }
  }
}
