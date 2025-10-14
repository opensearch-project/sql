/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.handlers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.AllFieldsExcludeMeta;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRexNodeVisitor;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
import org.opensearch.sql.calcite.utils.WildcardUtils;

/** Handler for project operations in CalciteRelNodeVisitor. */
@AllArgsConstructor
public class ProjectHandler {

  private final CalciteRexNodeVisitor rexVisitor;

  /**
   * Handles project operations including field expansion, exclusion, and metadata handling.
   *
   * @param node Project node to process
   * @param context CalcitePlanContext
   * @return true if project was handled successfully
   */
  public void handleProject(Project node, CalcitePlanContext context) {
    if (isSingleAllFieldsProject(node)) {
      handleAllFieldsProject(node, context);
    } else {
      List<String> currentFields = context.relBuilder.peek().getRowType().getFieldNames();
      List<RexNode> expandedFields =
          expandProjectFields(node.getProjectList(), currentFields, context);

      if (node.isExcluded()) {
        validateExclusion(expandedFields, currentFields);
        context.relBuilder.projectExcept(expandedFields);
      } else {
        if (!context.isResolvingSubquery()) {
          context.setProjectVisited(true);
        }
        context.relBuilder.project(expandedFields);
      }
    }
  }

  private boolean isSingleAllFieldsProject(Project node) {
    return node.getProjectList().size() == 1
        && node.getProjectList().getFirst() instanceof AllFields;
  }

  private void handleAllFieldsProject(Project node, CalcitePlanContext context) {
    if (node.isExcluded()) {
      throw new IllegalArgumentException(
          "Invalid field exclusion: operation would exclude all fields from the result set");
    }
    AllFields allFields = (AllFields) node.getProjectList().getFirst();
    ProjectionUtils.tryToRemoveNestedFields(context);
    ProjectionUtils.tryToRemoveMetaFields(context, allFields instanceof AllFieldsExcludeMeta);
  }

  private List<RexNode> expandProjectFields(
      List<UnresolvedExpression> projectList,
      List<String> currentFields,
      CalcitePlanContext context) {
    List<RexNode> expandedFields = new ArrayList<>();
    Set<String> addedFields = new HashSet<>();

    for (UnresolvedExpression expr : projectList) {
      switch (expr) {
        case Field field -> {
          String fieldName = field.getField().toString();
          if (WildcardUtils.containsWildcard(fieldName)) {
            List<String> matchingFields =
                WildcardUtils.expandWildcardPattern(fieldName, currentFields).stream()
                    .filter(f -> !isMetadataField(f))
                    .filter(addedFields::add)
                    .toList();
            if (matchingFields.isEmpty()) {
              continue;
            }
            matchingFields.forEach(f -> expandedFields.add(context.relBuilder.field(f)));
          } else if (addedFields.add(fieldName)) {
            expandedFields.add(rexVisitor.analyze(field, context));
          }
        }
        case AllFields ignored -> {
          currentFields.stream()
              .filter(field -> !isMetadataField(field))
              .filter(addedFields::add)
              .forEach(field -> expandedFields.add(context.relBuilder.field(field)));
        }
        default -> throw new IllegalStateException(
            "Unexpected expression type in project list: " + expr.getClass().getSimpleName());
      }
    }

    if (expandedFields.isEmpty()) {
      validateWildcardPatterns(projectList, currentFields);
    }

    return expandedFields;
  }

  private void validateExclusion(List<RexNode> fieldsToExclude, List<String> currentFields) {
    Set<String> nonMetaFields =
        currentFields.stream().filter(field -> !isMetadataField(field)).collect(Collectors.toSet());

    if (fieldsToExclude.size() >= nonMetaFields.size()) {
      throw new IllegalArgumentException(
          "Invalid field exclusion: operation would exclude all fields from the result set");
    }
  }

  private void validateWildcardPatterns(
      List<UnresolvedExpression> projectList, List<String> currentFields) {
    String firstWildcardPattern =
        projectList.stream()
            .filter(
                expr ->
                    expr instanceof Field field
                        && WildcardUtils.containsWildcard(field.getField().toString()))
            .map(expr -> ((Field) expr).getField().toString())
            .findFirst()
            .orElse(null);

    if (firstWildcardPattern != null) {
      throw new IllegalArgumentException(
          String.format("wildcard pattern [%s] matches no fields", firstWildcardPattern));
    }
  }

  private boolean isMetadataField(String fieldName) {
    return OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(fieldName);
  }
}
