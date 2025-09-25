/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRexNodeVisitor;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/**
 * Utility class for processing wildcard patterns that may match dynamic columns. Handles both
 * static field wildcards (resolved at planning time) and dynamic field wildcards (resolved at
 * execution time). Supports both positive (inclusion) and negative (exclusion) patterns, with
 * efficient processing of multiple patterns.
 */
@UtilityClass
public class DynamicWildcardProcessor {

  /**
   * Expands wildcard patterns in project fields, handling both static and dynamic columns. This
   * method processes positive wildcard patterns (inclusion).
   *
   * @param projectList List of expressions to expand
   * @param currentFields Static fields available in the current schema
   * @param context CalcitePlanContext for building expressions
   * @return List of RexNode expressions with wildcards expanded
   */
  public static List<RexNode> expandWildcardFields(
      CalciteRexNodeVisitor rexVisitor,
      List<UnresolvedExpression> projectList,
      List<String> currentFields,
      CalcitePlanContext context) {

    List<RexNode> expandedFields = new ArrayList<>();
    Set<String> addedFields = new HashSet<>();
    boolean hasDynamicColumns =
        currentFields.contains(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD);

    // Collect dynamic wildcard patterns for batch processing
    List<String> dynamicWildcardPatterns = new ArrayList<>();

    for (UnresolvedExpression expr : projectList) {
      switch (expr) {
        case Field field -> {
          String fieldName = field.getField().toString();

          if (WildcardUtils.containsWildcard(fieldName)) {
            // Handle wildcard patterns
            List<String> staticMatches =
                WildcardUtils.expandWildcardPattern(fieldName, currentFields).stream()
                    .filter(f -> !isMetadataField(f))
                    .filter(addedFields::add)
                    .toList();

            // Add static matches
            staticMatches.forEach(f -> expandedFields.add(context.relBuilder.field(f)));

            if (hasDynamicColumns) {
              dynamicWildcardPatterns.add(fieldName);
            }
          } else {
            // Handle non-wildcard fields
            if (addedFields.add(fieldName)) {
              RexNode node = rexVisitor.analyze(field, context);
              System.out.println("Resolved RexNode: " + node);
              if (node != null) {
                // Static field
                expandedFields.add(node);
              } else if (hasDynamicColumns) {
                // Potential dynamic field
                RexNode dynamicField =
                    DynamicColumnProcessor.resolveDynamicField(fieldName, context);
                expandedFields.add(dynamicField);
              } else {
                // Field not found - let normal error handling take care of this
                expandedFields.add(context.relBuilder.field(fieldName));
              }
            }
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

    // Process dynamic wildcard patterns in batch if any exist
    if (!dynamicWildcardPatterns.isEmpty()) {
      RexNode dynamicWildcardExpansion =
          createDynamicWildcardExpansion(dynamicWildcardPatterns, context);
      // Project the expanded results back into _dynamic_columns field so DynamicColumnProcessor can
      // expand it
      RexNode aliasedExpansion =
          context.relBuilder.alias(
              dynamicWildcardExpansion, DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD);
      expandedFields.add(aliasedExpansion);
    }

    return expandedFields;
  }

  /**
   * Handles exclusion wildcard patterns for the fields command. This method processes negative
   * wildcard patterns (exclusion).
   *
   * @param exclusionList List of expressions to exclude
   * @param currentFields Static fields available in the current schema
   * @param context CalcitePlanContext for building expressions
   * @return List of RexNode expressions representing fields to exclude
   */
  public static List<RexNode> handleExclusionWildcards(
      List<UnresolvedExpression> exclusionList,
      List<String> currentFields,
      CalcitePlanContext context) {

    List<RexNode> fieldsToExclude = new ArrayList<>();
    boolean hasDynamicColumns =
        currentFields.contains(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD);

    // Collect dynamic wildcard patterns for batch processing
    List<String> dynamicExclusionPatterns = new ArrayList<>();

    for (UnresolvedExpression expr : exclusionList) {
      if (expr instanceof Field field) {
        String fieldName = field.getField().toString();

        if (WildcardUtils.containsWildcard(fieldName)) {
          // Handle wildcard exclusion patterns
          List<String> staticMatches =
              WildcardUtils.expandWildcardPattern(fieldName, currentFields).stream()
                  .filter(f -> !isMetadataField(f))
                  .toList();

          // Add static matches to exclusion list
          staticMatches.forEach(f -> fieldsToExclude.add(context.relBuilder.field(f)));
        } else {
          // Handle non-wildcard exclusion fields
          if (currentFields.contains(fieldName)) {
            // Static field
            fieldsToExclude.add(context.relBuilder.field(fieldName));
          }
        }
        dynamicExclusionPatterns.add(fieldName);
      }
    }

    // Process dynamic exclusion patterns in batch if any exist
    if (hasDynamicColumns && !dynamicExclusionPatterns.isEmpty()) {
      // For exclusion, we need to modify the _dynamic_columns field itself
      // This will be handled by applying the exclusion function to the dynamic columns
      RexNode dynamicColumnsField =
          context.relBuilder.field(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD);
      RexNode filteredDynamicColumns =
          createDynamicWildcardExclusion(dynamicExclusionPatterns, dynamicColumnsField, context);

      // Use projectPlusOverriding to properly replace the _dynamic_columns field
      // This ensures we get _dynamic_columns instead of _dynamic_columns0
      projectPlusOverriding(
          context,
          List.of(filteredDynamicColumns),
          List.of(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD));
    }

    return fieldsToExclude;
  }

  /**
   * Creates a dynamic wildcard expansion expression that will be resolved at execution time. This
   * creates a function call that expands the wildcard patterns against the keys in the
   * _dynamic_columns MAP field. Always passes patterns as an array for consistent function
   * signature.
   */
  private static RexNode createDynamicWildcardExpansion(
      List<String> wildcardPatterns, CalcitePlanContext context) {
    RexNode dynamicColumnsField =
        context.relBuilder.field(DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD);

    // Always pass patterns as array for consistent function signature
    List<RexNode> patternLiterals =
        wildcardPatterns.stream()
            .map(pattern -> (RexNode) context.rexBuilder.makeLiteral(pattern))
            .toList();
    RexNode patternsArray =
        context.rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
            patternLiterals);

    return PPLFuncImpTable.INSTANCE.resolve(
        context.rexBuilder,
        BuiltinFunctionName.DYNAMIC_WILDCARD_EXPAND,
        dynamicColumnsField,
        patternsArray);
  }

  /**
   * Creates a dynamic wildcard exclusion expression that will be resolved at execution time. This
   * creates a function call that excludes keys matching the wildcard patterns from the
   * _dynamic_columns MAP field. Always passes patterns as an array for consistent function
   * signature.
   */
  private static RexNode createDynamicWildcardExclusion(
      List<String> exclusionPatterns, RexNode dynamicColumnsField, CalcitePlanContext context) {

    List<RexNode> patternLiterals =
        exclusionPatterns.stream()
            .map(pattern -> (RexNode) context.rexBuilder.makeLiteral(pattern))
            .toList();
    RexNode patternsArray =
        context.rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
            patternLiterals);

    return PPLFuncImpTable.INSTANCE.resolve(
        context.rexBuilder,
        BuiltinFunctionName.DYNAMIC_WILDCARD_EXCLUDE,
        dynamicColumnsField,
        patternsArray);
  }

  /**
   * Validates that at least one wildcard pattern has matches (either static or potentially
   * dynamic). This prevents the "no fields match" error when dynamic wildcards might have matches
   * at runtime.
   */
  public static void validateWildcardPatterns(
      List<UnresolvedExpression> projectList,
      List<String> currentFields,
      boolean hasDynamicColumns) {

    String firstWildcardPattern = null;

    for (UnresolvedExpression expr : projectList) {
      if (expr instanceof Field field) {
        String fieldName = field.getField().toString();

        if (WildcardUtils.containsWildcard(fieldName)) {
          if (firstWildcardPattern == null) {
            firstWildcardPattern = fieldName;
          }

          List<String> staticMatches =
              WildcardUtils.expandWildcardPattern(fieldName, currentFields);

          if (!staticMatches.isEmpty() || hasDynamicColumns) {
            // just ensure at lease one valid pattern found
            return;
          }
        }
      }
    }

    // If no valid patterns found and we have wildcard patterns, throw error
    if (firstWildcardPattern != null) {
      throw new IllegalArgumentException(
          String.format("wildcard pattern [%s] matches no fields", firstWildcardPattern));
    }
  }

  /**
   * Helper method to properly replace fields without creating duplicates like _dynamic_columns0.
   * This is copied from CalciteRelNodeVisitor.projectPlusOverriding() method.
   */
  private static void projectPlusOverriding(
      CalcitePlanContext context, List<RexNode> newFields, List<String> newNames) {
    List<String> originalFieldNames = context.relBuilder.peek().getRowType().getFieldNames();
    List<RexNode> toOverrideList =
        originalFieldNames.stream()
            .filter(newNames::contains)
            .map(a -> (RexNode) context.relBuilder.field(a))
            .toList();
    // 1. add the new fields, For example "age0, country0"
    context.relBuilder.projectPlus(newFields);
    // 2. drop the overriding field list, it's duplicated now. For example "age, country"
    if (!toOverrideList.isEmpty()) {
      context.relBuilder.projectExcept(toOverrideList);
    }
    // 3. get current fields list, the "age0, country0" should include in it.
    List<String> currentFields = context.relBuilder.peek().getRowType().getFieldNames();
    int length = currentFields.size();
    // 4. add new names "age, country" to the end of rename list.
    List<String> expectedRenameFields =
        new ArrayList<>(currentFields.subList(0, length - newNames.size()));
    expectedRenameFields.addAll(newNames);
    // 5. rename
    context.relBuilder.rename(expectedRenameFields);
  }

  private static boolean isMetadataField(String fieldName) {
    return org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(
        fieldName);
  }
}
