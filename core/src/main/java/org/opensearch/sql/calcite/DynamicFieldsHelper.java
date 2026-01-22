/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.calcite.plan.DynamicFieldsConstants.DYNAMIC_FIELDS_MAP;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;

/** Helper class for dynamic fields operations in Calcite plan building. */
class DynamicFieldsHelper {

  /** Check if a field name is the dynamic fields map constant. */
  static boolean isDynamicFieldMap(String field) {
    return DYNAMIC_FIELDS_MAP.equals(field);
  }

  /** Get static fields from the left side of a join (excludes dynamic fields map). */
  static List<String> getLeftStaticFields(CalcitePlanContext context) {
    return excludeDynamicFields(context.relBuilder.peek(1).getRowType().getFieldNames());
  }

  /** Get static fields from current relation (excludes dynamic fields map). */
  static List<String> getStaticFields(CalcitePlanContext context) {
    return excludeDynamicFields(context.relBuilder.peek().getRowType().getFieldNames());
  }

  /** Get static fields from the right side of a join (excludes dynamic fields map). */
  static List<String> getRightStaticFields(CalcitePlanContext context) {
    return getStaticFields(context);
  }

  /** Filter out dynamic fields map from field names list. */
  static List<String> excludeDynamicFields(List<String> fieldNames) {
    return fieldNames.stream()
        .filter(fieldName -> !isDynamicFieldMap(fieldName))
        .collect(Collectors.toList());
  }

  /** Filter out metadata fields from field names collection. */
  static List<String> excludeMetaFields(Collection<String> fields) {
    return fields.stream().filter(field -> !isMetadataField(field)).collect(Collectors.toList());
  }

  /** Get remaining fields after excluding specified fields. */
  static List<String> getRemainingFields(
      Collection<String> existingFields, Collection<String> excluded) {
    List<String> keys = excludeMetaFields(existingFields);
    keys.removeAll(excluded);
    Collections.sort(keys);
    return keys;
  }

  /** Check if dynamic fields map exists in current relation. */
  static boolean isDynamicFieldsExists(CalcitePlanContext context) {
    return context.relBuilder.peek().getRowType().getFieldNames().contains(DYNAMIC_FIELDS_MAP);
  }

  /** Check if a RelNode has dynamic fields. */
  static boolean hasDynamicFields(RelNode node) {
    return node.getRowType().getFieldNames().contains(DYNAMIC_FIELDS_MAP);
  }

  /** Adjust fields to align the static/dynamic fields for join. */
  static void adjustJoinInputsForDynamicFields(
      Optional<String> leftAlias, Optional<String> rightAlias, CalcitePlanContext context) {
    if (hasDynamicFields(context.relBuilder.peek())
        || hasDynamicFields(context.relBuilder.peek(1))) {
      // build once to modify the inputs already in the stack.
      RelNode right = context.relBuilder.build();
      RelNode left = context.relBuilder.build();
      left = adjustFieldsForDynamicFields(left, right, context);
      right = adjustFieldsForDynamicFields(right, left, context);
      context.relBuilder.push(left);
      // `as(alias)` is needed since `build()` won't preserve alias
      leftAlias.map(alias -> context.relBuilder.as(alias));
      context.relBuilder.push(right);
      rightAlias.map(alias -> context.relBuilder.as(alias));
    }
  }

  /** Adjust fields to align the static/dynamic fields in `target` to `theOtherInput` */
  static RelNode adjustFieldsForDynamicFields(
      RelNode target, RelNode theOtherInput, CalcitePlanContext context) {
    if (hasDynamicFields(theOtherInput) && !hasDynamicFields(target)) {
      return adjustFieldsForDynamicFields(
          target, theOtherInput.getRowType().getFieldNames(), context);
    }
    return target;
  }

  /**
   * Project node's fields in `requiredFieldNames` as static field, and put other fields into `_MAP`
   * (dynamic fields) This projection is needed when merging an input with dynamic fields and an
   * input without dynamic fields. This process can be eliminated once we fully integrate
   * schema-on-read (https://github.com/opensearch-project/sql/issues/4984)
   */
  static RelNode adjustFieldsForDynamicFields(
      RelNode node, List<String> requiredFieldNames, CalcitePlanContext context) {
    context.relBuilder.push(node);
    List<String> existingFields = node.getRowType().getFieldNames();
    List<RexNode> project = new ArrayList<>();
    for (String existingField : existingFields) {
      if (requiredFieldNames.contains(existingField)) {
        project.add(context.rexBuilder.makeInputRef(node, existingFields.indexOf(existingField)));
      }
    }
    project.add(
        context.relBuilder.alias(
            CalciteRelNodeVisitor.getFieldsAsMap(existingFields, requiredFieldNames, context),
            DYNAMIC_FIELDS_MAP));
    return context.relBuilder.project(project).build();
  }

  /** Get map item as string with type casting. */
  static RexNode getItemAsString(RexNode map, String fieldName, CalcitePlanContext context) {
    RexNode item = context.rexBuilder.itemCall(map, fieldName);
    // Cast to string for type consistency. (This cast will be removed once functions are adopted
    // to ANY type)
    return context.relBuilder.cast(item, SqlTypeName.VARCHAR);
  }

  /** Cast a RexNode to string type. */
  static RexNode castToString(RexNode node, CalcitePlanContext context) {
    return context.relBuilder.cast(node, SqlTypeName.VARCHAR);
  }

  /** Check if a field name is a metadata field. */
  private static boolean isMetadataField(String fieldName) {
    return OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(fieldName);
  }
}
