/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.calcite.plan.DynamicFieldsConstants.DYNAMIC_FIELDS_MAP;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/** Helper class for dynamic fields operations in Calcite plan building. */
class DynamicFieldsHelper {

  /** Check if a field name is the dynamic fields map constant. */
  static boolean isDynamicFieldMap(String field) {
    return DYNAMIC_FIELDS_MAP.equals(field);
  }

  /** Check if dynamic fields map exists in current relation. */
  static boolean isDynamicFieldsExists(CalcitePlanContext context) {
    return context.relBuilder.peek().getRowType().getFieldNames().contains(DYNAMIC_FIELDS_MAP);
  }

  /** Check if a RelNode has dynamic fields. */
  private static boolean hasDynamicFields(RelNode node) {
    return node.getRowType().getFieldNames().contains(DYNAMIC_FIELDS_MAP);
  }

  /** Check if a field name is a metadata field. */
  private static boolean isMetadataField(String fieldName) {
    return OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(fieldName);
  }

  /** Get static fields from the left side of a join (excludes dynamic fields map). */
  static List<String> getLeftStaticFields(CalcitePlanContext context) {
    return excludeDynamicFields(context.relBuilder.peek(1).getRowType().getFieldNames());
  }

  /** Get static fields from current relation (excludes dynamic fields map). */
  static List<String> getStaticFields(CalcitePlanContext context) {
    return excludeDynamicFields(context.relBuilder.peek().getRowType().getFieldNames());
  }

  /** Get static fields from node */
  static List<String> getStaticFields(RelNode node) {
    return excludeDynamicFields(node.getRowType().getFieldNames());
  }

  /** Get static fields from the right side of a join (excludes dynamic fields map). */
  static List<String> getRightStaticFields(CalcitePlanContext context) {
    return getStaticFields(context);
  }

  /** Filter out dynamic fields map from field names collection. */
  private static List<String> excludeDynamicFields(Collection<String> fields) {
    return fields.stream().filter(field -> !isDynamicFieldMap(field)).collect(Collectors.toList());
  }

  /** Filter out metadata fields from field names collection. */
  private static List<String> excludeMetaFields(Collection<String> fields) {
    return fields.stream().filter(field -> !isMetadataField(field)).collect(Collectors.toList());
  }

  /** Filter out dynamic fields and metadata fields from field names collection */
  private static List<String> excludeSpecialFields(Collection<String> fields) {
    return fields.stream()
        .filter(field -> !isMetadataField(field) && !isDynamicFieldMap(field))
        .collect(Collectors.toList());
  }

  /** Get remaining fields after excluding specified fields. */
  private static List<String> getRemainingFields(
      Collection<String> existingFields, Collection<String> excluded) {
    List<String> keys = excludeSpecialFields(existingFields);
    keys.removeAll(excluded);
    Collections.sort(keys);
    return keys;
  }

  /** Adjust fields to align the static/dynamic fields for join. */
  static void adjustJoinInputsForDynamicFields(
      Optional<String> leftAlias, Optional<String> rightAlias, CalcitePlanContext context) {
    if (hasDynamicFields(context.relBuilder.peek())
        || hasDynamicFields(context.relBuilder.peek(1))) {
      // build once to modify the inputs already in the stack.
      RelNode right = context.relBuilder.build();
      RelNode left = context.relBuilder.build();
      List<RelNode> inputs = adjustInputsForDynamicFields(List.of(right, left), context);
      right = inputs.get(0);
      left = inputs.get(1);
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
      List<String> requiredStaticFields = getStaticFields(theOtherInput);
      return adjustFieldsForDynamicFields(target, requiredStaticFields, context);
    }
    return target;
  }

  /** Adjust inputs to align the static/dynamic fields each other */
  static List<RelNode> adjustInputsForDynamicFields(
      List<RelNode> inputs, CalcitePlanContext context) {
    boolean requireAdjustment = inputs.stream().anyMatch(input -> hasDynamicFields(input));
    if (requireAdjustment) {
      List<String> requiredStaticFields = getRequiredStaticFields(inputs);
      return inputs.stream()
          .map(input -> adjustFieldsForDynamicFields(input, requiredStaticFields, context))
          .collect(Collectors.toList());
    } else {
      return inputs;
    }
  }

  static List<String> getRequiredStaticFields(List<RelNode> inputs) {
    Set<String> requiredStaticFields = new HashSet<String>();
    for (RelNode input : inputs) {
      if (hasDynamicFields(input)) {
        requiredStaticFields.addAll(getStaticFields(input));
      }
    }
    return toSortedList(requiredStaticFields);
  }

  private static List<String> toSortedList(Collection<String> collection) {
    ArrayList<String> result = new ArrayList<>(collection);
    Collections.sort(result);
    return result;
  }

  /**
   * Project node's fields in `requiredFieldNames` as static field, and put other fields into `_MAP`
   * (dynamic fields) This projection is needed when merging an input with dynamic fields and an
   * input without dynamic fields. This process can be eliminated once we fully integrate
   * schema-on-read (https://github.com/opensearch-project/sql/issues/4984)
   */
  static RelNode adjustFieldsForDynamicFields(
      RelNode node, List<String> staticFieldNames, CalcitePlanContext context) {
    context.relBuilder.push(node);
    List<String> existingFields = getStaticFields(node);
    List<RexNode> project = new ArrayList<>();
    for (String existingField : existingFields) {
      if (staticFieldNames.contains(existingField)) {
        project.add(context.rexBuilder.makeInputRef(node, existingFields.indexOf(existingField)));
      }
    }
    if (hasDynamicFields(node)) {
      // _MAP = MAP_APPEND(_MAP, MAP(existingFields - staticFields))
      RexNode existingDynamicFieldsMap = context.relBuilder.field(DYNAMIC_FIELDS_MAP);
      RexNode additionalFieldsMap = getFieldsAsMap(existingFields, staticFieldNames, context);
      RexNode mapAppend =
          context.rexBuilder.makeCall(
              BuiltinFunctionName.MAP_APPEND, existingDynamicFieldsMap, additionalFieldsMap);
      project.add(context.relBuilder.alias(mapAppend, DYNAMIC_FIELDS_MAP));
    } else {
      // _MAP = MAP(existingFields - staticFields)
      project.add(
          context.relBuilder.alias(
              getFieldsAsMap(existingFields, staticFieldNames, context), DYNAMIC_FIELDS_MAP));
    }
    return context.relBuilder.project(project).build();
  }

  /**
   * Build regular field projections from a map, with optional append logic for existing fields.
   *
   * @param map The source map made by JSON_EXTRACT_ALL
   * @param regularFieldNames List of field names to extract
   * @param existingFields Set of fields that already exist in the current relation
   * @param context CalcitePlanContext
   * @return List of RexNode projections with aliases
   */
  static List<RexNode> buildRegularFieldProjections(
      RexNode map,
      List<String> regularFieldNames,
      Set<String> existingFields,
      CalcitePlanContext context) {
    List<RexNode> fields = new ArrayList<>();
    for (String fieldName : regularFieldNames) {
      RexNode item = getItemAsString(map, fieldName, context);
      // Append if field already exists
      if (existingFields.contains(fieldName)) {
        item =
            context.rexBuilder.makeCall(
                BuiltinFunctionName.INTERNAL_APPEND, context.relBuilder.field(fieldName), item);
        item = castToString(item, context);
      }
      fields.add(context.relBuilder.alias(item, fieldName));
    }
    return fields;
  }

  /**
   * Build dynamic map field projection when wildcards are present.
   *
   * @param map The source map RexNode
   * @param sortedRegularFields Sorted list of regular fields to exclude from dynamic map
   * @param existingFields Set of existing fields in the current relation
   * @param context CalcitePlanContext
   * @return RexNode for the dynamic map field
   */
  static RexNode buildDynamicMapFieldProjection(
      RexNode map,
      List<String> sortedRegularFields,
      Set<String> existingFields,
      CalcitePlanContext context) {
    RexNode dynamicMapField = createDynamicMapField(map, sortedRegularFields, context);
    List<String> remainingFields = getRemainingFields(existingFields, sortedRegularFields);

    if (!remainingFields.isEmpty()) {
      // Add existing fields to map
      RexNode existingFieldsMap = getFieldsAsMap(existingFields, sortedRegularFields, context);
      dynamicMapField =
          context.rexBuilder.makeCall(
              BuiltinFunctionName.MAP_APPEND, existingFieldsMap, dynamicMapField);
    }

    if (isDynamicFieldsExists(context)) {
      RexNode existingMap = context.relBuilder.field(DYNAMIC_FIELDS_MAP);
      dynamicMapField =
          context.rexBuilder.makeCall(BuiltinFunctionName.MAP_APPEND, existingMap, dynamicMapField);
    }

    return dynamicMapField;
  }

  /** Get map item as string with type casting. */
  private static RexNode getItemAsString(
      RexNode map, String fieldName, CalcitePlanContext context) {
    RexNode item = context.rexBuilder.itemCall(map, fieldName);
    // Cast to string for type consistency. (This cast will be removed once functions are adopted
    // to ANY type)
    return context.relBuilder.cast(item, SqlTypeName.VARCHAR);
  }

  /** Cast a RexNode to string type. */
  private static RexNode castToString(RexNode node, CalcitePlanContext context) {
    return context.relBuilder.cast(node, SqlTypeName.VARCHAR);
  }

  /** Create dynamic map field by removing regular fields from full map */
  private static RexNode createDynamicMapField(
      RexNode fullMap, List<String> sortedRegularFields, CalcitePlanContext context) {
    if (sortedRegularFields.isEmpty()) {
      return fullMap;
    }

    List<RexNode> stringLiteralList = getStringLiteralList(sortedRegularFields, context);

    RelDataType stringArrayType =
        context
            .rexBuilder
            .getTypeFactory()
            .createArrayType(
                context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), -1);
    RexNode keyArray =
        context.rexBuilder.makeCall(
            stringArrayType, SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, stringLiteralList);

    // MAP_REMOVE(fullMap, keyArray) → filtered map with only unmapped fields
    return context.rexBuilder.makeCall(BuiltinFunctionName.MAP_REMOVE, fullMap, keyArray);
  }

  /** Convert fields to map representation */
  private static RexNode getFieldsAsMap(
      Collection<String> existingFields, Collection<String> excluded, CalcitePlanContext context) {
    List<String> keys = excludeMetaFields(existingFields);
    keys.removeAll(excluded);
    Collections.sort(keys);
    // Handle empty keys case - Calcite ARRAY[] requires at least 1 element
    if (keys.isEmpty()) {
      return createEmptyMap(context);
    }
    RexNode keysArray = getStringLiteralArray(keys, context);
    List<RexNode> values =
        keys.stream()
            .map(key -> context.relBuilder.cast(context.relBuilder.field(key), SqlTypeName.VARCHAR))
            .collect(Collectors.toList());
    RexNode valuesArray = makeArray(values, context);
    return context.rexBuilder.makeCall(BuiltinFunctionName.MAP_FROM_ARRAYS, keysArray, valuesArray);
  }

  /** Create an empty map by casting NULL to MAP type */
  private static RexNode createEmptyMap(CalcitePlanContext context) {
    RelDataType varcharType =
        context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    RelDataType mapType =
        context.rexBuilder.getTypeFactory().createMapType(varcharType, varcharType);
    return context.rexBuilder.makeNullLiteral(mapType);
  }

  /** Create string literal array from collection of strings */
  private static RexNode getStringLiteralArray(
      Collection<String> keys, CalcitePlanContext context) {
    List<RexNode> stringLiteralList = getStringLiteralList(keys, context);

    return context.rexBuilder.makeCall(
        getStringArrayType(context),
        SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
        stringLiteralList);
  }

  /** Create list of string literals from list of strings */
  private static List<RexNode> getStringLiteralList(
      Collection<String> strings, CalcitePlanContext context) {
    return strings.stream()
        .sorted()
        .map(name -> context.rexBuilder.makeLiteral(name))
        .collect(Collectors.toList());
  }

  /** Create an array from list of RexNodes */
  private static RexNode makeArray(List<RexNode> items, CalcitePlanContext context) {
    return context.rexBuilder.makeCall(
        getStringArrayType(context), SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, items);
  }

  /** Get RelDataType for string arrays */
  private static RelDataType getStringArrayType(CalcitePlanContext context) {
    return context
        .rexBuilder
        .getTypeFactory()
        .createArrayType(
            context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), -1);
  }
}
