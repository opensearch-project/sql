/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Utility class for unifying schemas across multiple RelNodes. Supports two strategies:
 *
 * <ul>
 *   <li>Conflict resolution (multisearch): throws on type mismatch, fills missing fields with NULL
 *   <li>Type coercion (union): widens compatible types (e.g. INTEGER→BIGINT), falls back to VARCHAR
 *       for incompatible types, fills missing fields with NULL
 * </ul>
 */
public class SchemaUnifier {

  /**
   * Builds a unified schema for multiple nodes. Throws an exception if type conflicts are detected.
   *
   * @param nodes List of RelNodes to unify schemas for
   * @param context Calcite plan context
   * @return List of projected RelNodes with unified schema
   * @throws IllegalArgumentException if type conflicts are detected
   */
  public static List<RelNode> buildUnifiedSchemaWithConflictResolution(
      List<RelNode> nodes, CalcitePlanContext context) {
    if (nodes.isEmpty()) {
      return new ArrayList<>();
    }

    if (nodes.size() == 1) {
      return nodes;
    }

    // Step 1: Build the unified schema by processing all nodes (throws on conflict)
    List<SchemaField> unifiedSchema = buildUnifiedSchema(nodes);

    // Step 2: Create projections for each node to align with unified schema
    List<RelNode> projectedNodes = new ArrayList<>();
    List<String> fieldNames =
        unifiedSchema.stream().map(SchemaField::getName).collect(Collectors.toList());

    for (RelNode node : nodes) {
      List<RexNode> projection = buildProjectionForNode(node, unifiedSchema, context);
      RelNode projectedNode = context.relBuilder.push(node).project(projection, fieldNames).build();
      projectedNodes.add(projectedNode);
    }

    return projectedNodes;
  }

  /**
   * Builds a unified schema by merging fields from all nodes. Throws an exception if fields with
   * the same name have different types.
   *
   * @param nodes List of RelNodes to merge schemas from
   * @return List of SchemaField representing the unified schema
   * @throws IllegalArgumentException if type conflicts are detected
   */
  private static List<SchemaField> buildUnifiedSchema(List<RelNode> nodes) {
    List<SchemaField> schema = new ArrayList<>();
    Map<String, RelDataType> seenFields = new HashMap<>();

    for (RelNode node : nodes) {
      for (RelDataTypeField field : node.getRowType().getFieldList()) {
        String fieldName = field.getName();
        RelDataType fieldType = field.getType();

        RelDataType existingType = seenFields.get(fieldName);
        if (existingType == null) {
          // New field - add to schema
          schema.add(new SchemaField(fieldName, fieldType));
          seenFields.put(fieldName, fieldType);
        } else if (!areTypesCompatible(existingType, fieldType)) {
          // Same field name but different type - throw exception
          throw new IllegalArgumentException(
              String.format(
                  "Unable to process column '%s' due to incompatible types: '%s' and '%s'",
                  fieldName, existingType.getSqlTypeName(), fieldType.getSqlTypeName()));
        }
        // If we've seen this exact (name, type) combination, skip it
      }
    }

    return schema;
  }

  private static boolean areTypesCompatible(RelDataType type1, RelDataType type2) {
    return type1.getSqlTypeName() != null && type1.getSqlTypeName().equals(type2.getSqlTypeName());
  }

  /**
   * Builds a projection for a node to align with the unified schema. For each field in the unified
   * schema: - If the node has a matching field with the same type, use it - Otherwise, project NULL
   *
   * @param node The node to build projection for
   * @param unifiedSchema List of SchemaField representing the unified schema
   * @param context Calcite plan context
   * @return List of RexNode representing the projection
   */
  private static List<RexNode> buildProjectionForNode(
      RelNode node, List<SchemaField> unifiedSchema, CalcitePlanContext context) {
    Map<String, RelDataTypeField> nodeFieldMap =
        node.getRowType().getFieldList().stream()
            .collect(Collectors.toMap(RelDataTypeField::getName, field -> field));

    List<RexNode> projection = new ArrayList<>();
    for (SchemaField schemaField : unifiedSchema) {
      String fieldName = schemaField.getName();
      RelDataType expectedType = schemaField.getType();
      RelDataTypeField nodeField = nodeFieldMap.get(fieldName);

      if (nodeField != null && areTypesCompatible(nodeField.getType(), expectedType)) {
        // Field exists with compatible type - use it
        projection.add(context.rexBuilder.makeInputRef(node, nodeField.getIndex()));
      } else {
        // Field missing or type mismatch - project NULL
        projection.add(context.rexBuilder.makeNullLiteral(expectedType));
      }
    }

    return projection;
  }

  /** Represents a field in the unified schema with name and type. */
  private static class SchemaField {
    private final String name;
    private final RelDataType type;

    SchemaField(String name, RelDataType type) {
      this.name = name;
      this.type = type;
    }

    String getName() {
      return name;
    }

    RelDataType getType() {
      return type;
    }
  }

  /**
   * Builds unified schema with type coercion for UNION command. Coerces compatible types to a
   * common supertype (e.g. int+float→float), falls back to VARCHAR for incompatible types, and
   * fills missing fields with NULL.
   */
  public static List<RelNode> buildUnifiedSchemaWithTypeCoercion(
      List<RelNode> inputs, CalcitePlanContext context) {
    if (inputs.isEmpty() || inputs.size() == 1) {
      return inputs;
    }

    List<RelNode> coercedInputs = coerceUnionTypes(inputs, context);
    return unifySchemasForUnion(coercedInputs, context);
  }

  /**
   * Aligns schemas by projecting NULL for missing fields and CAST for type mismatches. Uses
   * force=true to clear collation traits and prevent EnumerableMergeUnion cast exception.
   */
  private static List<RelNode> unifySchemasForUnion(
      List<RelNode> inputs, CalcitePlanContext context) {
    List<SchemaField> unifiedSchema = buildUnifiedSchemaForUnion(inputs);
    List<String> fieldNames =
        unifiedSchema.stream().map(SchemaField::getName).collect(Collectors.toList());

    List<RelNode> projectedNodes = new ArrayList<>();
    for (RelNode node : inputs) {
      List<RexNode> projection = buildProjectionForUnion(node, unifiedSchema, context);
      RelNode projectedNode =
          context.relBuilder.push(node).project(projection, fieldNames, true).build();
      projectedNodes.add(projectedNode);
    }
    return projectedNodes;
  }

  private static List<SchemaField> buildUnifiedSchemaForUnion(List<RelNode> nodes) {
    List<SchemaField> schema = new ArrayList<>();
    Map<String, RelDataType> seenFields = new HashMap<>();

    for (RelNode node : nodes) {
      for (RelDataTypeField field : node.getRowType().getFieldList()) {
        if (!seenFields.containsKey(field.getName())) {
          schema.add(new SchemaField(field.getName(), field.getType()));
          seenFields.put(field.getName(), field.getType());
        }
      }
    }
    return schema;
  }

  private static List<RexNode> buildProjectionForUnion(
      RelNode node, List<SchemaField> unifiedSchema, CalcitePlanContext context) {
    Map<String, RelDataTypeField> nodeFieldMap =
        node.getRowType().getFieldList().stream()
            .collect(Collectors.toMap(RelDataTypeField::getName, field -> field));

    List<RexNode> projection = new ArrayList<>();
    for (SchemaField schemaField : unifiedSchema) {
      RelDataTypeField nodeField = nodeFieldMap.get(schemaField.getName());

      if (nodeField != null) {
        RexNode fieldRef = context.rexBuilder.makeInputRef(node, nodeField.getIndex());
        if (!nodeField.getType().equals(schemaField.getType())) {
          projection.add(context.rexBuilder.makeCast(schemaField.getType(), fieldRef));
        } else {
          projection.add(fieldRef);
        }
      } else {
        projection.add(context.rexBuilder.makeNullLiteral(schemaField.getType()));
      }
    }
    return projection;
  }

  /** Casts fields to their common supertypes across all inputs when types differ. */
  private static List<RelNode> coerceUnionTypes(List<RelNode> inputs, CalcitePlanContext context) {
    Map<String, List<SqlTypeName>> fieldTypeMap = new HashMap<>();
    for (RelNode input : inputs) {
      for (RelDataTypeField field : input.getRowType().getFieldList()) {
        String fieldName = field.getName();
        SqlTypeName typeName = field.getType().getSqlTypeName();
        if (typeName != null) {
          fieldTypeMap.computeIfAbsent(fieldName, k -> new ArrayList<>()).add(typeName);
        }
      }
    }

    Map<String, SqlTypeName> targetTypeMap = new HashMap<>();
    for (Map.Entry<String, List<SqlTypeName>> entry : fieldTypeMap.entrySet()) {
      String fieldName = entry.getKey();
      List<SqlTypeName> types = entry.getValue();

      SqlTypeName commonType = types.getFirst();
      for (int i = 1; i < types.size(); i++) {
        commonType = findCommonTypeForUnion(commonType, types.get(i));
      }
      targetTypeMap.put(fieldName, commonType);
    }

    boolean needsCoercion = false;
    for (RelNode input : inputs) {
      for (RelDataTypeField field : input.getRowType().getFieldList()) {
        SqlTypeName targetType = targetTypeMap.get(field.getName());
        if (targetType != null && field.getType().getSqlTypeName() != targetType) {
          needsCoercion = true;
          break;
        }
      }
      if (needsCoercion) break;
    }

    if (!needsCoercion) {
      return inputs;
    }

    List<RelNode> coercedInputs = new ArrayList<>();
    for (RelNode input : inputs) {
      List<RexNode> projections = new ArrayList<>();
      List<String> projectionNames = new ArrayList<>();
      boolean needsProjection = false;

      for (RelDataTypeField field : input.getRowType().getFieldList()) {
        String fieldName = field.getName();
        SqlTypeName currentType = field.getType().getSqlTypeName();
        SqlTypeName targetType = targetTypeMap.get(fieldName);

        RexNode fieldRef = context.rexBuilder.makeInputRef(input, field.getIndex());

        if (currentType != targetType && targetType != null) {
          projections.add(context.relBuilder.cast(fieldRef, targetType));
          needsProjection = true;
        } else {
          projections.add(fieldRef);
        }
        projectionNames.add(fieldName);
      }

      if (needsProjection) {
        context.relBuilder.push(input);
        context.relBuilder.project(projections, projectionNames, true);
        coercedInputs.add(context.relBuilder.build());
      } else {
        coercedInputs.add(input);
      }
    }

    return coercedInputs;
  }

  /**
   * Returns the wider type for two SqlTypeNames. Within the same family, returns the wider type
   * (e.g. INTEGER+BIGINT-->BIGINT). Across families, falls back to VARCHAR.
   */
  private static SqlTypeName findCommonTypeForUnion(SqlTypeName type1, SqlTypeName type2) {
    if (type1 == type2) {
      return type1;
    }

    if (type1 == SqlTypeName.NULL) {
      return type2;
    }
    if (type2 == SqlTypeName.NULL) {
      return type1;
    }

    if (isNumericTypeForUnion(type1) && isNumericTypeForUnion(type2)) {
      return getWiderNumericTypeForUnion(type1, type2);
    }

    if (isStringTypeForUnion(type1) && isStringTypeForUnion(type2)) {
      return SqlTypeName.VARCHAR;
    }

    if (isTemporalTypeForUnion(type1) && isTemporalTypeForUnion(type2)) {
      return getWiderTemporalTypeForUnion(type1, type2);
    }

    return SqlTypeName.VARCHAR;
  }

  private static boolean isNumericTypeForUnion(SqlTypeName typeName) {
    return typeName == SqlTypeName.TINYINT
        || typeName == SqlTypeName.SMALLINT
        || typeName == SqlTypeName.INTEGER
        || typeName == SqlTypeName.BIGINT
        || typeName == SqlTypeName.FLOAT
        || typeName == SqlTypeName.REAL
        || typeName == SqlTypeName.DOUBLE
        || typeName == SqlTypeName.DECIMAL;
  }

  private static boolean isStringTypeForUnion(SqlTypeName typeName) {
    return typeName == SqlTypeName.CHAR || typeName == SqlTypeName.VARCHAR;
  }

  private static boolean isTemporalTypeForUnion(SqlTypeName typeName) {
    return typeName == SqlTypeName.DATE
        || typeName == SqlTypeName.TIMESTAMP
        || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
  }

  private static SqlTypeName getWiderNumericTypeForUnion(SqlTypeName type1, SqlTypeName type2) {
    int rank1 = getNumericTypeRankForUnion(type1);
    int rank2 = getNumericTypeRankForUnion(type2);
    return rank1 >= rank2 ? type1 : type2;
  }

  private static int getNumericTypeRankForUnion(SqlTypeName typeName) {
    return switch (typeName) {
      case TINYINT -> 1;
      case SMALLINT -> 2;
      case INTEGER -> 3;
      case BIGINT -> 4;
      case DECIMAL -> 5;
      case REAL -> 6;
      case FLOAT -> 7;
      case DOUBLE -> 8;
      default -> 0;
    };
  }

  private static SqlTypeName getWiderTemporalTypeForUnion(SqlTypeName type1, SqlTypeName type2) {
    if (type1 == SqlTypeName.TIMESTAMP || type2 == SqlTypeName.TIMESTAMP) {
      return SqlTypeName.TIMESTAMP;
    }
    if (type1 == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        || type2 == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }
    return SqlTypeName.DATE;
  }
}
