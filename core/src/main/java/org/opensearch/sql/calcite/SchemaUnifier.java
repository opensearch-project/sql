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

/**
 * Utility class for unifying schemas across multiple RelNodes. Throws an exception when type
 * conflicts are detected.
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
}
