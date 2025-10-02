/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

/**
 * Utility class for unifying schemas across multiple RelNodes with type conflict resolution. Uses
 * the same strategy as append command - renames conflicting fields to avoid type conflicts.
 */
public class SchemaUnifier {

  /**
   * Builds a unified schema for multiple nodes with type conflict resolution.
   *
   * @param nodes List of RelNodes to unify schemas for
   * @param context Calcite plan context
   * @return List of projected RelNodes with unified schema
   */
  public static List<RelNode> buildUnifiedSchemaWithConflictResolution(
      List<RelNode> nodes, CalcitePlanContext context) {
    if (nodes.isEmpty()) {
      return new ArrayList<>();
    }

    if (nodes.size() == 1) {
      return nodes;
    }

    // Step 1: Build the unified schema by processing all nodes
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

    // Step 3: Unify names to handle type conflicts (this creates age0, age1, etc.)
    List<String> uniqueNames =
        SqlValidatorUtil.uniquify(fieldNames, SqlValidatorUtil.EXPR_SUGGESTER, true);

    // Step 4: Re-project with unique names if needed
    if (!uniqueNames.equals(fieldNames)) {
      List<RelNode> renamedNodes = new ArrayList<>();
      for (RelNode node : projectedNodes) {
        RelNode renamedNode =
            context.relBuilder.push(node).project(context.relBuilder.fields(), uniqueNames).build();
        renamedNodes.add(renamedNode);
      }
      return renamedNodes;
    }

    return projectedNodes;
  }

  /**
   * Builds a unified schema by merging fields from all nodes. Fields with the same name but
   * different types are added as separate entries (which will be renamed during uniquification).
   *
   * @param nodes List of RelNodes to merge schemas from
   * @return List of SchemaField representing the unified schema (may contain duplicate names)
   */
  private static List<SchemaField> buildUnifiedSchema(List<RelNode> nodes) {
    List<SchemaField> schema = new ArrayList<>();
    Map<String, Set<RelDataType>> seenFields = new HashMap<>();

    for (RelNode node : nodes) {
      for (RelDataTypeField field : node.getRowType().getFieldList()) {
        String fieldName = field.getName();
        RelDataType fieldType = field.getType();

        // Track which (name, type) combinations we've seen
        Set<RelDataType> typesForName = seenFields.computeIfAbsent(fieldName, k -> new HashSet<>());

        if (!typesForName.contains(fieldType)) {
          // New field or same name with different type - add to schema
          schema.add(new SchemaField(fieldName, fieldType));
          typesForName.add(fieldType);
        }
        // If we've seen this exact (name, type) combination, skip it
      }
    }

    return schema;
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

      if (nodeField != null && nodeField.getType().equals(expectedType)) {
        // Field exists with matching type - use it
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
