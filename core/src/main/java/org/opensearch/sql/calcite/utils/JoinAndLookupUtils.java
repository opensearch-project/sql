/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.calcite.CalcitePlanContext;

public interface JoinAndLookupUtils {

  static JoinRelType translateJoinType(Join.JoinType joinType) {
    switch (joinType) {
      case LEFT:
        return JoinRelType.LEFT;
      case RIGHT:
        return JoinRelType.RIGHT;
      case FULL:
        return JoinRelType.FULL;
      case SEMI:
        return JoinRelType.SEMI;
      case ANTI:
        return JoinRelType.ANTI;
      case INNER:
      default:
        return JoinRelType.INNER;
    }
  }

  /* ------For Lookup------ */

  /**
   * Construct a duplicated field map: Key comes from providedFieldNames -> Value comes from
   * sourceFieldsNames. A provided field is detected to be duplicated if it has the same name with a
   * source field after alias mapping.
   */
  static Map<String, String> findDuplicatedFields(
      Lookup node, List<String> sourceFieldsNames, List<String> providedFieldNames) {
    return providedFieldNames.stream()
        .map(k -> Pair.of(node.getOutputAliasMap().getOrDefault(k, k), k))
        .filter(pair -> sourceFieldsNames.contains(pair.getKey()))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /**
   * For lookup table, if the output fields are specified, try to add a project list for it. Note,
   * join will fail if the mapping fields are excluded.
   */
  static void addProjectionIfNecessary(Lookup node, CalcitePlanContext context) {
    List<String> mappingField = node.getMappingAliasMap().keySet().stream().toList();
    List<String> outputField = node.getOutputAliasMap().keySet().stream().toList();
    if (!outputField.isEmpty()) {
      HashSet<String> lookupMappingFields = new HashSet<>(outputField);
      lookupMappingFields.addAll(mappingField);
      if (lookupMappingFields.size() != context.relBuilder.fields().size()) {
        List<RexNode> projectList =
            lookupMappingFields.stream()
                .map(fieldName -> (RexNode) context.relBuilder.field(fieldName))
                .toList();
        context.relBuilder.project(projectList);
      }
    }
  }

  static void addJoinForLookUp(Lookup node, CalcitePlanContext context) {
    RexNode joinCondition =
        node.getMappingAliasMap().entrySet().stream()
            .map(
                entry -> {
                  RexNode lookupKey = analyzeFieldsForLookUp(entry.getKey(), false, context);
                  RexNode sourceKey = analyzeFieldsForLookUp(entry.getValue(), true, context);
                  return context.rexBuilder.equals(sourceKey, lookupKey);
                })
            .reduce(context.rexBuilder::and)
            .orElse(context.relBuilder.literal(true));
    context.relBuilder.join(JoinRelType.LEFT, joinCondition);
  }

  static RexNode analyzeFieldsForLookUp(
      String fieldName, boolean isSourceTable, CalcitePlanContext context) {
    return context.relBuilder.field(2, isSourceTable ? 0 : 1, fieldName);
  }
}
