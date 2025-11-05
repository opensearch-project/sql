/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.rel.QualifiedNameResolver;

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
      if (lookupMappingFields.size() != context.fieldBuilder.staticFields().size()) {
        List<RexNode> projectList =
            lookupMappingFields.stream()
                .map(
                    fieldName ->
                        (RexNode) QualifiedNameResolver.resolveFieldOrThrow(fieldName, context))
                .toList();
        context.relBuilder.project(projectList);
      }
    }
  }

  /** Utility to verify join condition does not use ANY typed field to avoid */
  static void verifyJoinConditionNotUseAnyType(RexNode rexNode, CalcitePlanContext context) {
    rexNode.accept(
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitCall(RexCall call) {
            if (call.getKind() == SqlKind.EQUALS) {
              RexNode left = call.operands.get(0);
              RexNode right = call.operands.get(1);
              if (context.fieldBuilder.isAnyType(left) || context.fieldBuilder.isAnyType(right)) {
                throw new IllegalArgumentException(
                    "Join condition needs to use specific type. Please cast explicitly.");
              }
            }
            return super.visitCall(call);
          }
        });
  }

  static void addJoinForLookUp(Lookup node, CalcitePlanContext context) {
    RexNode joinCondition =
        node.getMappingAliasMap().entrySet().stream()
            .map(
                entry -> {
                  RexNode lookupKey = analyzeFieldsInRight(entry.getKey(), context);
                  RexNode sourceKey = analyzeFieldsInLeft(entry.getValue(), context);
                  if (context.fieldBuilder.isAnyType(sourceKey)) {
                    throw new IllegalArgumentException(
                        String.format(
                            "Source key `%s` needs to be specific type. Please cast explicitly.",
                            entry.getValue()));
                  }
                  return context.rexBuilder.equals(sourceKey, lookupKey);
                })
            .reduce(context.rexBuilder::and)
            .orElse(context.relBuilder.literal(true));
    context.relBuilder.join(JoinRelType.LEFT, joinCondition);
  }

  static RexNode analyzeFieldsInLeft(String fieldName, CalcitePlanContext context) {
    return QualifiedNameResolver.resolveField(2, 0, fieldName, context)
        .orElseThrow(() -> new IllegalArgumentException("field not found: " + fieldName));
  }

  static RexNode analyzeFieldsInRight(String fieldName, CalcitePlanContext context) {
    return QualifiedNameResolver.resolveField(2, 1, fieldName, context)
        .orElseThrow(() -> new IllegalArgumentException("field not found: " + fieldName));
  }

  static void renameToExpectedFields(
      List<String> expectedProvidedFieldNames,
      int sourceFieldsCountLeft,
      CalcitePlanContext context) {
    List<String> oldFields = context.fieldBuilder.getStaticFieldNames();
    assert sourceFieldsCountLeft + expectedProvidedFieldNames.size() == oldFields.size()
        : "The source fields count left plus new provided fields count must equal to the output"
            + " fields count of current plan(i.e project-join).";
    List<String> newFields = new ArrayList<>(oldFields.size());
    newFields.addAll(oldFields.subList(0, sourceFieldsCountLeft));
    newFields.addAll(expectedProvidedFieldNames);
    context.relBuilder.rename(newFields);
  }
}
