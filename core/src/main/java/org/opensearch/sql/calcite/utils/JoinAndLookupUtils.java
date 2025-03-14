/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.EqualTo;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRexNodeVisitor;

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

  static Optional<UnresolvedExpression> buildLookupMappingCondition(Lookup node) {
    // only equi-join conditions are accepted in lookup command
    List<UnresolvedExpression> equiConditions = new ArrayList<>();
    for (Map.Entry<Field, Field> entry : node.getLookupMappingMap().entrySet()) {
      EqualTo equalTo;
      if (entry.getKey().getField() == entry.getValue().getField()) {
        Field lookupWithAlias = buildFieldWithLookupSubqueryAlias(node, entry.getKey());
        Field sourceWithAlias = buildFieldWithSourceSubqueryAlias(node, entry.getValue());
        equalTo = new EqualTo(sourceWithAlias, lookupWithAlias);
      } else {
        equalTo = new EqualTo(entry.getValue(), entry.getKey());
      }

      equiConditions.add(equalTo);
    }
    return equiConditions.stream().reduce(And::new);
  }

  static Field buildFieldWithLookupSubqueryAlias(Lookup node, Field field) {
    return new Field(
        QualifiedName.of(node.getLookupSubqueryAliasName(), field.getField().toString()));
  }

  static Field buildFieldWithSourceSubqueryAlias(Lookup node, Field field) {
    return new Field(
        QualifiedName.of(node.getSourceSubqueryAliasName(), field.getField().toString()));
  }

  /** lookup mapping fields + input fields */
  static List<RexNode> buildLookupRelationProjectList(
      Lookup node, CalciteRexNodeVisitor rexVisitor, CalcitePlanContext context) {
    List<Field> lookupMappingFields = new ArrayList<>(node.getLookupMappingMap().keySet());
    List<Field> inputFields = new ArrayList<>(node.getInputFieldList());
    if (inputFields.isEmpty()) {
      // All fields will be applied to the output if no input field is specified.
      return Collections.emptyList();
    }
    lookupMappingFields.addAll(inputFields);
    return buildProjectListFromFields(lookupMappingFields, rexVisitor, context);
  }

  static List<RexNode> buildProjectListFromFields(
      List<Field> fields, CalciteRexNodeVisitor rexVisitor, CalcitePlanContext context) {
    return fields.stream()
        .map(expr -> rexVisitor.analyze(expr, context))
        .collect(Collectors.toList());
  }

  static List<RexNode> buildOutputProjectList(
      Lookup node, CalciteRexNodeVisitor rexVisitor, CalcitePlanContext context) {
    List<RexNode> outputProjectList = new ArrayList<>();
    for (Map.Entry<Alias, Field> entry : node.getOutputCandidateMap().entrySet()) {
      Alias inputFieldWithAlias = entry.getKey();
      Field inputField = (Field) inputFieldWithAlias.getDelegated();
      Field outputField = entry.getValue();
      RexNode inputCol = rexVisitor.visitField(inputField, context);
      RexNode outputCol = rexVisitor.visitField(outputField, context);

      RexNode child;
      if (node.getOutputStrategy() == Lookup.OutputStrategy.APPEND) {
        child = context.rexBuilder.coalesce(outputCol, inputCol);
      } else {
        child = inputCol;
      }
      RexNode output = context.relBuilder.alias(child, inputFieldWithAlias.getName());
      outputProjectList.add(output);
    }
    return outputProjectList;
  }
}
