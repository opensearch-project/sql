/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.apache.calcite.sql.SqlKind.AS;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_FIRST;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_LAST;
import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_DESC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.ASC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.DESC;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.utils.JoinAndLookupUtils;

public class CalciteRelNodeVisitor extends AbstractNodeVisitor<RelNode, CalcitePlanContext> {

  private final CalciteRexNodeVisitor rexVisitor;
  private final CalciteAggCallVisitor aggVisitor;

  public CalciteRelNodeVisitor() {
    this.rexVisitor = new CalciteRexNodeVisitor();
    this.aggVisitor = new CalciteAggCallVisitor(rexVisitor);
  }

  public RelNode analyze(UnresolvedPlan unresolved, CalcitePlanContext context) {
    return unresolved.accept(this, context);
  }

  @Override
  public RelNode visitRelation(Relation node, CalcitePlanContext context) {
    for (QualifiedName qualifiedName : node.getQualifiedNames()) {
      SchemaPlus schema = context.config.getDefaultSchema();
      if (schema != null && schema.getName().equals(OpenSearchSchema.OPEN_SEARCH_SCHEMA_NAME)) {
        schema.unwrap(OpenSearchSchema.class).registerTable(qualifiedName);
      }
      context.relBuilder.scan(qualifiedName.getParts());
    }
    if (node.getQualifiedNames().size() > 1) {
      context.relBuilder.union(true, node.getQualifiedNames().size());
    }
    return context.relBuilder.peek();
  }

  // This is a tool method to add an existed RelOptTable to builder stack, not used for now
  private RelBuilder scan(RelOptTable tableSchema, CalcitePlanContext context) {
    final RelNode scan =
        context
            .relBuilder
            .getScanFactory()
            .createScan(ViewExpanders.simpleContext(context.relBuilder.getCluster()), tableSchema);
    context.relBuilder.push(scan);
    return context.relBuilder;
  }

  @Override
  public RelNode visitFilter(Filter node, CalcitePlanContext context) {
    visitChildren(node, context);
    RexNode condition = rexVisitor.analyze(node.getCondition(), context);
    context.relBuilder.filter(condition);
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitProject(Project node, CalcitePlanContext context) {
    visitChildren(node, context);
    List<RexNode> projectList =
        node.getProjectList().stream()
            .filter(expr -> !(expr instanceof AllFields))
            .map(expr -> rexVisitor.analyze(expr, context))
            .collect(Collectors.toList());
    if (projectList.isEmpty()) {
      return context.relBuilder.peek();
    }
    if (node.isExcluded()) {
      context.relBuilder.projectExcept(projectList);
    } else {
      context.relBuilder.project(projectList);
    }
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitSort(Sort node, CalcitePlanContext context) {
    visitChildren(node, context);
    List<RexNode> sortList =
        node.getSortList().stream()
            .map(
                expr -> {
                  RexNode sortField = rexVisitor.analyze(expr, context);
                  Sort.SortOption sortOption = analyzeSortOption(expr.getFieldArgs());
                  if (sortOption == DEFAULT_DESC) {
                    return context.relBuilder.desc(sortField);
                  } else {
                    return sortField;
                  }
                })
            .collect(Collectors.toList());
    context.relBuilder.sort(sortList);
    return context.relBuilder.peek();
  }

  private Sort.SortOption analyzeSortOption(List<Argument> fieldArgs) {
    Boolean asc = (Boolean) fieldArgs.get(0).getValue().getValue();
    Optional<Argument> nullFirst =
        fieldArgs.stream().filter(option -> "nullFirst".equals(option.getArgName())).findFirst();

    if (nullFirst.isPresent()) {
      Boolean isNullFirst = (Boolean) nullFirst.get().getValue().getValue();
      return new Sort.SortOption((asc ? ASC : DESC), (isNullFirst ? NULL_FIRST : NULL_LAST));
    }
    return asc ? Sort.SortOption.DEFAULT_ASC : DEFAULT_DESC;
  }

  @Override
  public RelNode visitHead(Head node, CalcitePlanContext context) {
    visitChildren(node, context);
    context.relBuilder.limit(node.getFrom(), node.getSize());
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitEval(Eval node, CalcitePlanContext context) {
    visitChildren(node, context);
    List<String> originalFieldNames = context.relBuilder.peek().getRowType().getFieldNames();
    List<RexNode> evalList =
        node.getExpressionList().stream()
            .map(
                expr -> {
                  RexNode eval = rexVisitor.analyze(expr, context);
                  context.relBuilder.projectPlus(eval);
                  return eval;
                })
            .collect(Collectors.toList());
    // Overriding the existing field if the alias has the same name with original field name. For
    // example, eval field = 1
    List<String> overriding =
        evalList.stream()
            .filter(expr -> expr.getKind() == AS)
            .map(
                expr ->
                    ((RexLiteral) ((RexCall) expr).getOperands().get(1)).getValueAs(String.class))
            .collect(Collectors.toList());
    overriding.retainAll(originalFieldNames);
    if (!overriding.isEmpty()) {
      List<RexNode> toDrop = context.relBuilder.fields(overriding);
      context.relBuilder.projectExcept(toDrop);
    }
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitAggregation(Aggregation node, CalcitePlanContext context) {
    visitChildren(node, context);
    List<AggCall> aggList =
        node.getAggExprList().stream()
            .map(expr -> aggVisitor.analyze(expr, context))
            .collect(Collectors.toList());
    List<RexNode> groupByList =
        node.getGroupExprList().stream()
            .map(expr -> rexVisitor.analyze(expr, context))
            .collect(Collectors.toList());

    UnresolvedExpression span = node.getSpan();
    if (!Objects.isNull(span)) {
      RexNode spanRex = rexVisitor.analyze(span, context);
      groupByList.add(spanRex);
      // add span's group alias field (most recent added expression)
    }
    //        List<RexNode> aggList = node.getAggExprList().stream()
    //            .map(expr -> rexVisitor.analyze(expr, context))
    //            .collect(Collectors.toList());
    //        relBuilder.aggregate(relBuilder.groupKey(groupByList),
    //            aggList.stream().map(rex -> (MyAggregateCall) rex)
    //                .map(MyAggregateCall::getCall).collect(Collectors.toList()));
    context.relBuilder.aggregate(context.relBuilder.groupKey(groupByList), aggList);
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitJoin(Join node, CalcitePlanContext context) {
    List<UnresolvedPlan> children = node.getChildren();
    children.forEach(c -> analyze(c, context));
    RexNode joinCondition =
        node.getJoinCondition()
            .map(c -> rexVisitor.analyzeJoinCondition(c, context))
            .orElse(context.relBuilder.literal(true));
    context.relBuilder.join(
        JoinAndLookupUtils.translateJoinType(node.getJoinType()), joinCondition);
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitSubqueryAlias(SubqueryAlias node, CalcitePlanContext context) {
    visitChildren(node, context);
    context.relBuilder.as(node.getAlias());
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitLookup(Lookup node, CalcitePlanContext context) {
    // 1. resolve source side
    visitChildren(node, context);
    // get sourceOutputFields from top of stack which is used to build final output
    List<RexNode> sourceOutputFields = context.relBuilder.fields();

    // 2. resolve lookup table
    analyze(node.getLookupRelation(), context);
    // If the output fields are specified, build a project list for lookup table.
    // The mapping fields of lookup table should be added in this project list, otherwise join will
    // fail.
    // So the mapping fields of lookup table should be dropped after join.
    List<RexNode> projectList =
        JoinAndLookupUtils.buildLookupRelationProjectList(node, rexVisitor, context);
    if (!projectList.isEmpty()) {
      context.relBuilder.project(projectList);
    }

    // 3. resolve join condition
    RexNode joinCondition =
        JoinAndLookupUtils.buildLookupMappingCondition(node)
            .map(c -> rexVisitor.analyzeJoinCondition(c, context))
            .orElse(context.relBuilder.literal(true));

    // 4. If no output field is specified, all fields from lookup table are applied to the output.
    if (node.allFieldsShouldAppliedToOutputList()) {
      context.relBuilder.join(JoinRelType.LEFT, joinCondition);
      return context.relBuilder.peek();
    }

    // 5. push join to stack
    context.relBuilder.join(JoinRelType.LEFT, joinCondition);

    // 6. Drop the mapping fields of lookup table in result:
    // For example, in command "LOOKUP lookTbl Field1 AS Field2, Field3",
    // the Field1 and Field3 are projection fields and join keys which will be dropped in result.
    List<Field> mappingFieldsOfLookup =
        node.getLookupMappingMap().entrySet().stream()
            .map(
                kv ->
                    kv.getKey().getField() == kv.getValue().getField()
                        ? JoinAndLookupUtils.buildFieldWithLookupSubqueryAlias(node, kv.getKey())
                        : kv.getKey())
            .collect(Collectors.toList());
    List<RexNode> dropListOfLookupMappingFields =
        JoinAndLookupUtils.buildProjectListFromFields(mappingFieldsOfLookup, rexVisitor, context);
    // Drop the $sourceOutputField if existing
    List<RexNode> dropListOfSourceFields =
        node.getFieldListWithSourceSubqueryAlias().stream()
            .map(
                field -> {
                  try {
                    return rexVisitor.analyze(field, context);
                  } catch (RuntimeException e) {
                    // If the field is not found in the source, skip it
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    List<RexNode> toDrop = new ArrayList<>(dropListOfLookupMappingFields);
    toDrop.addAll(dropListOfSourceFields);

    // 7. build final outputs
    List<RexNode> outputFields = new ArrayList<>(sourceOutputFields);
    // Add new columns based on different strategies:
    // Append:  coalesce($outputField, $"inputField").as(outputFieldName)
    // Replace: $outputField.as(outputFieldName)
    outputFields.addAll(JoinAndLookupUtils.buildOutputProjectList(node, rexVisitor, context));
    outputFields.removeAll(toDrop);

    context.relBuilder.project(outputFields);

    return context.relBuilder.peek();
  }
}
