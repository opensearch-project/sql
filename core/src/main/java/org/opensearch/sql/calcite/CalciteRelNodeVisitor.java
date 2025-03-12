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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.util.Holder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Map;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.subquery.SubqueryExpression;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.utils.JoinAndLookupUtils;
import org.opensearch.sql.exception.SemanticCheckException;

public class CalciteRelNodeVisitor extends AbstractNodeVisitor<RelNode, CalcitePlanContext> {

  private final CalciteRexNodeVisitor rexVisitor;
  private final CalciteAggCallVisitor aggVisitor;

  public CalciteRelNodeVisitor() {
    this.rexVisitor = new CalciteRexNodeVisitor(this);
    this.aggVisitor = new CalciteAggCallVisitor(rexVisitor);
  }

  public RelNode analyze(UnresolvedPlan unresolved, CalcitePlanContext context) {
    return unresolved.accept(this, context);
  }

  @Override
  public RelNode visitRelation(Relation node, CalcitePlanContext context) {
    context.relBuilder.scan(node.getTableQualifiedName().getParts());
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
    boolean containsSubqueryExpression = containsSubqueryExpression(node.getCondition());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    if (containsSubqueryExpression) {
      context.relBuilder.variable(v::set);
      context.pushCorrelVar(v.get());
    }
    RexNode condition = rexVisitor.analyze(node.getCondition(), context);
    if (containsSubqueryExpression) {
      context.relBuilder.filter(ImmutableList.of(v.get().id), condition);
      context.popCorrelVar();
    } else {
      context.relBuilder.filter(condition);
    }
    return context.relBuilder.peek();
  }

  private boolean containsSubqueryExpression(Node expr) {
    if (expr == null) {
      return false;
    }
    if (expr instanceof SubqueryExpression) {
      return true;
    }
    if (expr instanceof Let l) {
      return containsSubqueryExpression(l.getExpression());
    }
    for (Node child : expr.getChild()) {
      if (containsSubqueryExpression(child)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public RelNode visitProject(Project node, CalcitePlanContext context) {
    visitChildren(node, context);
    List<RexNode> projectList;
    if (node.getProjectList().stream().anyMatch(e -> e instanceof AllFields)) {
      return context.relBuilder.peek();
    } else {
      projectList =
          node.getProjectList().stream()
              .map(expr -> rexVisitor.analyze(expr, context))
              .collect(Collectors.toList());
    }
    if (node.isExcluded()) {
      context.relBuilder.projectExcept(projectList);
    } else {
      context.relBuilder.project(projectList);
    }
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitRename(Rename node, CalcitePlanContext context) {
    visitChildren(node, context);
    List<String> originalNames = context.relBuilder.peek().getRowType().getFieldNames();
    List<String> newNames = new ArrayList<>(originalNames);
    for (Map renameMap : node.getRenameList()) {
      if (renameMap.getTarget() instanceof Field t) {
        String newName = t.getField().toString();
        RexNode check = rexVisitor.analyze(renameMap.getOrigin(), context);
        if (check instanceof RexInputRef ref) {
          newNames.set(ref.getIndex(), newName);
        } else {
          throw new SemanticCheckException(
              String.format("the original field %s cannot be resolved", renameMap.getOrigin()));
        }
      } else {
        throw new SemanticCheckException(
            String.format("the target expected to be field, but is %s", renameMap.getTarget()));
      }
    }
    context.relBuilder.rename(newNames);
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
                  boolean containsSubqueryExpression = containsSubqueryExpression(expr);
                  final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
                  if (containsSubqueryExpression) {
                    context.relBuilder.variable(v::set);
                    context.pushCorrelVar(v.get());
                  }
                  RexNode eval = rexVisitor.analyze(expr, context);
                  if (containsSubqueryExpression) {
                    // RelBuilder.projectPlus doesn't have a parameter with variablesSet:
                    // projectPlus(Iterable<CorrelationId> variablesSet, RexNode... nodes)
                    context.relBuilder.project(
                        Iterables.concat(context.relBuilder.fields(), ImmutableList.of(eval)),
                        ImmutableList.of(),
                        false,
                        ImmutableList.of(v.get().id));
                    context.popCorrelVar();
                  } else {
                    context.relBuilder.projectPlus(eval);
                  }
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

      // the overriding field in Calcite will add a numeric suffix, for example:
      // `| eval SAL = SAL + 1` creates a field SAL0 to replace SAL, so we rename it back to SAL,
      // or query `| eval SAL=SAL + 1 | where exists [ source=DEPT | where emp.SAL=HISAL ]` fails.
      List<String> newNames =
          context.relBuilder.peek().getRowType().getFieldNames().stream()
              .map(
                  cur -> {
                    String noNumericSuffix = cur.replaceAll("\\d", "");
                    if (overriding.contains(noNumericSuffix)) {
                      return noNumericSuffix;
                    } else {
                      return cur;
                    }
                  })
              .toList();
      context.relBuilder.rename(newNames);
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
    // The span column is always the first column in result whatever
    // the order of span in query is first or last one
    List<RexNode> groupByList = new ArrayList<>();
    UnresolvedExpression span = node.getSpan();
    if (!Objects.isNull(span)) {
      RexNode spanRex = rexVisitor.analyze(span, context);
      groupByList.add(spanRex);
      // add span's group alias field (most recent added expression)
    }
    groupByList.addAll(
        node.getGroupExprList().stream().map(expr -> rexVisitor.analyze(expr, context)).toList());

    context.relBuilder.aggregate(context.relBuilder.groupKey(groupByList), aggList);

    // handle normal aggregate
    // TODO Should we keep alignment with V2 behaviour in new Calcite implementation?
    // TODO how about add a legacy enable config to control behaviour in Calcite?
    // Some behaviours between PPL and Databases are different.
    // As an example, in command `stats count() by colA, colB`:
    // 1. the sequence of output schema is different:
    // In PPL v2, the sequence of output schema is "count, colA, colB".
    // But in most databases, the sequence of output schema is "colA, colB, count".
    // 2. the output order is different:
    // In PPL v2, the order of output results is ordered by "colA + colB".
    // But in most databases, the output order is random.
    // User must add ORDER BY clause after GROUP BY clause to keep the results aligning.
    // Following logic is to align with the PPL legacy behaviour.

    // alignment for 1.sequence of output schema: adding order-by
    // we use the groupByList instead of node.getSortExprList as input because
    // the groupByList may include span column.
    node.getGroupExprList()
        .forEach(
            g -> {
              // node.getGroupExprList() should all be instance of Alias
              // which defined in AstBuilder.
              assert g instanceof Alias;
            });
    List<String> aliasesFromGroupByList =
        groupByList.stream()
            .map(this::extractAliasLiteral)
            .flatMap(Optional::stream)
            .map(ref -> ((RexLiteral) ref).getValueAs(String.class))
            .toList();
    List<RexNode> aliasedGroupByList =
        aliasesFromGroupByList.stream()
            .map(context.relBuilder::field)
            .map(f -> (RexNode) f)
            .toList();
    context.relBuilder.sort(aliasedGroupByList);

    // alignment for 2.the output order: schema reordering
    List<RexNode> outputFields = context.relBuilder.fields();
    int numOfOutputFields = outputFields.size();
    int numOfAggList = aggList.size();
    List<RexNode> reordered = new ArrayList<>(numOfOutputFields);
    // Add aggregation results first
    List<RexNode> aggRexList =
        outputFields.subList(numOfOutputFields - numOfAggList, numOfOutputFields);
    reordered.addAll(aggRexList);
    // Add group by columns
    reordered.addAll(aliasedGroupByList);
    context.relBuilder.project(reordered);

    return context.relBuilder.peek();
  }

  /** extract the RexLiteral of Alias from a node */
  private Optional<RexLiteral> extractAliasLiteral(RexNode node) {
    if (node == null) {
      return Optional.empty();
    } else if (node.getKind() == SqlKind.AS) {
      return Optional.of((RexLiteral) ((RexCall) node).getOperands().get(1));
    } else {
      return Optional.empty();
    }
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
