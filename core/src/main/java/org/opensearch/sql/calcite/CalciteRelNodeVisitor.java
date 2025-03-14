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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.util.Holder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.subquery.SubqueryExpression;
import org.opensearch.sql.ast.tree.AD;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.CloseCursor;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FetchCursor;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Kmeans;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Lookup.OutputStrategy;
import org.opensearch.sql.ast.tree.ML;
import org.opensearch.sql.ast.tree.Paginate;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.utils.JoinAndLookupUtils;
import org.opensearch.sql.exception.CalciteUnsupportedException;
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
    for (org.opensearch.sql.ast.expression.Map renameMap : node.getRenameList()) {
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
                  SortOption sortOption = analyzeSortOption(expr.getFieldArgs());
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

  private SortOption analyzeSortOption(List<Argument> fieldArgs) {
    Boolean asc = (Boolean) fieldArgs.get(0).getValue().getValue();
    Optional<Argument> nullFirst =
        fieldArgs.stream().filter(option -> "nullFirst".equals(option.getArgName())).findFirst();

    if (nullFirst.isPresent()) {
      Boolean isNullFirst = (Boolean) nullFirst.get().getValue().getValue();
      return new SortOption((asc ? ASC : DESC), (isNullFirst ? NULL_FIRST : NULL_LAST));
    }
    return asc ? SortOption.DEFAULT_ASC : DEFAULT_DESC;
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

    // schema reordering
    // As an example, in command `stats count() by colA, colB`,
    // the sequence of output schema is "count, colA, colB".
    List<RexNode> outputFields = context.relBuilder.fields();
    int numOfOutputFields = outputFields.size();
    int numOfAggList = aggList.size();
    List<RexNode> reordered = new ArrayList<>(numOfOutputFields);
    // Add aggregation results first
    List<RexNode> aggRexList =
        outputFields.subList(numOfOutputFields - numOfAggList, numOfOutputFields);
    reordered.addAll(aggRexList);
    // Add group by columns
    List<RexNode> aliasedGroupByList =
        groupByList.stream()
            .map(this::extractAliasLiteral)
            .flatMap(Optional::stream)
            .map(ref -> ((RexLiteral) ref).getValueAs(String.class))
            .map(context.relBuilder::field)
            .map(f -> (RexNode) f)
            .toList();
    reordered.addAll(aliasedGroupByList);
    context.relBuilder.project(reordered);

    return context.relBuilder.peek();
  }

  /** extract the RexLiteral of Alias from a node */
  private Optional<RexLiteral> extractAliasLiteral(RexNode node) {
    if (node == null) {
      return Optional.empty();
    } else if (node.getKind() == AS) {
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
    List<String> sourceFieldsNames = context.relBuilder.peek().getRowType().getFieldNames();

    // 2. resolve lookup table
    analyze(node.getLookupRelation(), context);

    // 3. Add projection for lookup table if needed
    JoinAndLookupUtils.addProjectionIfNecessary(node, context);

    // Get lookupColumns from top of stack (after above potential projection).
    List<String> lookupTableFieldNames = context.relBuilder.peek().getRowType().getFieldNames();

    // 3. Find fields which should be removed in lookup-table.
    // For lookup table, the mapping fields should be dropped after join
    // unless they are explicitly put in the output fields
    List<String> toBeRemovedLookupFieldNames =
        node.getMappingAliasMap().keySet().stream()
            .filter(k -> !node.getOutputAliasMap().containsKey(k))
            .toList();
    List<String> providedFieldNames =
        lookupTableFieldNames.stream()
            .filter(k -> !toBeRemovedLookupFieldNames.contains(k))
            .toList();
    List<RexNode> toBeRemovedLookupFields =
        toBeRemovedLookupFieldNames.stream()
            .map(d -> (RexNode) context.relBuilder.field(2, 1, d))
            .toList();
    List<RexNode> toBeRemovedFields = new ArrayList<>(toBeRemovedLookupFields);

    // 4. Find duplicated fields between source table fields and lookup table provided fields.
    // Key: source fields names, value: lookup table provided field names
    Map<String, String> duplicatedFieldNamesMap =
        JoinAndLookupUtils.findDuplicatedFields(node, sourceFieldsNames, providedFieldNames);

    List<RexNode> duplicatedSourceFields =
        duplicatedFieldNamesMap.keySet().stream()
            .map(field -> JoinAndLookupUtils.analyzeFieldsForLookUp(field, true, context))
            .toList();
    // Duplicated fields in source-field should always be removed.
    toBeRemovedFields.addAll(duplicatedSourceFields);
    // Construct a new field name for the new provided-fields.
    List<String> expectedProvidedFieldNames =
        providedFieldNames.stream().map(k -> node.getOutputAliasMap().getOrDefault(k, k)).toList();

    List<RexNode> newCoalesceList = new ArrayList<>();
    if (!duplicatedFieldNamesMap.isEmpty() && node.getOutputStrategy() == OutputStrategy.APPEND) {
      List<RexNode> duplicatedProvidedFields =
          duplicatedFieldNamesMap.values().stream()
              .map(field -> JoinAndLookupUtils.analyzeFieldsForLookUp(field, false, context))
              .toList();
      for (int i = 0; i < duplicatedProvidedFields.size(); ++i) {
        newCoalesceList.add(
            context.rexBuilder.coalesce(
                duplicatedSourceFields.get(i), duplicatedProvidedFields.get(i)));
      }

      // For APPEND strategy, it needs to replace duplicated provided-fields with the new
      // constructed coalesced fields.
      // Hence, we need to remove the duplicated provided-fields as well and adjust the expected
      // provided-field names since new added fields are appended to the end of the project list.
      toBeRemovedFields.addAll(duplicatedProvidedFields);
      List<String> newExpectedFieldNames =
          new ArrayList<>(
              expectedProvidedFieldNames.stream()
                  .filter(k -> !duplicatedFieldNamesMap.containsKey(k))
                  .toList());
      newExpectedFieldNames.addAll(duplicatedFieldNamesMap.keySet());
      expectedProvidedFieldNames = newExpectedFieldNames;
    }

    // 5. Resolve join condition. Note, this operation should be done after finishing all analyze.
    JoinAndLookupUtils.addJoinForLookUp(node, context);

    // 6. Add projection for coalesce fields if there is.
    if (!newCoalesceList.isEmpty()) {
      context.relBuilder.projectPlus(newCoalesceList);
    }

    // 7. Add projection to remove unnecessary fields
    // NOTE: Need to lazy invoke projectExcept until finishing all analyzing,
    // otherwise the field names may have changed because of field name duplication.
    if (!toBeRemovedFields.isEmpty()) {
      context.relBuilder.projectExcept(toBeRemovedFields);
    }

    // 7. Rename the fields to the expected names.
    JoinAndLookupUtils.renameToExpectedFields(
        expectedProvidedFieldNames,
        sourceFieldsNames.size() - duplicatedSourceFields.size(),
        context);

    return context.relBuilder.peek();
  }

  /*
   * Unsupported Commands of PPL with Calcite for OpenSearch 3.0.0-beta
   */
  @Override
  public RelNode visitAD(AD node, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("AD command is unsupported in Calcite");
  }

  @Override
  public RelNode visitCloseCursor(CloseCursor closeCursor, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("Close cursor operation is unsupported in Calcite");
  }

  @Override
  public RelNode visitFetchCursor(FetchCursor cursor, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("Fetch cursor operation is unsupported in Calcite");
  }

  @Override
  public RelNode visitML(ML node, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("ML command is unsupported in Calcite");
  }

  @Override
  public RelNode visitPaginate(Paginate paginate, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("Paginate operation is unsupported in Calcite");
  }

  @Override
  public RelNode visitKmeans(Kmeans node, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("Kmeans command is unsupported in Calcite");
  }

  @Override
  public RelNode visitFillNull(FillNull fillNull, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("FillNull command is unsupported in Calcite");
  }

  @Override
  public RelNode visitParse(Parse node, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("Parse command is unsupported in Calcite");
  }

  @Override
  public RelNode visitRareTopN(RareTopN node, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("Rare and Top commands are unsupported in Calcite");
  }

  @Override
  public RelNode visitTableFunction(TableFunction node, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("Table function is unsupported in Calcite");
  }

  @Override
  public RelNode visitTrendline(Trendline node, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("Trendline command is unsupported in Calcite");
  }
}
