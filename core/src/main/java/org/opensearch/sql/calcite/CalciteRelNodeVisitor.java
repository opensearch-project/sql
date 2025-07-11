/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.apache.calcite.sql.SqlKind.AS;
import static org.opensearch.sql.ast.tree.Join.JoinType.ANTI;
import static org.opensearch.sql.ast.tree.Join.JoinType.SEMI;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_FIRST;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_LAST;
import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_DESC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.ASC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.DESC;
import static org.opensearch.sql.calcite.utils.PlanUtils.ROW_NUMBER_COLUMN_NAME;
import static org.opensearch.sql.calcite.utils.PlanUtils.ROW_NUMBER_COLUMN_NAME_MAIN;
import static org.opensearch.sql.calcite.utils.PlanUtils.ROW_NUMBER_COLUMN_NAME_SUBSEARCH;
import static org.opensearch.sql.calcite.utils.PlanUtils.getRelation;
import static org.opensearch.sql.calcite.utils.PlanUtils.transformPlanToAttachChild;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.util.Holder;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.AllFieldsExcludeMeta;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Argument.ArgumentMap;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.PatternMethod;
import org.opensearch.sql.ast.expression.PatternMode;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.WindowFrame;
import org.opensearch.sql.ast.expression.WindowFrame.FrameType;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.subquery.SubqueryExpression;
import org.opensearch.sql.ast.tree.AD;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.AppendCol;
import org.opensearch.sql.ast.tree.CloseCursor;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Expand;
import org.opensearch.sql.ast.tree.FetchCursor;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Flatten;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Kmeans;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Lookup.OutputStrategy;
import org.opensearch.sql.ast.tree.ML;
import org.opensearch.sql.ast.tree.Paginate;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Patterns;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Reverse;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.Trendline.TrendlineType;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Window;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
import org.opensearch.sql.calcite.utils.JoinAndLookupUtils;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.common.patterns.PatternUtils;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;
import org.opensearch.sql.utils.ParseUtils;

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
    if (node.getProjectList().size() == 1
        && node.getProjectList().getFirst() instanceof AllFields allFields) {
      tryToRemoveNestedFields(context);
      tryToRemoveMetaFields(context, allFields instanceof AllFieldsExcludeMeta);
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
      // Only set when not resolving subquery and it's not projectExcept.
      if (!context.isResolvingSubquery()) {
        context.setProjectVisited(true);
      }
      context.relBuilder.project(projectList);
    }
    return context.relBuilder.peek();
  }

  /** See logic in {@link org.opensearch.sql.analysis.symbol.SymbolTable#lookupAllFields} */
  private static void tryToRemoveNestedFields(CalcitePlanContext context) {
    Set<String> allFields = new HashSet<>(context.relBuilder.peek().getRowType().getFieldNames());
    List<RexNode> duplicatedNestedFields =
        allFields.stream()
            .filter(
                field -> {
                  int lastDot = field.lastIndexOf(".");
                  return -1 != lastDot && allFields.contains(field.substring(0, lastDot));
                })
            .map(field -> (RexNode) context.relBuilder.field(field))
            .toList();
    if (!duplicatedNestedFields.isEmpty()) {
      // This is a workaround to avoid the bug in Calcite:
      // In {@link RelBuilder#project_(Iterable, Iterable, Iterable, boolean, Iterable)},
      // the check `RexUtil.isIdentity(nodeList, inputRowType)` will pass when the input
      // and the output nodeList refer to the same fields, even if the field name list
      // is different. As a result, renaming operation will not be applied. This makes
      // the logical plan for the flatten command incorrect, where the operation is
      // equivalent to renaming the flattened sub-fields. E.g. emp.name -> name.
      forceProjectExcept(context.relBuilder, duplicatedNestedFields);
    }
  }

  /**
   * Project except with force.
   *
   * <p>This method is copied from {@link RelBuilder#projectExcept(Iterable)} and modified with the
   * force flag in project set to true. It is subject to future changes in Calcite.
   *
   * @param relBuilder RelBuilder
   * @param expressions Expressions to exclude from the project
   */
  private static void forceProjectExcept(RelBuilder relBuilder, Iterable<RexNode> expressions) {
    List<RexNode> allExpressions = new ArrayList<>(relBuilder.fields());
    Set<RexNode> excludeExpressions = new HashSet<>();
    for (RexNode excludeExp : expressions) {
      if (!excludeExpressions.add(excludeExp)) {
        throw new IllegalArgumentException(
            "Input list contains duplicates. Expression " + excludeExp + " exists multiple times.");
      }
      if (!allExpressions.remove(excludeExp)) {
        throw new IllegalArgumentException("Expression " + excludeExp.toString() + " not found.");
      }
    }
    relBuilder.project(allExpressions, ImmutableList.of(), true);
  }

  /**
   * Try to remove metadata fields in two cases:
   *
   * <p>1. It's explicitly specified excluding by force, usually for join or subquery.
   *
   * <p>2. There is no other project ever visited in the main query
   *
   * @param context CalcitePlanContext
   * @param excludeByForce whether exclude metadata fields by force
   */
  private static void tryToRemoveMetaFields(CalcitePlanContext context, boolean excludeByForce) {
    if (excludeByForce || !context.isProjectVisited()) {
      List<String> originalFields = context.relBuilder.peek().getRowType().getFieldNames();
      List<RexNode> metaFieldsRef =
          originalFields.stream()
              .filter(OpenSearchConstants.METADATAFIELD_TYPE_MAP::containsKey)
              .map(metaField -> (RexNode) context.relBuilder.field(metaField))
              .toList();
      // Remove metadata fields if there is and ensure there are other fields.
      if (!metaFieldsRef.isEmpty() && metaFieldsRef.size() != originalFields.size()) {
        context.relBuilder.projectExcept(metaFieldsRef);
      }
    }
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
                  // Default is ASC
                  if (sortOption.getSortOrder() == DESC) {
                    sortField = context.relBuilder.desc(sortField);
                  }
                  if (sortOption.getNullOrder() == NULL_LAST) {
                    sortField = context.relBuilder.nullsLast(sortField);
                  } else {
                    sortField = context.relBuilder.nullsFirst(sortField);
                  }
                  return sortField;
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
  public RelNode visitReverse(org.opensearch.sql.ast.tree.Reverse node, CalcitePlanContext context) {
    visitChildren(node, context);
    // Add ROW_NUMBER() column
    RexNode rowNumber = context.relBuilder.aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
        .over().rowsTo(RexWindowBounds.CURRENT_ROW).as("__reverse_row_num__");
    context.relBuilder.projectPlus(rowNumber);
    // Sort by row number descending
    context.relBuilder.sort(context.relBuilder.desc(context.relBuilder.field("__reverse_row_num__")));
    // Remove row number column
    context.relBuilder.projectExcept(context.relBuilder.field("__reverse_row_num__"));
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitParse(Parse node, CalcitePlanContext context) {
    visitChildren(node, context);
    buildParseRelNode(node, context);
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitPatterns(Patterns node, CalcitePlanContext context) {
    visitChildren(node, context);
    if (PatternMethod.SIMPLE_PATTERN.equals(node.getPatternMethod())) {
      Parse parseNode =
          new Parse(
              ParseMethod.PATTERNS,
              node.getSourceField(),
              node.getArguments().getOrDefault(PatternUtils.PATTERN, AstDSL.stringLiteral("")),
              node.getArguments());
      buildParseRelNode(parseNode, context);
      if (PatternMode.AGGREGATION.equals(node.getPatternMode())) {
        Field patternField = AstDSL.field(node.getAlias());
        List<AggCall> aggCalls =
            Stream.of(
                    new Alias(
                        PatternUtils.PATTERN_COUNT,
                        new AggregateFunction(BuiltinFunctionName.COUNT.name(), patternField)),
                    new Alias(
                        PatternUtils.SAMPLE_LOGS,
                        new AggregateFunction(
                            BuiltinFunctionName.TAKE.name(),
                            node.getSourceField(),
                            ImmutableList.of(node.getPatternMaxSampleCount()))))
                .map(aggFun -> aggVisitor.analyze(aggFun, context))
                .toList();
        List<RexNode> groupByList = new ArrayList<>();
        groupByList.add(rexVisitor.analyze(patternField, context));
        groupByList.addAll(
            node.getPartitionByList().stream()
                .map(expr -> rexVisitor.analyze(expr, context))
                .toList());
        context.relBuilder.aggregate(context.relBuilder.groupKey(groupByList), aggCalls);

        RexNode parsedNode =
            PPLFuncImpTable.INSTANCE.resolve(
                context.rexBuilder,
                BuiltinFunctionName.INTERNAL_PATTERN_PARSER,
                context.relBuilder.field(node.getAlias()),
                context.relBuilder.field(PatternUtils.SAMPLE_LOGS));
        flattenParsedPattern(node.getAlias(), parsedNode, context, false);
        context.relBuilder.projectExcept(context.relBuilder.field(PatternUtils.SAMPLE_LOGS));
      } else {
        RexNode parsedNode =
            PPLFuncImpTable.INSTANCE.resolve(
                context.rexBuilder,
                BuiltinFunctionName.INTERNAL_PATTERN_PARSER,
                context.relBuilder.field(node.getAlias()),
                rexVisitor.analyze(node.getSourceField(), context));
        flattenParsedPattern(node.getAlias(), parsedNode, context, false);
      }
    } else {
      List<UnresolvedExpression> funcParamList = new ArrayList<>();
      funcParamList.add(node.getSourceField());
      funcParamList.add(node.getPatternMaxSampleCount());
      funcParamList.add(node.getPatternBufferLimit());
      funcParamList.addAll(
          node.getArguments().entrySet().stream()
              .map(entry -> new Argument(entry.getKey(), entry.getValue()))
              .sorted(Comparator.comparing(Argument::getArgName))
              .toList());
      if (PatternMode.LABEL.equals(
          node.getPatternMode())) { // Label mode, resolve the plan as window function
        RexNode windowNode =
            rexVisitor.analyze(
                new WindowFunction(
                    new Function(
                        BuiltinFunctionName.INTERNAL_PATTERN.getName().getFunctionName(),
                        funcParamList),
                    node.getPartitionByList(),
                    List.of()),
                context);
        RexNode nestedNode =
            context.relBuilder.alias(
                PPLFuncImpTable.INSTANCE.resolve(
                    context.rexBuilder,
                    BuiltinFunctionName.INTERNAL_PATTERN_PARSER,
                    rexVisitor.analyze(node.getSourceField(), context),
                    windowNode),
                node.getAlias());
        context.relBuilder.projectPlus(nestedNode);
        flattenParsedPattern(
            node.getAlias(), context.relBuilder.field(node.getAlias()), context, false);
      } else { // Aggregation mode, resolve plan as aggregation
        AggCall aggCall =
            aggVisitor
                .analyze(
                    new Function(
                        BuiltinFunctionName.INTERNAL_PATTERN.getName().getFunctionName(),
                        funcParamList),
                    context)
                .as(node.getAlias());
        List<RexNode> groupByList =
            node.getPartitionByList().stream()
                .map(expr -> rexVisitor.analyze(expr, context))
                .toList();
        context.relBuilder.aggregate(context.relBuilder.groupKey(groupByList), aggCall);
        buildExpandRelNode(
            context.relBuilder.field(node.getAlias()), node.getAlias(), node.getAlias(), context);
        flattenParsedPattern(
            node.getAlias(), context.relBuilder.field(node.getAlias()), context, true);
      }
    }
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitEval(Eval node, CalcitePlanContext context) {
    visitChildren(node, context);
    List<String> originalFieldNames = context.relBuilder.peek().getRowType().getFieldNames();
    node.getExpressionList()
        .forEach(
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
                // Overriding the existing field if the alias has the same name with original field.
                String alias =
                    ((RexLiteral) ((RexCall) eval).getOperands().get(1)).getValueAs(String.class);
                projectPlusOverriding(List.of(eval), List.of(alias), context);
              }
            });
    return context.relBuilder.peek();
  }

  private void projectPlusOverriding(
      List<RexNode> newFields, List<String> newNames, CalcitePlanContext context) {
    List<String> originalFieldNames = context.relBuilder.peek().getRowType().getFieldNames();
    List<RexNode> toOverrideList =
        originalFieldNames.stream()
            .filter(newNames::contains)
            .map(a -> (RexNode) context.relBuilder.field(a))
            .toList();
    // 1. add the new fields, For example "age0, country0"
    context.relBuilder.projectPlus(newFields);
    // 2. drop the overriding field list, it's duplicated now. For example "age, country"
    if (!toOverrideList.isEmpty()) {
      context.relBuilder.projectExcept(toOverrideList);
    }
    // 3. get current fields list, the "age0, country0" should include in it.
    List<String> currentFields = context.relBuilder.peek().getRowType().getFieldNames();
    int length = currentFields.size();
    // 4. add new names "age, country" to the end of rename list.
    List<String> expectedRenameFields =
        new ArrayList<>(currentFields.subList(0, length - newNames.size()));
    expectedRenameFields.addAll(newNames);
    // 5. rename
    context.relBuilder.rename(expectedRenameFields);
  }

  /**
   * Resolve the aggregation with trimming unused fields to avoid bugs in {@link
   * org.apache.calcite.sql2rel.RelDecorrelator#decorrelateRel(Aggregate, boolean)}
   *
   * @param groupExprList group by expression list
   * @param aggExprList aggregate expression list
   * @param context CalcitePlanContext
   * @return Pair of (group-by list, field list, aggregate list)
   */
  private Pair<List<RexNode>, List<AggCall>> aggregateWithTrimming(
      List<UnresolvedExpression> groupExprList,
      List<UnresolvedExpression> aggExprList,
      CalcitePlanContext context) {
    // Example 1: source=t | where a > 1 | stats avg(b + 1) by c
    // Before: Aggregate(avg(b + 1))
    //         \- Filter(a > 1)
    //            \- Scan t
    // After: Aggregate(avg(b + 1))
    //        \- Project([c, b])
    //           \- Filter(a > 1)
    //              \- Scan t
    //
    // Example 2: source=t | where a > 1 | top b by c
    // Before: Aggregate(count)
    //         \-Filter(a > 1)
    //           \- Scan t
    // After: Aggregate(count)
    //        \- Project([c, b])
    //           \- Filter(a > 1)
    //              \- Scan t
    Pair<List<RexNode>, List<AggCall>> resolved =
        resolveAttributesForAggregation(groupExprList, aggExprList, context);
    List<RexInputRef> trimmedRefs = new ArrayList<>();
    trimmedRefs.addAll(PlanUtils.getInputRefs(resolved.getLeft())); // group-by keys first
    trimmedRefs.addAll(PlanUtils.getInputRefsFromAggCall(resolved.getRight()));
    context.relBuilder.project(trimmedRefs);

    // Re-resolve all attributes based on adding trimmed Project.
    // Using re-resolving rather than Calcite Mapping (ref Calcite ProjectTableScanRule)
    // because that Mapping only works for RexNode, but we need both AggCall and RexNode list.
    Pair<List<RexNode>, List<AggCall>> reResolved =
        resolveAttributesForAggregation(groupExprList, aggExprList, context);
    context.relBuilder.aggregate(
        context.relBuilder.groupKey(reResolved.getLeft()), reResolved.getRight());
    return Pair.of(reResolved.getLeft(), reResolved.getRight());
  }

  /**
   * Resolve attributes for aggregation.
   *
   * @param groupExprList group by expression list
   * @param aggExprList aggregate expression list
   * @param context CalcitePlanContext
   * @return Pair of (group-by list, aggregate list)
   */
  private Pair<List<RexNode>, List<AggCall>> resolveAttributesForAggregation(
      List<UnresolvedExpression> groupExprList,
      List<UnresolvedExpression> aggExprList,
      CalcitePlanContext context) {
    List<AggCall> aggCallList =
        aggExprList.stream().map(expr -> aggVisitor.analyze(expr, context)).toList();
    List<RexNode> groupByList =
        groupExprList.stream().map(expr -> rexVisitor.analyze(expr, context)).toList();
    return Pair.of(groupByList, aggCallList);
  }

  @Override
  public RelNode visitAggregation(Aggregation node, CalcitePlanContext context) {
    visitChildren(node, context);

    List<UnresolvedExpression> aggExprList = node.getAggExprList();
    List<UnresolvedExpression> groupExprList = new ArrayList<>();
    // The span column is always the first column in result whatever
    // the order of span in query is first or last one
    UnresolvedExpression span = node.getSpan();
    if (!Objects.isNull(span)) {
      groupExprList.add(span);
    }
    groupExprList.addAll(node.getGroupExprList());
    Pair<List<RexNode>, List<AggCall>> aggregationAttributes =
        aggregateWithTrimming(groupExprList, aggExprList, context);

    // schema reordering
    // As an example, in command `stats count() by colA, colB`,
    // the sequence of output schema is "count, colA, colB".
    List<RexNode> outputFields = context.relBuilder.fields();
    int numOfOutputFields = outputFields.size();
    int numOfAggList = aggExprList.size();
    List<RexNode> reordered = new ArrayList<>(numOfOutputFields);
    // Add aggregation results first
    List<RexNode> aggRexList =
        outputFields.subList(numOfOutputFields - numOfAggList, numOfOutputFields);
    reordered.addAll(aggRexList);
    // Add group by columns
    List<RexNode> aliasedGroupByList =
        aggregationAttributes.getLeft().stream()
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
    if (node.getJoinType() == SEMI || node.getJoinType() == ANTI) {
      // semi and anti join only return left table outputs
      context.relBuilder.join(
          JoinAndLookupUtils.translateJoinType(node.getJoinType()), joinCondition);
    } else {
      // Join condition could contain duplicated column name, Calcite will rename the duplicated
      // column name with numeric suffix, e.g. ON t1.id = t2.id, the output contains `id` and `id0`
      // when a new project add to stack. To avoid `id0`, we will rename the `id0` to `alias.id`
      // or `tableIdentifier.id`:
      List<String> leftColumns = context.relBuilder.peek(1).getRowType().getFieldNames();
      List<String> rightColumns = context.relBuilder.peek().getRowType().getFieldNames();
      List<String> rightTableName =
          PlanUtils.findTable(context.relBuilder.peek()).getQualifiedName();
      // Using `table.column` instead of `catalog.database.table.column` as column prefix because
      // the schema for OpenSearch index is always `OpenSearch`. But if we reuse this logic in other
      // query engines, the column can only be searched in current schema namespace. For example,
      // If the plan convert to Spark plan, and there are two table1: database1.table1 and
      // database2.table1. The query with column `table1.id` can only be resolved in the namespace
      // of "database1". User should run `using database1` before the query which access `table1.id`
      String rightTableQualifiedName = rightTableName.getLast();
      // new columns with alias or table;
      List<String> rightColumnsWithAliasIfConflict =
          rightColumns.stream()
              .map(
                  col ->
                      leftColumns.contains(col)
                          ? node.getRightAlias()
                              .map(a -> a + "." + col)
                              .orElse(rightTableQualifiedName + "." + col)
                          : col)
              .toList();
      context.relBuilder.join(
          JoinAndLookupUtils.translateJoinType(node.getJoinType()), joinCondition);
      JoinAndLookupUtils.renameToExpectedFields(
          rightColumnsWithAliasIfConflict, leftColumns.size(), context);
    }
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

  @Override
  public RelNode visitDedupe(Dedupe node, CalcitePlanContext context) {
    visitChildren(node, context);
    List<Argument> options = node.getOptions();
    Integer allowedDuplication = (Integer) options.get(0).getValue().getValue();
    Boolean keepEmpty = (Boolean) options.get(1).getValue().getValue();
    Boolean consecutive = (Boolean) options.get(2).getValue().getValue();
    if (allowedDuplication <= 0) {
      throw new IllegalArgumentException("Number of duplicate events must be greater than 0");
    }
    if (consecutive) {
      throw new UnsupportedOperationException("Consecutive deduplication is not supported");
    }
    // Columns to deduplicate
    List<RexNode> dedupeFields =
        node.getFields().stream().map(f -> rexVisitor.analyze(f, context)).toList();
    if (keepEmpty) {
      /*
       * | dedup 2 a, b keepempty=false
       * DropColumns('_row_number_)
       * +- Filter ('_row_number_ <= n OR isnull('a) OR isnull('b))
       *    +- Window [row_number() windowspecdefinition('a, 'b, 'a ASC NULLS FIRST, 'b ASC NULLS FIRST, specifiedwindowoundedpreceding$(), currentrow$())) AS _row_number_], ['a, 'b], ['a ASC NULLS FIRST, 'b ASC NULLS FIRST]
       *        +- ...
       */
      // Window [row_number() windowspecdefinition('a, 'b, 'a ASC NULLS FIRST, 'b ASC NULLS FIRST,
      // specifiedwindowoundedpreceding$(), currentrow$())) AS _row_number_], ['a, 'b], ['a ASC
      // NULLS FIRST, 'b ASC NULLS FIRST]
      RexNode rowNumber =
          context
              .relBuilder
              .aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
              .over()
              .partitionBy(dedupeFields)
              .orderBy(dedupeFields)
              .rowsTo(RexWindowBounds.CURRENT_ROW)
              .as("_row_number_");
      context.relBuilder.projectPlus(rowNumber);
      RexNode _row_number_ = context.relBuilder.field("_row_number_");
      // Filter (isnull('a) OR isnull('b) OR '_row_number_ <= n)
      context.relBuilder.filter(
          context.relBuilder.or(
              context.relBuilder.or(dedupeFields.stream().map(context.relBuilder::isNull).toList()),
              context.relBuilder.lessThanOrEqual(
                  _row_number_, context.relBuilder.literal(allowedDuplication))));
      // DropColumns('_row_number_)
      context.relBuilder.projectExcept(_row_number_);
    } else {
      /*
       * | dedup 2 a, b keepempty=false
       * DropColumns('_row_number_)
       * +- Filter ('_row_number_ <= n)
       *    +- Window [row_number() windowspecdefinition('a, 'b, 'a ASC NULLS FIRST, 'b ASC NULLS FIRST, specifiedwindowoundedpreceding$(), currentrow$())) AS _row_number_], ['a, 'b], ['a ASC NULLS FIRST, 'b ASC NULLS FIRST]
       *       +- Filter (isnotnull('a) AND isnotnull('b))
       *          +- ...
       */
      // Filter (isnotnull('a) AND isnotnull('b))
      context.relBuilder.filter(
          context.relBuilder.and(
              dedupeFields.stream().map(context.relBuilder::isNotNull).toList()));
      // Window [row_number() windowspecdefinition('a, 'b, 'a ASC NULLS FIRST, 'b ASC NULLS FIRST,
      // specifiedwindowoundedpreceding$(), currentrow$())) AS _row_number_], ['a, 'b], ['a ASC
      // NULLS FIRST, 'b ASC NULLS FIRST]
      RexNode rowNumber =
          context
              .relBuilder
              .aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
              .over()
              .partitionBy(dedupeFields)
              .orderBy(dedupeFields)
              .rowsTo(RexWindowBounds.CURRENT_ROW)
              .as("_row_number_");
      context.relBuilder.projectPlus(rowNumber);
      RexNode _row_number_ = context.relBuilder.field("_row_number_");
      // Filter ('_row_number_ <= n)
      context.relBuilder.filter(
          context.relBuilder.lessThanOrEqual(
              _row_number_, context.relBuilder.literal(allowedDuplication)));
      // DropColumns('_row_number_)
      context.relBuilder.projectExcept(_row_number_);
    }
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitWindow(Window node, CalcitePlanContext context) {
    visitChildren(node, context);
    List<RexNode> overExpressions =
        node.getWindowFunctionList().stream().map(w -> rexVisitor.analyze(w, context)).toList();
    context.relBuilder.projectPlus(overExpressions);
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitFillNull(FillNull node, CalcitePlanContext context) {
    visitChildren(node, context);
    if (node.getFields().size()
        != new HashSet<>(node.getFields().stream().map(f -> f.getField().toString()).toList())
            .size()) {
      throw new IllegalArgumentException("The field list cannot be duplicated in fillnull");
    }
    List<RexNode> projects = new ArrayList<>();
    List<RelDataTypeField> fieldsList = context.relBuilder.peek().getRowType().getFieldList();
    for (RelDataTypeField field : fieldsList) {
      RexNode fieldRef = context.rexBuilder.makeInputRef(field.getType(), field.getIndex());
      boolean toReplace = false;
      for (Pair<Field, UnresolvedExpression> pair : node.getReplacementPairs()) {
        if (field.getName().equalsIgnoreCase(pair.getLeft().getField().toString())) {
          RexNode replacement = rexVisitor.analyze(pair.getRight(), context);
          RexNode coalesce = context.rexBuilder.coalesce(fieldRef, replacement);
          RexNode coalesceWithAlias = context.relBuilder.alias(coalesce, field.getName());
          projects.add(coalesceWithAlias);
          toReplace = true;
          break;
        }
      }
      if (!toReplace && node.getReplacementForAll().isEmpty()) {
        projects.add(fieldRef);
      } else if (node.getReplacementForAll().isPresent()) {
        RexNode replacement = rexVisitor.analyze(node.getReplacementForAll().get(), context);
        RexNode coalesce = context.rexBuilder.coalesce(fieldRef, replacement);
        RexNode coalesceWithAlias = context.relBuilder.alias(coalesce, field.getName());
        projects.add(coalesceWithAlias);
      }
    }
    context.relBuilder.project(projects);
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitAppendCol(AppendCol node, CalcitePlanContext context) {
    // 1. resolve main plan
    visitChildren(node, context);
    // 2. add row_number() column to main
    RexNode mainRowNumber =
        PlanUtils.makeOver(
            context,
            BuiltinFunctionName.ROW_NUMBER,
            null,
            List.of(),
            List.of(),
            List.of(),
            WindowFrame.toCurrentRow());
    context.relBuilder.projectPlus(
        context.relBuilder.alias(mainRowNumber, ROW_NUMBER_COLUMN_NAME_MAIN));

    // 3. build subsearch tree (attach relation to subsearch)
    UnresolvedPlan relation = getRelation(node);
    transformPlanToAttachChild(node.getSubSearch(), relation);
    // 4. resolve subsearch plan
    node.getSubSearch().accept(this, context);
    // 5. add row_number() column to subsearch
    RexNode subsearchRowNumber =
        PlanUtils.makeOver(
            context,
            BuiltinFunctionName.ROW_NUMBER,
            null,
            List.of(),
            List.of(),
            List.of(),
            WindowFrame.toCurrentRow());
    context.relBuilder.projectPlus(
        context.relBuilder.alias(subsearchRowNumber, ROW_NUMBER_COLUMN_NAME_SUBSEARCH));

    List<String> subsearchFields = context.relBuilder.peek().getRowType().getFieldNames();
    List<String> mainFields = context.relBuilder.peek(1).getRowType().getFieldNames();
    if (!node.isOverride()) {
      // 6. if override = false, drop all the duplicated columns in subsearch before join
      List<String> subsearchProjectList =
          subsearchFields.stream().filter(r -> !mainFields.contains(r)).toList();
      context.relBuilder.project(context.relBuilder.fields(subsearchProjectList));
    }

    // 7. join with condition `_row_number_main_ = _row_number_subsearch_`
    RexNode joinCondition =
        context.relBuilder.equals(
            context.relBuilder.field(2, 0, ROW_NUMBER_COLUMN_NAME_MAIN),
            context.relBuilder.field(2, 1, ROW_NUMBER_COLUMN_NAME_SUBSEARCH));
    context.relBuilder.join(
        JoinAndLookupUtils.translateJoinType(Join.JoinType.FULL), joinCondition);

    if (!node.isOverride()) {
      // 8. if override = false, drop both _row_number_ columns
      context.relBuilder.projectExcept(
          List.of(
              context.relBuilder.field(ROW_NUMBER_COLUMN_NAME_MAIN),
              context.relBuilder.field(ROW_NUMBER_COLUMN_NAME_SUBSEARCH)));
      return context.relBuilder.peek();
    } else {
      // 9. if override = true, override the duplicated columns in main by subsearch values
      // when join condition matched.
      List<RexNode> finalProjections = new ArrayList<>();
      List<String> finalFieldNames = new ArrayList<>();
      int mainFieldCount = mainFields.size();
      Set<String> duplicatedFields =
          mainFields.stream().filter(subsearchFields::contains).collect(Collectors.toSet());
      RexNode caseCondition =
          context.relBuilder.equals(
              context.relBuilder.field(ROW_NUMBER_COLUMN_NAME_MAIN),
              context.relBuilder.field(ROW_NUMBER_COLUMN_NAME_SUBSEARCH));
      for (int mainFieldIndex = 0; mainFieldIndex < mainFields.size(); mainFieldIndex++) {
        String mainFieldName = mainFields.get(mainFieldIndex);
        if (mainFieldName.equals(ROW_NUMBER_COLUMN_NAME_MAIN)) {
          continue;
        }
        finalFieldNames.add(mainFieldName);
        if (duplicatedFields.contains(mainFieldName)) {
          int subsearchFieldIndex = mainFieldCount + subsearchFields.indexOf(mainFieldName);
          // build case("_row_number_main_" = "_row_number_subsearch_", subsearchField, mainField)
          // using subsearch value when join condition matched, otherwise main value
          RexNode caseExpr =
              context.relBuilder.call(
                  SqlStdOperatorTable.CASE,
                  caseCondition,
                  context.relBuilder.field(subsearchFieldIndex),
                  context.relBuilder.field(mainFieldIndex));
          finalProjections.add(caseExpr);
        } else {
          // keep main fields for non duplicated fields
          finalProjections.add(context.relBuilder.field(mainFieldIndex));
        }
      }
      // add non duplicated fields of subsearch
      for (int subsearchFieldIndex = 0;
          subsearchFieldIndex < subsearchFields.size();
          subsearchFieldIndex++) {
        String subsearchFieldName = subsearchFields.get(subsearchFieldIndex);
        if (subsearchFieldName.equals(ROW_NUMBER_COLUMN_NAME_SUBSEARCH)) {
          continue;
        }
        if (!duplicatedFields.contains(subsearchFieldName)) {
          finalProjections.add(context.relBuilder.field(mainFieldCount + subsearchFieldIndex));
          finalFieldNames.add(subsearchFieldName);
        }
      }
      context.relBuilder.project(finalProjections, finalFieldNames);
      return context.relBuilder.peek();
    }
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
  public RelNode visitRareTopN(RareTopN node, CalcitePlanContext context) {
    visitChildren(node, context);

    ArgumentMap arguments = ArgumentMap.of(node.getArguments());
    String countFieldName = (String) arguments.get("countField").getValue();
    if (context.relBuilder.peek().getRowType().getFieldNames().contains(countFieldName)) {
      throw new IllegalArgumentException(
          "Field `"
              + countFieldName
              + "` is existed, change the count field by setting countfield='xyz'");
    }

    // 1. group the group-by list + field list and add a count() aggregation
    List<UnresolvedExpression> groupExprList = new ArrayList<>(node.getGroupExprList());
    List<UnresolvedExpression> fieldList =
        node.getFields().stream().map(f -> (UnresolvedExpression) f).toList();
    groupExprList.addAll(fieldList);
    List<UnresolvedExpression> aggExprList =
        List.of(AstDSL.alias(countFieldName, AstDSL.aggregate("count", null)));
    aggregateWithTrimming(groupExprList, aggExprList, context);

    // 2. add a window column
    List<RexNode> partitionKeys = rexVisitor.analyze(node.getGroupExprList(), context);
    RexNode countField;
    if (node.getCommandType() == RareTopN.CommandType.TOP) {
      countField = context.relBuilder.desc(context.relBuilder.field(countFieldName));
    } else {
      countField = context.relBuilder.field(countFieldName);
    }
    RexNode rowNumberWindowOver =
        PlanUtils.makeOver(
            context,
            BuiltinFunctionName.ROW_NUMBER,
            null,
            List.of(),
            partitionKeys,
            List.of(countField),
            WindowFrame.toCurrentRow());
    context.relBuilder.projectPlus(
        context.relBuilder.alias(rowNumberWindowOver, ROW_NUMBER_COLUMN_NAME));

    // 3. filter row_number() <= k in each partition
    Integer N = (Integer) arguments.get("noOfResults").getValue();
    context.relBuilder.filter(
        context.relBuilder.lessThanOrEqual(
            context.relBuilder.field(ROW_NUMBER_COLUMN_NAME), context.relBuilder.literal(N)));

    // 4. project final output. the default output is group by list + field list
    Boolean showCount = (Boolean) arguments.get("showCount").getValue();
    if (showCount) {
      context.relBuilder.projectExcept(context.relBuilder.field(ROW_NUMBER_COLUMN_NAME));
    } else {
      context.relBuilder.projectExcept(
          context.relBuilder.field(ROW_NUMBER_COLUMN_NAME),
          context.relBuilder.field(countFieldName));
    }
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitTableFunction(TableFunction node, CalcitePlanContext context) {
    throw new CalciteUnsupportedException("Table function is unsupported in Calcite");
  }

  /**
   * Visit flatten command.
   *
   * <p>The flatten command is used to flatten a struct field into multiple fields. This
   * implementation simply projects the flattened fields and renames them according to the provided
   * aliases or the field names in the struct. This is possible because the struct / object field
   * are always read in a flattened manner in OpenSearch.
   *
   * @param node Flatten command node
   * @param context CalcitePlanContext
   * @return RelNode representing the visited logical plan
   */
  @Override
  public RelNode visitFlatten(Flatten node, CalcitePlanContext context) {
    visitChildren(node, context);
    RelBuilder relBuilder = context.relBuilder;
    String fieldName = node.getField().getField().toString();
    // Match the sub-field names with "field.*"
    List<RelDataTypeField> fieldsToExpand =
        relBuilder.peek().getRowType().getFieldList().stream()
            .filter(f -> f.getName().startsWith(fieldName + "."))
            .toList();

    List<String> expandedFieldNames;
    if (node.getAliases() != null) {
      if (node.getAliases().size() != fieldsToExpand.size()) {
        throw new IllegalArgumentException(
            String.format(
                "The number of aliases has to match the number of flattened fields. Expected %d"
                    + " (%s), got %d (%s)",
                fieldsToExpand.size(),
                fieldsToExpand.stream()
                    .map(RelDataTypeField::getName)
                    .collect(Collectors.joining(", ")),
                node.getAliases().size(),
                String.join(", ", node.getAliases())));
      }
      expandedFieldNames = node.getAliases();
    } else {
      // If no aliases provided, name the flattened fields to the key name in the struct.
      // E.g. message.author --renamed-to--> author
      expandedFieldNames =
          fieldsToExpand.stream()
              .map(RelDataTypeField::getName)
              .map(name -> name.substring(fieldName.length() + 1))
              .collect(Collectors.toList());
    }
    List<RexNode> expandedFields =
        Streams.zip(
                fieldsToExpand.stream(),
                expandedFieldNames.stream(),
                (f, n) -> relBuilder.alias(relBuilder.field(f.getName()), n))
            .collect(Collectors.toList());
    relBuilder.projectPlus(expandedFields);
    return relBuilder.peek();
  }

  @Override
  public RelNode visitTrendline(Trendline node, CalcitePlanContext context) {
    visitChildren(node, context);

    node.getSortByField()
        .ifPresent(
            sortField -> {
              SortOption sortOption = analyzeSortOption(sortField.getFieldArgs());
              RexNode field = rexVisitor.analyze(sortField, context);
              if (sortOption == DEFAULT_DESC) {
                context.relBuilder.sort(context.relBuilder.desc(field));
              } else {
                context.relBuilder.sort(field);
              }
            });

    List<RexNode> trendlineNodes = new ArrayList<>();
    List<String> aliases = new ArrayList<>();
    node.getComputations()
        .forEach(
            trendlineComputation -> {
              RexNode field = rexVisitor.analyze(trendlineComputation.getDataField(), context);
              context.relBuilder.filter(context.relBuilder.isNotNull(field));

              WindowFrame windowFrame =
                  WindowFrame.of(
                      FrameType.ROWS,
                      StringUtils.format(
                          "%d PRECEDING", trendlineComputation.getNumberOfDataPoints() - 1),
                      "CURRENT ROW");
              RexNode countExpr =
                  PlanUtils.makeOver(
                      context,
                      BuiltinFunctionName.COUNT,
                      null,
                      List.of(),
                      List.of(),
                      List.of(),
                      windowFrame);
              // CASE WHEN count() over (ROWS (windowSize-1) PRECEDING) > windowSize - 1
              RexNode whenConditionExpr =
                  PPLFuncImpTable.INSTANCE.resolve(
                      context.rexBuilder,
                      ">",
                      countExpr,
                      context.relBuilder.literal(trendlineComputation.getNumberOfDataPoints() - 1));

              RexNode thenExpr;
              switch (trendlineComputation.getComputationType()) {
                case TrendlineType.SMA:
                  // THEN avg(field) over (ROWS (windowSize-1) PRECEDING)
                  thenExpr =
                      PlanUtils.makeOver(
                          context,
                          BuiltinFunctionName.AVG,
                          field,
                          List.of(),
                          List.of(),
                          List.of(),
                          windowFrame);
                  break;
                case TrendlineType.WMA:
                  // THEN wma expression
                  thenExpr =
                      buildWmaRexNode(
                          field,
                          trendlineComputation.getNumberOfDataPoints(),
                          windowFrame,
                          context);
                  break;
                default:
                  throw new IllegalStateException("Unsupported trendline type");
              }

              // ELSE NULL
              RexNode elseExpr = context.relBuilder.literal(null);

              List<RexNode> caseOperands = new ArrayList<>();
              caseOperands.add(whenConditionExpr);
              caseOperands.add(thenExpr);
              caseOperands.add(elseExpr);
              RexNode trendlineNode =
                  context.rexBuilder.makeCall(SqlStdOperatorTable.CASE, caseOperands);
              trendlineNodes.add(trendlineNode);
              aliases.add(trendlineComputation.getAlias());
            });

    projectPlusOverriding(trendlineNodes, aliases, context);
    return context.relBuilder.peek();
  }

  private RexNode buildWmaRexNode(
      RexNode field,
      Integer numberOfDataPoints,
      WindowFrame windowFrame,
      CalcitePlanContext context) {

    // Divisor: 1 + 2 + 3 + ... + windowSize, aka (windowSize * (windowSize + 1) / 2)
    RexNode divisor = context.relBuilder.literal(numberOfDataPoints * (numberOfDataPoints + 1) / 2);

    // Divider: 1 * NTH_VALUE(field, 1) + 2 * NTH_VALUE(field, 2) + ... + windowSize *
    // NTH_VALUE(field, windowSize)
    RexNode divider = context.relBuilder.literal(0);
    for (int i = 1; i <= numberOfDataPoints; i++) {
      RexNode nthValueExpr =
          PlanUtils.makeOver(
              context,
              BuiltinFunctionName.NTH_VALUE,
              field,
              List.of(context.relBuilder.literal(i)),
              List.of(),
              List.of(),
              windowFrame);
      divider =
          context.relBuilder.call(
              SqlStdOperatorTable.PLUS,
              divider,
              context.relBuilder.call(
                  SqlStdOperatorTable.MULTIPLY, nthValueExpr, context.relBuilder.literal(i)));
    }
    // Divider / CAST(Divisor, DOUBLE)
    return context.relBuilder.call(
        SqlStdOperatorTable.DIVIDE, divider, context.relBuilder.cast(divisor, SqlTypeName.DOUBLE));
  }

  /**
   * Expand command visitor to handle array field expansion. 1. Unnest 2. Join with the original
   * table to get all fields
   *
   * <p>S = π_{field, other_fields}(R ⨝ UNNEST_field(R))
   *
   * @param expand Expand command to be visited
   * @param context CalcitePlanContext containing the RelBuilder and other context
   * @return RelNode representing records with the expanded array field
   */
  @Override
  public RelNode visitExpand(Expand expand, CalcitePlanContext context) {
    // 1. Visit Children
    visitChildren(expand, context);

    // 2. Get the field to expand and an optional alias.
    Field arrayField = expand.getField();
    RexInputRef arrayFieldRex = (RexInputRef) rexVisitor.analyze(arrayField, context);
    String alias = expand.getAlias();

    buildExpandRelNode(arrayFieldRex, arrayField.getField().toString(), alias, context);

    return context.relBuilder.peek();
  }

  private void buildParseRelNode(Parse node, CalcitePlanContext context) {
    RexNode sourceField = rexVisitor.analyze(node.getSourceField(), context);
    ParseMethod parseMethod = node.getParseMethod();
    java.util.Map<String, Literal> arguments = node.getArguments();
    String patternValue = (String) node.getPattern().getValue();
    String pattern =
        ParseMethod.PATTERNS.equals(parseMethod) && Strings.isNullOrEmpty(patternValue)
            ? "[a-zA-Z0-9]+"
            : patternValue;
    List<String> groupCandidates =
        ParseUtils.getNamedGroupCandidates(parseMethod, pattern, arguments);
    RexNode[] rexNodeList =
        new RexNode[] {
          sourceField,
          context.rexBuilder.makeLiteral(
              pattern, context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), true)
        };
    if (ParseMethod.PATTERNS.equals(parseMethod)) {
      rexNodeList = ArrayUtils.add(rexNodeList, context.relBuilder.literal("<*>"));
    }
    List<RexNode> newFields = new ArrayList<>();
    for (String groupCandidate : groupCandidates) {
      RexNode innerRex =
          PPLFuncImpTable.INSTANCE.resolve(
              context.rexBuilder, ParseUtils.BUILTIN_FUNCTION_MAP.get(parseMethod), rexNodeList);
      if (ParseMethod.GROK.equals(parseMethod)) {
        newFields.add(
            PPLFuncImpTable.INSTANCE.resolve(
                context.rexBuilder,
                BuiltinFunctionName.INTERNAL_ITEM,
                innerRex,
                context.relBuilder.literal(groupCandidate)));
      } else {
        newFields.add(innerRex);
      }
    }
    projectPlusOverriding(newFields, groupCandidates, context);
  }

  private void flattenParsedPattern(
      String originalPatternResultAlias,
      RexNode parsedNode,
      CalcitePlanContext context,
      boolean flattenPatternCount) {
    List<RexNode> fattenedNodes = new ArrayList<>();
    List<String> projectNames = new ArrayList<>();
    // Flatten map struct fields
    RexNode patternExpr =
        context.rexBuilder.makeCast(
            context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
            PPLFuncImpTable.INSTANCE.resolve(
                context.rexBuilder,
                BuiltinFunctionName.INTERNAL_ITEM,
                parsedNode,
                context.rexBuilder.makeLiteral(PatternUtils.PATTERN)),
            true,
            true);
    fattenedNodes.add(context.relBuilder.alias(patternExpr, originalPatternResultAlias));
    projectNames.add(originalPatternResultAlias);
    if (flattenPatternCount) {
      RexNode patternCountExpr =
          context.rexBuilder.makeCast(
              context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
              PPLFuncImpTable.INSTANCE.resolve(
                  context.rexBuilder,
                  BuiltinFunctionName.INTERNAL_ITEM,
                  parsedNode,
                  context.rexBuilder.makeLiteral(PatternUtils.PATTERN_COUNT)),
              true,
              true);
      fattenedNodes.add(context.relBuilder.alias(patternCountExpr, PatternUtils.PATTERN_COUNT));
      projectNames.add(PatternUtils.PATTERN_COUNT);
    }
    RexNode tokensExpr =
        context.rexBuilder.makeCast(
            UserDefinedFunctionUtils.tokensMap,
            PPLFuncImpTable.INSTANCE.resolve(
                context.rexBuilder,
                BuiltinFunctionName.INTERNAL_ITEM,
                parsedNode,
                context.rexBuilder.makeLiteral(PatternUtils.TOKENS)),
            true,
            true);
    fattenedNodes.add(context.relBuilder.alias(tokensExpr, PatternUtils.TOKENS));
    projectNames.add(PatternUtils.TOKENS);
    projectPlusOverriding(fattenedNodes, projectNames, context);
  }

  private void buildExpandRelNode(
      RexInputRef arrayFieldRex, String arrayFieldName, String alias, CalcitePlanContext context) {
    // 3. Capture the outer row in a CorrelationId
    Holder<RexCorrelVariable> correlVariable = Holder.empty();
    context.relBuilder.variable(correlVariable::set);

    // 4. Create RexFieldAccess to access left node's array field with correlationId and build join
    // left node
    RexNode correlArrayFieldAccess =
        context.relBuilder.field(
            context.rexBuilder.makeCorrel(
                context.relBuilder.peek().getRowType(), correlVariable.get().id),
            arrayFieldRex.getIndex());
    RelNode leftNode = context.relBuilder.build();

    // 5. Build join right node and expand the array field using uncollect
    RelNode rightNode =
        context
            .relBuilder
            // fake input, see convertUnnest and convertExpression in Calcite SqlToRelConverter
            .push(LogicalValues.createOneRow(context.relBuilder.getCluster()))
            .project(List.of(correlArrayFieldAccess), List.of(arrayFieldName))
            .uncollect(List.of(), false)
            .build();

    // 6. Perform a nested-loop join (correlate) between the original table and the expanded
    // array field.
    // The last parameter has to refer to the array to be expanded on the left side. It will
    // be used by the right side to correlate with the left side.
    context
        .relBuilder
        .push(leftNode)
        .push(rightNode)
        .correlate(JoinRelType.INNER, correlVariable.get().id, List.of(arrayFieldRex))
        // 7. Remove the original array field from the output.
        // TODO: RFC: should we keep the original array field when alias is present?
        .projectExcept(arrayFieldRex);

    if (alias != null) {
      // Sub-nested fields cannot be removed after renaming the nested field.
      tryToRemoveNestedFields(context);
      RexInputRef expandedField = context.relBuilder.field(arrayFieldName);
      List<String> names = new ArrayList<>(context.relBuilder.peek().getRowType().getFieldNames());
      names.set(expandedField.getIndex(), alias);
      context.relBuilder.rename(names);
    }
  }
}
