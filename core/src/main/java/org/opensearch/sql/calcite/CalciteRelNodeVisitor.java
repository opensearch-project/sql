/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.apache.calcite.sql.SqlKind.AS;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.INFORMATION_SCHEMA_NAME;
import static org.opensearch.sql.ast.tree.Join.JoinType.ANTI;
import static org.opensearch.sql.ast.tree.Join.JoinType.SEMI;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_FIRST;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_LAST;
import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_DESC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.ASC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.DESC;
import static org.opensearch.sql.calcite.plan.DynamicFieldsConstants.DYNAMIC_FIELDS_MAP;
import static org.opensearch.sql.calcite.plan.rule.PPLDedupConvertRule.buildDedupNotNull;
import static org.opensearch.sql.calcite.plan.rule.PPLDedupConvertRule.buildDedupOrNull;
import static org.opensearch.sql.calcite.utils.PlanUtils.ROW_NUMBER_COLUMN_FOR_MAIN;
import static org.opensearch.sql.calcite.utils.PlanUtils.ROW_NUMBER_COLUMN_FOR_RARE_TOP;
import static org.opensearch.sql.calcite.utils.PlanUtils.ROW_NUMBER_COLUMN_FOR_STREAMSTATS;
import static org.opensearch.sql.calcite.utils.PlanUtils.ROW_NUMBER_COLUMN_FOR_SUBSEARCH;
import static org.opensearch.sql.calcite.utils.PlanUtils.getRelation;
import static org.opensearch.sql.calcite.utils.PlanUtils.getRexCall;
import static org.opensearch.sql.calcite.utils.PlanUtils.transformPlanToAttachChild;
import static org.opensearch.sql.utils.SystemIndexUtils.DATASOURCES_TABLE_NAME;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.util.Holder;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.AstNodeUtils;
import org.opensearch.sql.ast.EmptySourcePropagateVisitor;
import org.opensearch.sql.ast.analysis.FieldResolutionResult;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.AllFieldsExcludeMeta;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Argument.ArgumentMap;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.PatternMethod;
import org.opensearch.sql.ast.expression.PatternMode;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.WindowFrame;
import org.opensearch.sql.ast.expression.WindowFrame.FrameType;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.tree.AD;
import org.opensearch.sql.ast.tree.AddColTotals;
import org.opensearch.sql.ast.tree.AddTotals;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Append;
import org.opensearch.sql.ast.tree.AppendCol;
import org.opensearch.sql.ast.tree.AppendPipe;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.ast.tree.Chart;
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
import org.opensearch.sql.ast.tree.Multisearch;
import org.opensearch.sql.ast.tree.Paginate;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Patterns;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Regex;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Replace;
import org.opensearch.sql.ast.tree.ReplacePair;
import org.opensearch.sql.ast.tree.Rex;
import org.opensearch.sql.ast.tree.SPath;
import org.opensearch.sql.ast.tree.Search;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.ast.tree.StreamWindow;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.Trendline.TrendlineType;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Values;
import org.opensearch.sql.ast.tree.Window;
import org.opensearch.sql.calcite.plan.AliasFieldsWrappable;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.SystemLimitType;
import org.opensearch.sql.calcite.utils.BinUtils;
import org.opensearch.sql.calcite.utils.JoinAndLookupUtils;
import org.opensearch.sql.calcite.utils.JoinAndLookupUtils.OverwriteMode;
import org.opensearch.sql.calcite.utils.PPLHintUtils;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.WildcardUtils;
import org.opensearch.sql.common.patterns.PatternUtils;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;
import org.opensearch.sql.expression.parse.RegexCommonUtils;
import org.opensearch.sql.utils.ParseUtils;
import org.opensearch.sql.utils.WildcardRenameUtils;

public class CalciteRelNodeVisitor extends AbstractNodeVisitor<RelNode, CalcitePlanContext> {

  private final CalciteRexNodeVisitor rexVisitor;
  private final CalciteAggCallVisitor aggVisitor;
  private final DataSourceService dataSourceService;

  public CalciteRelNodeVisitor(DataSourceService dataSourceService) {
    this.rexVisitor = new CalciteRexNodeVisitor(this);
    this.aggVisitor = new CalciteAggCallVisitor(rexVisitor);
    this.dataSourceService = dataSourceService;
  }

  public RelNode analyze(UnresolvedPlan unresolved, CalcitePlanContext context) {
    return unresolved.accept(this, context);
  }

  @Override
  public RelNode visitRelation(Relation node, CalcitePlanContext context) {
    DataSourceSchemaIdentifierNameResolver nameResolver =
        new DataSourceSchemaIdentifierNameResolver(
            dataSourceService, node.getTableQualifiedName().getParts());
    if (!nameResolver
        .getDataSourceName()
        .equals(DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME)) {
      throw new CalciteUnsupportedException(
          "Datasource " + nameResolver.getDataSourceName() + " is unsupported in Calcite");
    }
    if (nameResolver.getIdentifierName().equals(DATASOURCES_TABLE_NAME)) {
      throw new CalciteUnsupportedException("SHOW DATASOURCES is unsupported in Calcite");
    }
    if (nameResolver.getSchemaName().equals(INFORMATION_SCHEMA_NAME)) {
      throw new CalciteUnsupportedException("information_schema is unsupported in Calcite");
    }
    context.relBuilder.scan(node.getTableQualifiedName().getParts());
    RelNode scan = context.relBuilder.peek();
    if (scan instanceof AliasFieldsWrappable) {
      return ((AliasFieldsWrappable) scan).wrapProjectForAliasFields(context.relBuilder);
    }
    return scan;
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
  public RelNode visitSearch(Search node, CalcitePlanContext context) {
    // Visit the Relation child to get the scan
    node.getChild().get(0).accept(this, context);
    // Create query_string function
    Function queryStringFunc =
        AstDSL.function(
            "query_string",
            AstDSL.unresolvedArg("query", AstDSL.stringLiteral(node.getQueryString())));
    RexNode queryStringRex = rexVisitor.analyze(queryStringFunc, context);

    context.relBuilder.filter(queryStringRex);
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitFilter(Filter node, CalcitePlanContext context) {
    visitChildren(node, context);
    boolean containsSubqueryExpression =
        AstNodeUtils.containsSubqueryExpression(node.getCondition());
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

  @Override
  public RelNode visitAppendPipe(AppendPipe node, CalcitePlanContext context) {
    visitChildren(node, context);
    UnresolvedPlan subqueryPlan = node.getSubQuery();
    UnresolvedPlan childNode = subqueryPlan;
    while (childNode.getChild() != null
        && !childNode.getChild().isEmpty()
        && !(childNode.getChild().getFirst() instanceof Values)) {
      if (childNode.getChild().size() > 1) {
        throw new RuntimeException("AppendPipe doesn't support multiply children subquery.");
      }
      childNode = (UnresolvedPlan) childNode.getChild().getFirst();
    }
    childNode.attach(node.getChild().getFirst());

    subqueryPlan.accept(this, context);

    RelNode subPipelineNode = context.relBuilder.build();
    RelNode mainNode = context.relBuilder.build();
    return mergeTableAndResolveColumnConflict(mainNode, subPipelineNode, context);
  }

  @Override
  public RelNode visitRegex(Regex node, CalcitePlanContext context) {
    visitChildren(node, context);

    RexNode fieldRex = rexVisitor.analyze(node.getField(), context);
    RexNode patternRex = rexVisitor.analyze(node.getPattern(), context);

    if (!SqlTypeFamily.CHARACTER.contains(fieldRex.getType())) {
      throw new IllegalArgumentException(
          String.format(
              "Regex command requires field of string type, but got %s for field '%s'",
              fieldRex.getType().getSqlTypeName(), node.getField().toString()));
    }

    RexNode regexCondition =
        context.rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_CONTAINS, fieldRex, patternRex);

    if (node.isNegated()) {
      regexCondition = context.rexBuilder.makeCall(SqlStdOperatorTable.NOT, regexCondition);
    }

    context.relBuilder.filter(regexCondition);
    return context.relBuilder.peek();
  }

  public RelNode visitRex(Rex node, CalcitePlanContext context) {
    visitChildren(node, context);

    RexNode fieldRex = rexVisitor.analyze(node.getField(), context);
    String patternStr = (String) node.getPattern().getValue();

    if (node.getMode() == Rex.RexMode.SED) {
      RexNode sedCall = createOptimizedSedCall(fieldRex, patternStr, context);
      String fieldName = node.getField().toString();
      projectPlusOverriding(List.of(sedCall), List.of(fieldName), context);
      return context.relBuilder.peek();
    }

    List<String> namedGroups = RegexCommonUtils.getNamedGroupCandidates(patternStr);

    if (namedGroups.isEmpty()) {
      throw new IllegalArgumentException(
          "Rex pattern must contain at least one named capture group");
    }

    // TODO: Once JDK 20+ is supported, consider using Pattern.namedGroups() API for more efficient
    // named group handling instead of manual parsing in RegexCommonUtils

    List<RexNode> newFields = new ArrayList<>();
    List<String> newFieldNames = new ArrayList<>();

    for (String groupName : namedGroups) {
      RexNode extractCall;
      if (node.getMaxMatch().isPresent() && node.getMaxMatch().get() > 1) {
        extractCall =
            PPLFuncImpTable.INSTANCE.resolve(
                context.rexBuilder,
                BuiltinFunctionName.REX_EXTRACT_MULTI,
                fieldRex,
                context.rexBuilder.makeLiteral(patternStr),
                context.rexBuilder.makeLiteral(groupName),
                context.relBuilder.literal(node.getMaxMatch().get()));
      } else {
        extractCall =
            PPLFuncImpTable.INSTANCE.resolve(
                context.rexBuilder,
                BuiltinFunctionName.REX_EXTRACT,
                fieldRex,
                context.rexBuilder.makeLiteral(patternStr),
                context.rexBuilder.makeLiteral(groupName));
      }
      newFields.add(extractCall);
      newFieldNames.add(groupName);
    }

    if (node.getOffsetField().isPresent()) {
      RexNode offsetCall =
          PPLFuncImpTable.INSTANCE.resolve(
              context.rexBuilder,
              BuiltinFunctionName.REX_OFFSET,
              fieldRex,
              context.rexBuilder.makeLiteral(patternStr));
      newFields.add(offsetCall);
      newFieldNames.add(node.getOffsetField().get());
    }

    projectPlusOverriding(newFields, newFieldNames, context);
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitProject(Project node, CalcitePlanContext context) {
    visitChildren(node, context);

    if (isSingleAllFieldsProject(node)) {
      return handleAllFieldsProject(node, context);
    }

    List<String> currentFields = context.relBuilder.peek().getRowType().getFieldNames();
    List<RexNode> expandedFields =
        expandProjectFields(node.getProjectList(), currentFields, context);

    if (node.isExcluded()) {
      validateExclusion(expandedFields, currentFields);
      context.relBuilder.projectExcept(expandedFields);
    } else {
      if (!context.isResolvingSubquery()) {
        context.setProjectVisited(true);
      }
      context.relBuilder.project(expandedFields);
    }
    return context.relBuilder.peek();
  }

  private boolean isSingleAllFieldsProject(Project node) {
    return node.getProjectList().size() == 1
        && node.getProjectList().getFirst() instanceof AllFields;
  }

  private RelNode handleAllFieldsProject(Project node, CalcitePlanContext context) {
    if (node.isExcluded()) {
      throw new IllegalArgumentException(
          "Invalid field exclusion: operation would exclude all fields from the result set");
    }
    AllFields allFields = (AllFields) node.getProjectList().getFirst();
    if (!(allFields instanceof AllFieldsExcludeMeta)) {
      // Should not remove nested fields for AllFieldsExcludeMeta.
      tryToRemoveNestedFields(context);
    }
    tryToRemoveMetaFields(context, allFields instanceof AllFieldsExcludeMeta);
    return context.relBuilder.peek();
  }

  private List<RexNode> expandProjectFields(
      List<UnresolvedExpression> projectList,
      List<String> currentFields,
      CalcitePlanContext context) {
    List<RexNode> expandedFields = new ArrayList<>();
    Set<String> addedFields = new HashSet<>();

    for (UnresolvedExpression expr : projectList) {
      switch (expr) {
        case Field field -> {
          String fieldName = field.getField().toString();
          if (WildcardUtils.containsWildcard(fieldName)) {
            List<String> matchingFields =
                WildcardUtils.expandWildcardPattern(fieldName, currentFields).stream()
                    .filter(f -> !isMetadataField(f) && !f.equals(DYNAMIC_FIELDS_MAP))
                    .filter(addedFields::add)
                    .toList();
            if (matchingFields.isEmpty()) {
              continue;
            }
            matchingFields.forEach(f -> expandedFields.add(context.relBuilder.field(f)));
          } else if (addedFields.add(fieldName)) {
            expandedFields.add(rexVisitor.analyze(field, context));
          }
        }
        case AllFields ignored -> {
          currentFields.stream()
              .filter(field -> !isMetadataField(field))
              .filter(addedFields::add)
              .forEach(field -> expandedFields.add(context.relBuilder.field(field)));
        }
        default ->
            throw new IllegalStateException(
                "Unexpected expression type in project list: " + expr.getClass().getSimpleName());
      }
    }

    if (expandedFields.isEmpty()) {
      validateWildcardPatterns(projectList, currentFields);
    }

    return expandedFields;
  }

  private void validateExclusion(List<RexNode> fieldsToExclude, List<String> currentFields) {
    Set<String> nonMetaFields =
        currentFields.stream().filter(field -> !isMetadataField(field)).collect(Collectors.toSet());

    if (fieldsToExclude.size() >= nonMetaFields.size()) {
      throw new IllegalArgumentException(
          "Invalid field exclusion: operation would exclude all fields from the result set");
    }
  }

  private void validateWildcardPatterns(
      List<UnresolvedExpression> projectList, List<String> currentFields) {
    String firstWildcardPattern =
        projectList.stream()
            .filter(
                expr ->
                    expr instanceof Field field
                        && WildcardUtils.containsWildcard(field.getField().toString()))
            .map(expr -> ((Field) expr).getField().toString())
            .findFirst()
            .orElse(null);

    if (firstWildcardPattern != null) {
      throw new IllegalArgumentException(
          String.format("wildcard pattern [%s] matches no fields", firstWildcardPattern));
    }
  }

  private static boolean isMetadataField(String fieldName) {
    return OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(fieldName);
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
      if (!(renameMap.getTarget() instanceof Field)) {
        throw new SemanticCheckException(
            String.format("the target expected to be field, but is %s", renameMap.getTarget()));
      }

      String sourcePattern = ((Field) renameMap.getOrigin()).getField().toString();
      String targetPattern = ((Field) renameMap.getTarget()).getField().toString();

      if (WildcardRenameUtils.isWildcardPattern(sourcePattern)
          && !WildcardRenameUtils.validatePatternCompatibility(sourcePattern, targetPattern)) {
        throw new SemanticCheckException(
            "Source and target patterns have different wildcard counts");
      }

      List<String> matchingFields = WildcardRenameUtils.matchFieldNames(sourcePattern, newNames);

      for (String fieldName : matchingFields) {
        String newName =
            WildcardRenameUtils.applyWildcardTransformation(
                sourcePattern, targetPattern, fieldName);
        if (newNames.contains(newName) && !newName.equals(fieldName)) {
          removeFieldIfExists(newName, newNames, context);
        }
        int fieldIndex = newNames.indexOf(fieldName);
        if (fieldIndex != -1) {
          newNames.set(fieldIndex, newName);
        }
      }

      if (matchingFields.isEmpty() && newNames.contains(targetPattern)) {
        removeFieldIfExists(targetPattern, newNames, context);
        context.relBuilder.rename(newNames);
      }
    }
    context.relBuilder.rename(newNames);
    return context.relBuilder.peek();
  }

  private void removeFieldIfExists(
      String fieldName, List<String> newNames, CalcitePlanContext context) {
    newNames.remove(fieldName);
    context.relBuilder.projectExcept(context.relBuilder.field(fieldName));
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
    // Apply count parameter as limit
    if (node.getCount() != 0) {
      context.relBuilder.limit(0, node.getCount());
    }

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

  private static final String REVERSE_ROW_NUM = "__reverse_row_num__";

  @Override
  public RelNode visitReverse(
      org.opensearch.sql.ast.tree.Reverse node, CalcitePlanContext context) {
    visitChildren(node, context);
    // Add ROW_NUMBER() column
    RexNode rowNumber =
        context
            .relBuilder
            .aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
            .over()
            .rowsTo(RexWindowBounds.CURRENT_ROW)
            .as(REVERSE_ROW_NUM);
    context.relBuilder.projectPlus(rowNumber);
    // Sort by row number descending
    context.relBuilder.sort(context.relBuilder.desc(context.relBuilder.field(REVERSE_ROW_NUM)));
    // Remove row number column
    context.relBuilder.projectExcept(context.relBuilder.field(REVERSE_ROW_NUM));
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitBin(Bin node, CalcitePlanContext context) {
    visitChildren(node, context);

    RexNode fieldExpr = rexVisitor.analyze(node.getField(), context);
    String fieldName = BinUtils.extractFieldName(node);

    RexNode binExpression = BinUtils.createBinExpression(node, fieldExpr, context, rexVisitor);

    String alias = node.getAlias() != null ? node.getAlias() : fieldName;
    projectPlusOverriding(List.of(binExpression), List.of(alias), context);

    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitParse(Parse node, CalcitePlanContext context) {
    visitChildren(node, context);
    buildParseRelNode(node, context);
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitSpath(SPath node, CalcitePlanContext context) {
    if (node.getPath() != null) {
      return visitEval(node.rewriteAsEval(), context);
    } else {
      return spathExtractAll(node, context);
    }
  }

  private RelNode spathExtractAll(SPath node, CalcitePlanContext context) {
    visitChildren(node, context);

    FieldResolutionResult resolutionResult = context.resolveFields(node);

    // 1. Extract all fields from JSON in `inField`
    RexNode inField = rexVisitor.analyze(AstDSL.field(node.getInField()), context);
    RexNode map = makeCall(context, BuiltinFunctionName.JSON_EXTRACT_ALL, inField);

    // 2. Project items from FieldResolutionResult
    Set<String> existingFields = new HashSet<>(getStaticFields(context));
    List<String> regularFieldNames =
        resolutionResult.getRegularFields().stream().sorted().collect(Collectors.toList());
    List<RexNode> fields = new ArrayList<>();
    for (String fieldName : regularFieldNames) {
      RexNode item = getItemAsString(map, fieldName, context);
      // Append if field already exist
      if (existingFields.contains(fieldName)) {
        item =
            makeCall(
                context,
                BuiltinFunctionName.INTERNAL_APPEND,
                context.relBuilder.field(fieldName),
                item);
        item = castToString(item, context);
      }
      fields.add(context.relBuilder.alias(item, fieldName));
    }

    // 3. Add _MAP field for dynamic fields when wildcards present
    if (resolutionResult.hasWildcards()) {
      RexNode dynamicMapField =
          createDynamicMapField(map, resolutionResult.getRegularFields(), context);
      List<String> remainingFields = getRemainingFields(existingFields, regularFieldNames);
      if (!remainingFields.isEmpty()) {
        // Add existing fields to map
        RexNode existingFieldsMap = getFieldsAsMap(existingFields, regularFieldNames, context);
        dynamicMapField =
            makeCall(context, BuiltinFunctionName.MAP_APPEND, existingFieldsMap, dynamicMapField);
      }
      if (isDynamicFieldsExists(context)) {
        RexNode existingMap = context.relBuilder.field(DYNAMIC_FIELDS_MAP);
        dynamicMapField =
            makeCall(context, BuiltinFunctionName.MAP_APPEND, existingMap, dynamicMapField);
      }
      fields.add(context.relBuilder.alias(dynamicMapField, DYNAMIC_FIELDS_MAP));
    }

    context.relBuilder.project(fields);
    return context.relBuilder.peek();
  }

  private static List<String> getRemainingFields(
      Collection<String> existingFields, Collection<String> excluded) {
    List<String> keys = excludeMetaFields(existingFields);
    keys.removeAll(excluded);
    Collections.sort(keys);
    return keys;
  }

  private static RexNode getFieldsAsMap(
      Collection<String> existingFields, Collection<String> excluded, CalcitePlanContext context) {
    List<String> keys = excludeMetaFields(existingFields);
    keys.removeAll(excluded);
    Collections.sort(keys); // sort for plan consistency
    RexNode keysArray = getStringLiteralArray(keys, context);
    List<RexNode> values =
        keys.stream().map(key -> context.relBuilder.field(key)).collect(Collectors.toList());
    RexNode valuesArray = makeStringArray(values, context);
    return makeCall(context, BuiltinFunctionName.MAP_FROM_ARRAYS, keysArray, valuesArray);
  }

  private static List<String> excludeMetaFields(Collection<String> fields) {
    return fields.stream().filter(field -> !isMetadataField(field)).collect(Collectors.toList());
  }

  private static RexNode getItemAsString(
      RexNode map, String fieldName, CalcitePlanContext context) {
    RexNode item = itemCall(map, fieldName, context);
    // Cast to string for type consistency. (This cast will be removed once functions are adopted
    // to ANY type)
    return context.relBuilder.cast(item, SqlTypeName.VARCHAR);
  }

  private static RexNode castToString(RexNode node, CalcitePlanContext context) {
    return context.relBuilder.cast(node, SqlTypeName.VARCHAR);
  }

  private static boolean isDynamicFieldsExists(CalcitePlanContext context) {
    return context.relBuilder.peek().getRowType().getFieldNames().contains(DYNAMIC_FIELDS_MAP);
  }

  private RexNode createDynamicMapField(
      RexNode fullMap, Set<String> regularFields, CalcitePlanContext context) {
    if (regularFields.isEmpty()) {
      return fullMap;
    }

    RexNode keyArray = getStringLiteralArray(regularFields, context);

    // MAP_REMOVE(fullMap, keyArray) → filtered map with only unmapped fields
    return makeCall(context, BuiltinFunctionName.MAP_REMOVE, fullMap, keyArray);
  }

  private static RexNode getStringLiteralArray(
      Collection<String> keys, CalcitePlanContext context) {
    List<RexNode> stringLiteralList =
        keys.stream()
            .sorted()
            .map(name -> context.rexBuilder.makeLiteral(name))
            .collect(Collectors.toList());

    return context.rexBuilder.makeCall(
        getStringArrayType(context),
        SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
        stringLiteralList);
  }

  private static RelDataType getStringArrayType(CalcitePlanContext context) {
    return context
        .rexBuilder
        .getTypeFactory()
        .createArrayType(
            context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), -1);
  }

  private static RexNode makeStringArray(List<RexNode> items, CalcitePlanContext context) {
    return context.rexBuilder.makeCall(
        getStringArrayType(context), SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, items);
  }

  private static RexNode itemCall(RexNode node, String key, CalcitePlanContext context) {
    return makeCall(
        context, BuiltinFunctionName.INTERNAL_ITEM, node, context.rexBuilder.makeLiteral(key));
  }

  private static RexNode makeCall(
      CalcitePlanContext context, BuiltinFunctionName functionName, RexNode... args) {
    return PPLFuncImpTable.INSTANCE.resolve(context.rexBuilder, functionName, args);
  }

  @Override
  public RelNode visitPatterns(Patterns node, CalcitePlanContext context) {
    visitChildren(node, context);
    RexNode showNumberedTokenExpr = rexVisitor.analyze(node.getShowNumberedToken(), context);
    Boolean showNumberedToken =
        Boolean.TRUE.equals(((RexLiteral) showNumberedTokenExpr).getValueAs(Boolean.class));
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

        if (showNumberedToken) {
          RexNode parsedNode =
              PPLFuncImpTable.INSTANCE.resolve(
                  context.rexBuilder,
                  BuiltinFunctionName.INTERNAL_PATTERN_PARSER,
                  context.relBuilder.field(node.getAlias()),
                  context.relBuilder.field(PatternUtils.SAMPLE_LOGS));
          flattenParsedPattern(node.getAlias(), parsedNode, context, false, true);
          // Reorder fields for consistency with Brain's output
          projectPlusOverriding(
              List.of(
                  context.relBuilder.field(node.getAlias()),
                  context.relBuilder.field(PatternUtils.PATTERN_COUNT),
                  context.relBuilder.field(PatternUtils.TOKENS),
                  context.relBuilder.field(PatternUtils.SAMPLE_LOGS)),
              List.of(
                  node.getAlias(),
                  PatternUtils.PATTERN_COUNT,
                  PatternUtils.TOKENS,
                  PatternUtils.SAMPLE_LOGS),
              context);
        }
      } else if (showNumberedToken) {
        RexNode parsedNode =
            PPLFuncImpTable.INSTANCE.resolve(
                context.rexBuilder,
                BuiltinFunctionName.INTERNAL_PATTERN_PARSER,
                context.relBuilder.field(node.getAlias()),
                rexVisitor.analyze(node.getSourceField(), context));
        flattenParsedPattern(node.getAlias(), parsedNode, context, false, true);
      }
    } else {
      List<UnresolvedExpression> funcParamList = new ArrayList<>();
      funcParamList.add(node.getSourceField());
      funcParamList.add(node.getPatternMaxSampleCount());
      funcParamList.add(node.getPatternBufferLimit());
      funcParamList.add(node.getShowNumberedToken());
      funcParamList.addAll(
          node.getArguments().entrySet().stream()
              .filter(entry -> PatternUtils.VALID_BRAIN_PARAMETERS.contains(entry.getKey()))
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
                    windowNode,
                    showNumberedTokenExpr),
                node.getAlias());
        context.relBuilder.projectPlus(nestedNode);
        flattenParsedPattern(
            node.getAlias(),
            context.relBuilder.field(node.getAlias()),
            context,
            false,
            showNumberedToken);
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
            node.getAlias(),
            context.relBuilder.field(node.getAlias()),
            context,
            true,
            showNumberedToken);
      }
    }
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitEval(Eval node, CalcitePlanContext context) {
    visitChildren(node, context);
    node.getExpressionList()
        .forEach(
            expr -> {
              boolean containsSubqueryExpression = AstNodeUtils.containsSubqueryExpression(expr);
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
            .filter(originalName -> shouldOverrideField(originalName, newNames))
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

  private boolean shouldOverrideField(String originalName, List<String> newNames) {
    return newNames.stream()
        .anyMatch(
            newName ->
                // Match exact field names (e.g., "age" == "age") for flat fields
                newName.equals(originalName)
                    // OR match nested paths (e.g., "resource.attributes..." starts with
                    // "resource.")
                    || newName.startsWith(originalName + "."));
  }

  private List<List<RexInputRef>> extractInputRefList(List<RelBuilder.AggCall> aggCalls) {
    return aggCalls.stream()
        .map(RelBuilder.AggCall::over)
        .map(RelBuilder.OverCall::toRex)
        .map(node -> getRexCall(node, this::isCountField))
        .map(list -> list.isEmpty() ? null : list.getFirst())
        .map(PlanUtils::getInputRefs)
        .toList();
  }

  /** Is count(FIELD) */
  private boolean isCountField(RexCall call) {
    return call.isA(SqlKind.COUNT)
        && call.getOperands().size() == 1 // count(FIELD)
        && call.getOperands().get(0) instanceof RexInputRef;
  }

  /**
   * Resolve the aggregation with trimming unused fields to avoid bugs in {@link
   * org.apache.calcite.sql2rel.RelDecorrelator#decorrelateRel(Aggregate, boolean, boolean)}
   *
   * @param groupExprList group by expression list
   * @param aggExprList aggregate expression list
   * @param context CalcitePlanContext
   * @param hintIgnoreNullBucket true if bucket_nullable=false
   * @return Pair of (group-by list, field list, aggregate list)
   */
  private Pair<List<RexNode>, List<AggCall>> aggregateWithTrimming(
      List<UnresolvedExpression> groupExprList,
      List<UnresolvedExpression> aggExprList,
      CalcitePlanContext context,
      boolean hintIgnoreNullBucket) {
    Pair<List<RexNode>, List<AggCall>> resolved =
        resolveAttributesForAggregation(groupExprList, aggExprList, context);
    List<RexNode> resolvedGroupByList = resolved.getLeft();
    List<AggCall> resolvedAggCallList = resolved.getRight();

    // `doc_count` optimization required a filter `isNotNull(RexInputRef)` for the
    // `count(FIELD)` aggregation which only can be applied to single FIELD without grouping:
    //
    // Example 1: source=t | stats count(a)
    // Before: Aggregate(count(a))
    //         \- Scan t
    // After: Aggregate(count(a))
    //        \- Filter(isNotNull(a))
    //           \- Scan t
    //
    // Example 2: source=t | stats count(a), count(a)
    // Before: Aggregate(count(a), count(a))
    //         \- Scan t
    // After: Aggregate(count(a), count(a))
    //        \- Filter(isNotNull(a))
    //           \- Scan t
    //
    // Example 3: source=t | stats count(a) by b
    // Before & After: Aggregate(count(a) by b)
    //                 \- Scan t
    //
    // Example 4: source=t | stats count()
    // Before & After: Aggregate(count())
    //                 \- Scan t
    //
    // Example 5: source=t | stats count(), count(a)
    // Before & After: Aggregate(count(), count(a))
    //                 \- Scan t
    //
    // Example 6: source=t | stats count(a), count(b)
    // Before & After: Aggregate(count(a), count(b))
    //                 \- Scan t
    //
    // Example 7: source=t | stats count(a+1)
    // Before & After: Aggregate(count(a+1))
    //                 \- Scan t
    if (resolvedGroupByList.isEmpty()) {
      List<List<RexInputRef>> refsPerCount = extractInputRefList(resolvedAggCallList);
      List<RexInputRef> distinctRefsOfCounts;
      if (context.relBuilder.peek() instanceof org.apache.calcite.rel.core.Project project) {
        List<RexNode> mappedInProject =
            refsPerCount.stream()
                .flatMap(List::stream)
                .map(ref -> project.getProjects().get(ref.getIndex()))
                .toList();
        if (mappedInProject.stream().allMatch(RexInputRef.class::isInstance)) {
          distinctRefsOfCounts =
              mappedInProject.stream().map(RexInputRef.class::cast).distinct().toList();
        } else {
          distinctRefsOfCounts = List.of();
        }
      } else {
        distinctRefsOfCounts = refsPerCount.stream().flatMap(List::stream).distinct().toList();
      }
      if (distinctRefsOfCounts.size() == 1 && refsPerCount.stream().noneMatch(List::isEmpty)) {
        context.relBuilder.filter(context.relBuilder.isNotNull(distinctRefsOfCounts.getFirst()));
      }
    }

    // Add project before aggregate:
    //
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
    // Before: Aggregate(count(b) by c)
    //         \-Filter(a > 1 && isNotNull(b))
    //           \- Scan t
    // After: Aggregate(count(b) by c)
    //        \- Project([c, b])
    //           \- Filter(a > 1 && isNotNull(b))
    //              \- Scan t
    //
    // Example 3: source=t | stats count(): no change for count()
    // Before: Aggregate(count())
    //           \- Scan t
    // After: Aggregate(count())
    //           \- Scan t
    List<RexInputRef> trimmedRefs = new ArrayList<>();
    trimmedRefs.addAll(PlanUtils.getInputRefs(resolvedGroupByList)); // group-by keys first
    List<RexInputRef> aggCallRefs = PlanUtils.getInputRefsFromAggCall(resolvedAggCallList);
    boolean hintNestedAgg = containsNestedAggregator(context.relBuilder, aggCallRefs);
    trimmedRefs.addAll(aggCallRefs);
    context.relBuilder.project(trimmedRefs);

    // Re-resolve all attributes based on adding trimmed Project.
    // Using re-resolving rather than Calcite Mapping (ref Calcite ProjectTableScanRule)
    // because that Mapping only works for RexNode, but we need both AggCall and RexNode list.
    Pair<List<RexNode>, List<AggCall>> reResolved =
        resolveAttributesForAggregation(groupExprList, aggExprList, context);

    List<String> intendedGroupKeyAliases = getGroupKeyNamesAfterAggregation(reResolved.getLeft());
    context.relBuilder.aggregate(
        context.relBuilder.groupKey(reResolved.getLeft()), reResolved.getRight());
    if (hintIgnoreNullBucket) PPLHintUtils.addIgnoreNullBucketHintToAggregate(context.relBuilder);
    if (hintNestedAgg) PPLHintUtils.addNestedAggCallHintToAggregate(context.relBuilder);
    // During aggregation, Calcite projects both input dependencies and output group-by fields.
    // When names conflict, Calcite adds numeric suffixes (e.g., "value0").
    // Apply explicit renaming to restore the intended aliases.
    context.relBuilder.rename(intendedGroupKeyAliases);

    return Pair.of(reResolved.getLeft(), reResolved.getRight());
  }

  /**
   * Return true if the aggCalls contains a nested field. For example: aggCalls: [count(),
   * count(a.b)] returns true.
   */
  private boolean containsNestedAggregator(RelBuilder relBuilder, List<RexInputRef> aggCallRefs) {
    return aggCallRefs.stream()
        .map(r -> relBuilder.peek().getRowType().getFieldNames().get(r.getIndex()))
        .map(name -> org.apache.commons.lang3.StringUtils.substringBefore(name, "."))
        .anyMatch(root -> relBuilder.field(root).getType().getSqlTypeName() == SqlTypeName.ARRAY);
  }

  /**
   * Imitates {@code Registrar.registerExpression} of {@link RelBuilder} to derive the output order
   * of group-by keys after aggregation.
   *
   * <p>The projected input reference comes first, while any other computed expression follows.
   */
  private List<String> getGroupKeyNamesAfterAggregation(List<RexNode> nodes) {
    List<RexNode> reordered = new ArrayList<>();
    List<RexNode> left = new ArrayList<>();
    for (RexNode n : nodes) {
      // The same group-key won't be added twice
      if (reordered.contains(n) || left.contains(n)) {
        continue;
      }
      if (isInputRef(n)) {
        reordered.add(n);
      } else {
        left.add(n);
      }
    }
    reordered.addAll(left);
    return reordered.stream()
        .map(this::extractAliasLiteral)
        .flatMap(Optional::stream)
        .map(RexLiteral::stringValue)
        .toList();
  }

  /** Whether a rex node is an aliased input reference */
  private boolean isInputRef(RexNode node) {
    return switch (node.getKind()) {
      case AS, DESCENDING, NULLS_FIRST, NULLS_LAST -> {
        final List<RexNode> operands = ((RexCall) node).operands;
        yield isInputRef(operands.getFirst());
      }
      default -> node instanceof RexInputRef;
    };
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

  /** Visits an aggregation for stats command */
  @Override
  public RelNode visitAggregation(Aggregation node, CalcitePlanContext context) {
    Argument.ArgumentMap statsArgs = Argument.ArgumentMap.of(node.getArgExprList());
    Boolean bucketNullable = (Boolean) statsArgs.get(Argument.BUCKET_NULLABLE).getValue();
    int nGroup = node.getGroupExprList().size() + (Objects.nonNull(node.getSpan()) ? 1 : 0);
    BitSet nonNullGroupMask = new BitSet(nGroup);
    if (!bucketNullable) {
      nonNullGroupMask.set(0, nGroup);
    }
    visitAggregation(node, context, nonNullGroupMask, true, false);
    return context.relBuilder.peek();
  }

  /**
   * Visits an aggregation node and builds the corresponding Calcite RelNode.
   *
   * @param node the aggregation node containing group expressions and aggregation functions
   * @param context the Calcite plan context for building RelNodes
   * @param nonNullGroupMask bit set indicating group by fields that need to be non-null
   * @param metricsFirst if true, aggregation results (metrics) appear first in output schema
   *     (metrics, group-by fields); if false, group expressions appear first (group-by fields,
   *     metrics).
   * @param includeAggFieldsInNullFilter if true, also applies non-null filters to aggregation input
   *     fields in addition to group-by fields
   */
  private void visitAggregation(
      Aggregation node,
      CalcitePlanContext context,
      BitSet nonNullGroupMask,
      boolean metricsFirst,
      boolean includeAggFieldsInNullFilter) {
    visitChildren(node, context);

    List<UnresolvedExpression> aggExprList = node.getAggExprList();
    List<UnresolvedExpression> groupExprList = new ArrayList<>();
    // The span column is always the first column in result whatever
    // the order of span in query is first or last one
    UnresolvedExpression span = node.getSpan();
    if (Objects.nonNull(span)) {
      groupExprList.add(span);
      if (getTimeSpanField(span).isPresent()) {
        nonNullGroupMask.set(0);
      }
    }
    groupExprList.addAll(node.getGroupExprList());

    // Add a hint to LogicalAggregation when bucket_nullable=false.
    boolean hintIgnoreNullBucket =
        !groupExprList.isEmpty()
            // This checks if all group-bys should be nonnull
            && nonNullGroupMask.nextClearBit(0) >= groupExprList.size();
    // Add isNotNull filter before aggregation for non-nullable buckets
    List<RexNode> nonNullCandidates =
        groupExprList.stream()
            .map(expr -> rexVisitor.analyze(expr, context))
            .collect(Collectors.toCollection(ArrayList::new));
    if (includeAggFieldsInNullFilter) {
      nonNullCandidates.addAll(
          PlanUtils.getInputRefsFromAggCall(
              aggExprList.stream().map(expr -> aggVisitor.analyze(expr, context)).toList()));
      nonNullGroupMask.set(groupExprList.size(), nonNullCandidates.size());
    }
    List<RexNode> nonNullFields =
        IntStream.range(0, nonNullCandidates.size())
            .filter(nonNullGroupMask::get)
            .mapToObj(nonNullCandidates::get)
            .toList();
    if (!nonNullFields.isEmpty()) {
      context.relBuilder.filter(
          PlanUtils.getSelectColumns(nonNullFields).stream()
              .map(context.relBuilder::field)
              .map(context.relBuilder::isNotNull)
              .toList());
    }

    Pair<List<RexNode>, List<AggCall>> aggregationAttributes =
        aggregateWithTrimming(groupExprList, aggExprList, context, hintIgnoreNullBucket);

    // schema reordering
    List<RexNode> outputFields = context.relBuilder.fields();
    int numOfOutputFields = outputFields.size();
    int numOfAggList = aggExprList.size();
    List<RexNode> reordered = new ArrayList<>(numOfOutputFields);
    // Add aggregation results first
    List<RexNode> aggRexList =
        outputFields.subList(numOfOutputFields - numOfAggList, numOfOutputFields);
    List<RexNode> aliasedGroupByList =
        aggregationAttributes.getLeft().stream()
            .map(this::extractAliasLiteral)
            .flatMap(Optional::stream)
            .map(ref -> ref.getValueAs(String.class))
            .map(context.relBuilder::field)
            .map(f -> (RexNode) f)
            .toList();
    if (metricsFirst) {
      // As an example, in command `stats count() by colA, colB`,
      // the sequence of output schema is "count, colA, colB".
      reordered.addAll(aggRexList);
      // Add group by columns
      reordered.addAll(aliasedGroupByList);
    } else {
      reordered.addAll(aliasedGroupByList);
      reordered.addAll(aggRexList);
    }
    context.relBuilder.project(reordered);
  }

  private Optional<UnresolvedExpression> getTimeSpanField(UnresolvedExpression expr) {
    if (Objects.isNull(expr)) return Optional.empty();
    if (expr instanceof Span span && SpanUnit.isTimeUnit(span.getUnit())) {
      return Optional.of(span.getField());
    }
    if (expr instanceof Alias alias) {
      return getTimeSpanField(alias.getDelegated());
    }
    return Optional.empty();
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
    if (node.getJoinCondition().isEmpty()) {
      // join-with-field-list grammar
      List<String> leftColumns = getLeftStaticFields(context);
      List<String> rightColumns = getRightStaticFields(context);
      List<String> duplicatedFieldNames =
          leftColumns.stream()
              .filter(column -> !isDynamicFieldMap(column) && rightColumns.contains(column))
              .toList();
      RexNode joinCondition;
      if (node.getJoinFields().isPresent()) {
        joinCondition =
            node.getJoinFields().get().stream()
                .map(field -> buildJoinConditionByFieldName(context, field.getField().toString()))
                .reduce(context.rexBuilder::and)
                .orElse(context.relBuilder.literal(true));
      } else {
        joinCondition =
            duplicatedFieldNames.stream()
                .map(fieldName -> buildJoinConditionByFieldName(context, fieldName))
                .reduce(context.rexBuilder::and)
                .orElse(context.relBuilder.literal(true));
      }
      if (node.getJoinType() == SEMI || node.getJoinType() == ANTI) {
        // semi and anti join only return left table outputs
        context.relBuilder.join(
            JoinAndLookupUtils.translateJoinType(node.getJoinType()), joinCondition);
        return context.relBuilder.peek();
      }
      List<RexNode> toBeRemovedFields;
      if (node.isOverwrite()) {
        toBeRemovedFields =
            duplicatedFieldNames.stream()
                .map(field -> JoinAndLookupUtils.analyzeFieldsForLookUp(field, true, context))
                .toList();
      } else {
        toBeRemovedFields =
            duplicatedFieldNames.stream()
                .map(field -> JoinAndLookupUtils.analyzeFieldsForLookUp(field, false, context))
                .toList();
      }
      Literal max = node.getArgumentMap().get("max");
      if (max != null && !max.equals(Literal.ZERO)) {
        // max != 0 means the right-side should be dedup
        Integer allowedDuplication = (Integer) max.getValue();
        if (allowedDuplication < 0) {
          throw new SemanticCheckException("max option must be a positive integer");
        }
        List<RexNode> dedupeFields =
            node.getJoinFields().isPresent()
                ? node.getJoinFields().get().stream()
                    .map(a -> (RexNode) context.relBuilder.field(a.getField().toString()))
                    .toList()
                : duplicatedFieldNames.stream()
                    .map(a -> (RexNode) context.relBuilder.field(a))
                    .toList();
        buildDedupNotNull(context.relBuilder, dedupeFields, allowedDuplication);
      }
      // add LogicalSystemLimit after dedup
      addSysLimitForJoinSubsearch(context);
      context.relBuilder.join(
          JoinAndLookupUtils.translateJoinType(node.getJoinType()), joinCondition);
      if (!toBeRemovedFields.isEmpty()) {
        context.relBuilder.projectExcept(toBeRemovedFields);
      }
      JoinAndLookupUtils.mergeDynamicFieldsAsNeeded(
          context, node.isOverwrite() ? OverwriteMode.RIGHT_WINS : OverwriteMode.LEFT_WINS);
    } else {
      // The join-with-criteria grammar doesn't allow empty join condition
      RexNode joinCondition =
          node.getJoinCondition()
              .map(c -> rexVisitor.analyzeJoinCondition(c, context))
              .orElse(context.relBuilder.literal(true));
      if (node.getJoinType() == SEMI || node.getJoinType() == ANTI) {
        // semi and anti join only return left table outputs
        context.relBuilder.join(
            JoinAndLookupUtils.translateJoinType(node.getJoinType()), joinCondition);
        return context.relBuilder.peek();
      }
      // Join condition could contain duplicated column name, Calcite will rename the duplicated
      // column name with numeric suffix, e.g. ON t1.id = t2.id, the output contains `id` and `id0`
      // when a new project add to stack. To avoid `id0`, we will rename the `id0` to `alias.id`
      // or `tableIdentifier.id`:
      List<String> leftColumns = getLeftStaticFields(context);
      List<String> rightColumns = getRightStaticFields(context);
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
                      !isDynamicFieldMap(col) && leftColumns.contains(col)
                          ? node.getRightAlias()
                              .map(a -> a + "." + col)
                              .orElse(rightTableQualifiedName + "." + col)
                          : col)
              .toList();

      Literal max = node.getArgumentMap().get("max");
      if (max != null && !max.equals(Literal.ZERO)) {
        // max != 0 means the right-side should be dedup
        Integer allowedDuplication = (Integer) max.getValue();
        if (allowedDuplication < 0) {
          throw new SemanticCheckException("max option must be a positive integer");
        }
        List<RexNode> dedupeFields =
            getRightColumnsInJoinCriteria(context.relBuilder, joinCondition);

        buildDedupNotNull(context.relBuilder, dedupeFields, allowedDuplication);
      }
      // add LogicalSystemLimit after dedup
      addSysLimitForJoinSubsearch(context);
      context.relBuilder.join(
          JoinAndLookupUtils.translateJoinType(node.getJoinType()), joinCondition);
      JoinAndLookupUtils.mergeDynamicFieldsAsNeeded(context, OverwriteMode.LEFT_WINS);
      JoinAndLookupUtils.renameToExpectedFields(
          rightColumnsWithAliasIfConflict, leftColumns.size(), context);
    }
    return context.relBuilder.peek();
  }

  private static boolean isDynamicFieldMap(String field) {
    return DYNAMIC_FIELDS_MAP.equals(field);
  }

  private static List<String> getLeftStaticFields(CalcitePlanContext context) {
    return excludeDynamicFields(context.relBuilder.peek(1).getRowType().getFieldNames());
  }

  private static List<String> getStaticFields(CalcitePlanContext context) {
    return excludeDynamicFields(context.relBuilder.peek().getRowType().getFieldNames());
  }

  private static List<String> getRightStaticFields(CalcitePlanContext context) {
    return getStaticFields(context);
  }

  private static List<String> excludeDynamicFields(List<String> fieldNames) {
    return fieldNames.stream()
        .filter(fieldName -> !isDynamicFieldMap(fieldName))
        .collect(Collectors.toList());
  }

  private static void addSysLimitForJoinSubsearch(CalcitePlanContext context) {
    // add join.subsearch_maxout limit to subsearch side, 0 and negative means unlimited.
    if (context.sysLimit.joinSubsearchLimit() > 0) {
      PlanUtils.replaceTop(
          context.relBuilder,
          LogicalSystemLimit.create(
              SystemLimitType.JOIN_SUBSEARCH_MAXOUT,
              context.relBuilder.peek(),
              context.relBuilder.literal(context.sysLimit.joinSubsearchLimit())));
    }
  }

  private List<RexNode> getRightColumnsInJoinCriteria(
      RelBuilder relBuilder, RexNode joinCondition) {
    int stackSize = relBuilder.size();
    int leftFieldCount = relBuilder.peek(stackSize - 1).getRowType().getFieldCount();
    RelNode right = relBuilder.peek(stackSize - 2);
    List<String> allColumnNamesOfRight = right.getRowType().getFieldNames();

    List<Integer> rightColumnIndexes = new ArrayList<>();
    joinCondition.accept(
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitInputRef(RexInputRef inputRef) {
            if (inputRef.getIndex() >= leftFieldCount) {
              rightColumnIndexes.add(inputRef.getIndex() - leftFieldCount);
            }
            return super.visitInputRef(inputRef);
          }
        });
    return rightColumnIndexes.stream()
        .map(allColumnNamesOfRight::get)
        .map(n -> (RexNode) relBuilder.field(n))
        .toList();
  }

  private static RexNode buildJoinConditionByFieldName(
      CalcitePlanContext context, String fieldName) {
    RexNode lookupKey = JoinAndLookupUtils.analyzeFieldsForLookUp(fieldName, false, context);
    RexNode sourceKey = JoinAndLookupUtils.analyzeFieldsForLookUp(fieldName, true, context);
    return context.rexBuilder.equals(sourceKey, lookupKey);
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
      throw new CalciteUnsupportedException("Consecutive deduplication is unsupported in Calcite");
    }
    // Columns to deduplicate
    List<RexNode> dedupeFields =
        node.getFields().stream().map(f -> rexVisitor.analyze(f, context)).toList();
    if (keepEmpty) {
      buildDedupOrNull(context.relBuilder, dedupeFields, allowedDuplication);
    } else {
      buildDedupNotNull(context.relBuilder, dedupeFields, allowedDuplication);
    }
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitWindow(Window node, CalcitePlanContext context) {
    visitChildren(node, context);

    List<UnresolvedExpression> groupList = node.getGroupList();
    boolean hasGroup = groupList != null && !groupList.isEmpty();
    boolean bucketNullable = node.isBucketNullable();

    List<RexNode> overExpressions =
        node.getWindowFunctionList().stream().map(w -> rexVisitor.analyze(w, context)).toList();

    if (hasGroup && !bucketNullable) {
      // construct groupNotNull predicate
      List<RexNode> groupByList =
          groupList.stream().map(expr -> rexVisitor.analyze(expr, context)).toList();
      List<RexNode> notNullList =
          PlanUtils.getSelectColumns(groupByList).stream()
              .map(context.relBuilder::field)
              .map(context.relBuilder::isNotNull)
              .toList();
      RexNode groupNotNull = context.relBuilder.and(notNullList);

      // wrap each expr: CASE WHEN groupNotNull THEN rawExpr ELSE CAST(NULL AS rawType) END
      List<RexNode> wrappedOverExprs =
          wrapWindowFunctionsWithGroupNotNull(overExpressions, groupNotNull, context);
      context.relBuilder.projectPlus(wrappedOverExprs);
    } else {
      context.relBuilder.projectPlus(overExpressions);
    }
    return context.relBuilder.peek();
  }

  /**
   * Validates type compatibility between replacement value and field for fillnull operation. Throws
   * SemanticCheckException if types are incompatible.
   */
  private void validateFillNullTypeCompatibility(
      RexNode replacement, RexNode fieldRef, String fieldName) {
    RelDataTypeFamily replacementFamily = replacement.getType().getFamily();
    RelDataTypeFamily fieldFamily = fieldRef.getType().getFamily();

    // Check if the replacement type is compatible with the field type
    // Allow NULL type family as it's compatible with any type
    if (fieldFamily != replacementFamily
        && fieldFamily != SqlTypeFamily.NULL
        && replacementFamily != SqlTypeFamily.NULL) {
      throw new SemanticCheckException(
          String.format(
              "fillnull failed: replacement value type %s is not compatible with field '%s' "
                  + "(type: %s). The replacement value type must match the field type.",
              replacement.getType().getSqlTypeName(),
              fieldName,
              fieldRef.getType().getSqlTypeName()));
    }
  }

  @Override
  public RelNode visitStreamWindow(StreamWindow node, CalcitePlanContext context) {
    visitChildren(node, context);

    List<UnresolvedExpression> groupList = node.getGroupList();
    boolean hasGroup = groupList != null && !groupList.isEmpty();
    boolean hasWindow = node.getWindow() > 0;
    boolean hasReset = node.getResetBefore() != null || node.getResetAfter() != null;

    // Local helper column names
    final String RESET_BEFORE_FLAG_COL = "__reset_before_flag__"; // flag for reset_before
    final String RESET_AFTER_FLAG_COL = "__reset_after_flag__"; // flag for reset_after
    final String SEGMENT_ID_COL = "__seg_id__"; // segment id

    // CASE: reset
    if (hasReset) {
      // 1. Build helper columns: seq, before/after flags, segment_id
      RelNode leftWithSeg = buildResetHelperColumns(context, node);

      // 2. Run correlate + aggregate with reset-specific filter and cleanup
      return buildStreamWindowJoinPlan(
          context,
          leftWithSeg,
          node,
          groupList,
          ROW_NUMBER_COLUMN_FOR_STREAMSTATS,
          SEGMENT_ID_COL,
          new String[] {
            ROW_NUMBER_COLUMN_FOR_STREAMSTATS,
            RESET_BEFORE_FLAG_COL,
            RESET_AFTER_FLAG_COL,
            SEGMENT_ID_COL
          });
    }

    // CASE: global=true + window>0 + has group
    if (node.isGlobal() && hasWindow && hasGroup) {
      // 1. Add global sequence column for sliding window
      RexNode streamSeq =
          context
              .relBuilder
              .aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
              .over()
              .rowsTo(RexWindowBounds.CURRENT_ROW)
              .as(ROW_NUMBER_COLUMN_FOR_STREAMSTATS);
      context.relBuilder.projectPlus(streamSeq);
      RelNode left = context.relBuilder.build();

      // 2. Run correlate + aggregate
      return buildStreamWindowJoinPlan(
          context,
          left,
          node,
          groupList,
          ROW_NUMBER_COLUMN_FOR_STREAMSTATS,
          null,
          new String[] {ROW_NUMBER_COLUMN_FOR_STREAMSTATS});
    }

    // Default: first get rawExpr
    List<RexNode> overExpressions =
        node.getWindowFunctionList().stream().map(w -> rexVisitor.analyze(w, context)).toList();

    if (hasGroup) {
      // only build sequence when there is by condition
      RexNode streamSeq =
          context
              .relBuilder
              .aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
              .over()
              .rowsTo(RexWindowBounds.CURRENT_ROW)
              .as(ROW_NUMBER_COLUMN_FOR_STREAMSTATS);
      context.relBuilder.projectPlus(streamSeq);

      if (!node.isBucketNullable()) {
        // construct groupNotNull predicate
        List<RexNode> groupByList =
            groupList.stream().map(expr -> rexVisitor.analyze(expr, context)).toList();
        List<RexNode> notNullList =
            PlanUtils.getSelectColumns(groupByList).stream()
                .map(context.relBuilder::field)
                .map(context.relBuilder::isNotNull)
                .toList();
        RexNode groupNotNull = context.relBuilder.and(notNullList);

        // wrap each expr: CASE WHEN groupNotNull THEN rawExpr ELSE CAST(NULL AS rawType) END
        List<RexNode> wrappedOverExprs =
            wrapWindowFunctionsWithGroupNotNull(overExpressions, groupNotNull, context);
        context.relBuilder.projectPlus(wrappedOverExprs);
      } else {
        context.relBuilder.projectPlus(overExpressions);
      }

      // resort when there is by condition
      context.relBuilder.sort(context.relBuilder.field(ROW_NUMBER_COLUMN_FOR_STREAMSTATS));
      context.relBuilder.projectExcept(context.relBuilder.field(ROW_NUMBER_COLUMN_FOR_STREAMSTATS));
    } else {
      context.relBuilder.projectPlus(overExpressions);
    }

    return context.relBuilder.peek();
  }

  private List<RexNode> wrapWindowFunctionsWithGroupNotNull(
      List<RexNode> overExpressions, RexNode groupNotNull, CalcitePlanContext context) {
    List<RexNode> wrappedOverExprs = new ArrayList<>(overExpressions.size());
    for (RexNode overExpr : overExpressions) {
      RexNode rawExpr = overExpr;
      String aliasName = null;
      if (overExpr instanceof RexCall rc && rc.getOperator() == SqlStdOperatorTable.AS) {
        rawExpr = rc.getOperands().get(0);
        if (rc.getOperands().size() >= 2 && rc.getOperands().get(1) instanceof RexLiteral lit) {
          aliasName = lit.getValueAs(String.class);
        }
      }
      RexNode nullLiteral = context.rexBuilder.makeNullLiteral(rawExpr.getType());
      RexNode caseExpr =
          context.rexBuilder.makeCall(SqlStdOperatorTable.CASE, groupNotNull, rawExpr, nullLiteral);
      if (aliasName != null) {
        caseExpr = context.relBuilder.alias(caseExpr, aliasName);
      }
      wrappedOverExprs.add(caseExpr);
    }
    return wrappedOverExprs;
  }

  private RelNode buildStreamWindowJoinPlan(
      CalcitePlanContext context,
      RelNode leftWithHelpers,
      StreamWindow node,
      List<UnresolvedExpression> groupList,
      String seqCol,
      String segmentCol,
      String[] helperColsToCleanup) {

    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    context.relBuilder.push(leftWithHelpers);
    context.relBuilder.variable(v::set);

    RexNode rightSeq = context.relBuilder.field(seqCol);
    RexNode outerSeq = context.relBuilder.field(v.get(), seqCol);

    RexNode filter;
    if (segmentCol != null) { // reset condition
      RexNode segRight = context.relBuilder.field(segmentCol);
      RexNode segOuter = context.relBuilder.field(v.get(), segmentCol);
      RexNode frame = buildResetFrameFilter(context, node, outerSeq, rightSeq, segOuter, segRight);
      RexNode group = buildGroupFilter(context, node, groupList, v.get());
      filter = (group == null) ? frame : context.relBuilder.and(frame, group);
    } else { // global + window + by condition
      RexNode frame = buildFrameFilter(context, node, outerSeq, rightSeq);
      RexNode group = buildGroupFilter(context, node, groupList, v.get());
      filter = context.relBuilder.and(frame, group);
    }
    context.relBuilder.filter(filter);

    // aggregate all window functions on right side
    aggregateWithTrimming(List.of(), node.getWindowFunctionList(), context, false);
    RelNode rightAgg = context.relBuilder.build();

    // correlate LEFT with RIGHT using seq + group fields
    context.relBuilder.push(leftWithHelpers);
    context.relBuilder.push(rightAgg);
    List<RexNode> requiredLeft = buildRequiredLeft(context, seqCol, groupList);
    if (segmentCol != null) { // also require seg_id for reset segmentation equality
      requiredLeft = new ArrayList<>(requiredLeft);
      requiredLeft.add(context.relBuilder.field(2, 0, segmentCol));
    }
    context.relBuilder.correlate(JoinRelType.LEFT, v.get().id, requiredLeft);

    // resort to original order
    boolean hasGroup = !groupList.isEmpty();
    // resort when 1. global + window + by condition 2.reset + by condition
    if (hasGroup) {
      context.relBuilder.sort(context.relBuilder.field(seqCol));
    }

    // cleanup helper columns
    List<RexNode> cleanup = new ArrayList<>();
    for (String c : helperColsToCleanup) {
      cleanup.add(context.relBuilder.field(c));
    }
    context.relBuilder.projectExcept(cleanup);
    return context.relBuilder.peek();
  }

  private RelNode buildResetHelperColumns(CalcitePlanContext context, StreamWindow node) {
    // 1. global sequence to define order
    RexNode rowNum =
        context
            .relBuilder
            .aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
            .over()
            .rowsTo(RexWindowBounds.CURRENT_ROW)
            .as(ROW_NUMBER_COLUMN_FOR_STREAMSTATS);
    context.relBuilder.projectPlus(rowNum);

    // 2. before/after flags
    RexNode beforePred =
        (node.getResetBefore() == null)
            ? context.relBuilder.literal(false)
            : rexVisitor.analyze(node.getResetBefore(), context);
    RexNode afterPred =
        (node.getResetAfter() == null)
            ? context.relBuilder.literal(false)
            : rexVisitor.analyze(node.getResetAfter(), context);
    RexNode beforeFlag =
        context.relBuilder.call(
            SqlStdOperatorTable.CASE,
            beforePred,
            context.relBuilder.literal(1),
            context.relBuilder.literal(0));
    RexNode afterFlag =
        context.relBuilder.call(
            SqlStdOperatorTable.CASE,
            afterPred,
            context.relBuilder.literal(1),
            context.relBuilder.literal(0));
    context.relBuilder.projectPlus(context.relBuilder.alias(beforeFlag, "__reset_before_flag__"));
    context.relBuilder.projectPlus(context.relBuilder.alias(afterFlag, "__reset_after_flag__"));

    // 3. session id = SUM(beforeFlag) over (to current) + SUM(afterFlag) over (to 1 preceding)
    RexNode sumBefore =
        context
            .relBuilder
            .aggregateCall(
                SqlStdOperatorTable.SUM, context.relBuilder.field("__reset_before_flag__"))
            .over()
            .rowsTo(RexWindowBounds.CURRENT_ROW)
            .toRex();
    RexNode sumAfterPrev =
        context
            .relBuilder
            .aggregateCall(
                SqlStdOperatorTable.SUM, context.relBuilder.field("__reset_after_flag__"))
            .over()
            .rowsBetween(
                RexWindowBounds.UNBOUNDED_PRECEDING,
                RexWindowBounds.preceding(context.relBuilder.literal(1)))
            .toRex();
    sumBefore =
        context.relBuilder.call(
            SqlStdOperatorTable.COALESCE, sumBefore, context.relBuilder.literal(0));
    sumAfterPrev =
        context.relBuilder.call(
            SqlStdOperatorTable.COALESCE, sumAfterPrev, context.relBuilder.literal(0));

    RexNode segId = context.relBuilder.call(SqlStdOperatorTable.PLUS, sumBefore, sumAfterPrev);
    context.relBuilder.projectPlus(context.relBuilder.alias(segId, "__seg_id__"));
    return context.relBuilder.build();
  }

  private RexNode buildFrameFilter(
      CalcitePlanContext context, StreamWindow node, RexNode outerSeq, RexNode rightSeq) {
    // window always >0
    // frame: either [outer-(w-1), outer] or [outer-w, outer-1]
    if (node.isCurrent()) {
      RexNode lower =
          context.relBuilder.call(
              SqlStdOperatorTable.MINUS,
              outerSeq,
              context.relBuilder.literal(node.getWindow() - 1));
      return context.relBuilder.between(rightSeq, lower, outerSeq);
    } else {
      RexNode lower =
          context.relBuilder.call(
              SqlStdOperatorTable.MINUS, outerSeq, context.relBuilder.literal(node.getWindow()));
      RexNode upper =
          context.relBuilder.call(
              SqlStdOperatorTable.MINUS, outerSeq, context.relBuilder.literal(1));
      return context.relBuilder.between(rightSeq, lower, upper);
    }
  }

  private RexNode buildResetFrameFilter(
      CalcitePlanContext context,
      StreamWindow node,
      RexNode outerSeq,
      RexNode rightSeq,
      RexNode segIdOuter,
      RexNode segIdRight) {
    // 1. Compute sequence range (handle running window semantics when window == 0)
    RexNode seqFilter;
    if (node.getWindow() == 0) {
      // running: current => rightSeq <= outerSeq; excluding current => rightSeq < outerSeq
      seqFilter =
          node.isCurrent()
              ? context.relBuilder.lessThanOrEqual(rightSeq, outerSeq)
              : context.relBuilder.lessThan(rightSeq, outerSeq);
    } else {
      // Reuse normal frame filter logic when window > 0
      seqFilter = buildFrameFilter(context, node, outerSeq, rightSeq);
    }
    // 2. Ensure same segment (seg_id) for reset partitioning
    RexNode segFilter = context.relBuilder.equals(segIdRight, segIdOuter);
    // 3. Combine filters
    return context.relBuilder.and(seqFilter, segFilter);
  }

  private RexNode buildGroupFilter(
      CalcitePlanContext context,
      StreamWindow node,
      List<UnresolvedExpression> groupList,
      RexCorrelVariable correl) {
    // build conjunctive equality filters: right.g_i = outer.g_i
    if (groupList.isEmpty()) {
      return null;
    }
    List<RexNode> equalsList =
        groupList.stream()
            .map(
                expr -> {
                  String groupName = extractGroupFieldName(expr);
                  RexNode rightGroup = context.relBuilder.field(groupName);
                  RexNode outerGroup = context.relBuilder.field(correl, groupName);
                  RexNode equalCondition = context.relBuilder.equals(rightGroup, outerGroup);
                  // handle bucket_nullable case
                  if (!node.isBucketNullable()) {
                    return equalCondition;
                  } else {
                    RexNode bothNull =
                        context.relBuilder.and(
                            context.relBuilder.isNull(rightGroup),
                            context.relBuilder.isNull(outerGroup));
                    return context.relBuilder.or(equalCondition, bothNull);
                  }
                })
            .toList();
    return context.relBuilder.and(equalsList);
  }

  private String extractGroupFieldName(UnresolvedExpression groupExpr) {
    if (groupExpr instanceof Alias groupAlias
        && groupAlias.getDelegated() instanceof Field groupField) {
      return groupField.getField().toString();
    } else if (groupExpr instanceof Field groupField) {
      return groupField.getField().toString();
    } else {
      throw new IllegalArgumentException(
          "Unsupported group expression: only field or alias(field) is supported");
    }
  }

  private List<RexNode> buildRequiredLeft(
      CalcitePlanContext context, String seqCol, List<UnresolvedExpression> groupList) {
    List<RexNode> requiredLeft = new ArrayList<>();
    // reference to left seq column
    requiredLeft.add(context.relBuilder.field(2, 0, seqCol));
    for (UnresolvedExpression groupExpr : groupList) {
      String groupName = extractGroupFieldName(groupExpr);
      requiredLeft.add(context.relBuilder.field(2, 0, groupName));
    }
    return requiredLeft;
  }

  @Override
  public RelNode visitFillNull(FillNull node, CalcitePlanContext context) {
    visitChildren(node, context);
    if (node.getFields().size()
        != new HashSet<>(node.getFields().stream().map(f -> f.getField().toString()).toList())
            .size()) {
      throw new IllegalArgumentException("The field list cannot be duplicated in fillnull");
    }

    // Validate type compatibility when replacementForAll is present
    if (node.getReplacementForAll().isPresent()) {
      List<RelDataTypeField> fieldsList = context.relBuilder.peek().getRowType().getFieldList();
      RexNode replacement = rexVisitor.analyze(node.getReplacementForAll().get(), context);

      // Validate all fields are compatible with the replacement value
      for (RelDataTypeField field : fieldsList) {
        RexNode fieldRef = context.rexBuilder.makeInputRef(field.getType(), field.getIndex());
        validateFillNullTypeCompatibility(replacement, fieldRef, field.getName());
      }
    }

    List<RexNode> projects = new ArrayList<>();
    List<RelDataTypeField> fieldsList = context.relBuilder.peek().getRowType().getFieldList();
    for (RelDataTypeField field : fieldsList) {
      RexNode fieldRef = context.rexBuilder.makeInputRef(field.getType(), field.getIndex());
      boolean toReplace = false;
      for (Pair<Field, UnresolvedExpression> pair : node.getReplacementPairs()) {
        if (field.getName().equalsIgnoreCase(pair.getLeft().getField().toString())) {
          RexNode replacement = rexVisitor.analyze(pair.getRight(), context);
          // Validate type compatibility before COALESCE
          validateFillNullTypeCompatibility(replacement, fieldRef, field.getName());
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
        context.relBuilder.alias(mainRowNumber, ROW_NUMBER_COLUMN_FOR_MAIN));

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
        context.relBuilder.alias(subsearchRowNumber, ROW_NUMBER_COLUMN_FOR_SUBSEARCH));

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
            context.relBuilder.field(2, 0, ROW_NUMBER_COLUMN_FOR_MAIN),
            context.relBuilder.field(2, 1, ROW_NUMBER_COLUMN_FOR_SUBSEARCH));
    context.relBuilder.join(
        JoinAndLookupUtils.translateJoinType(Join.JoinType.FULL), joinCondition);

    if (!node.isOverride()) {
      // 8. if override = false, drop both _row_number_ columns
      context.relBuilder.projectExcept(
          List.of(
              context.relBuilder.field(ROW_NUMBER_COLUMN_FOR_MAIN),
              context.relBuilder.field(ROW_NUMBER_COLUMN_FOR_SUBSEARCH)));
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
              context.relBuilder.field(ROW_NUMBER_COLUMN_FOR_MAIN),
              context.relBuilder.field(ROW_NUMBER_COLUMN_FOR_SUBSEARCH));
      for (int mainFieldIndex = 0; mainFieldIndex < mainFields.size(); mainFieldIndex++) {
        String mainFieldName = mainFields.get(mainFieldIndex);
        if (mainFieldName.equals(ROW_NUMBER_COLUMN_FOR_MAIN)) {
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
        if (subsearchFieldName.equals(ROW_NUMBER_COLUMN_FOR_SUBSEARCH)) {
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

  @Override
  public RelNode visitAppend(Append node, CalcitePlanContext context) {
    // 1. Resolve main plan
    visitChildren(node, context);

    // 2. Resolve subsearch plan
    UnresolvedPlan prunedSubSearch =
        node.getSubSearch().accept(new EmptySourcePropagateVisitor(), null);
    prunedSubSearch.accept(this, context);

    // 3. Merge two query schemas using shared logic
    RelNode subsearchNode = context.relBuilder.build();
    RelNode mainNode = context.relBuilder.build();
    return mergeTableAndResolveColumnConflict(mainNode, subsearchNode, context);
  }

  private RelNode mergeTableAndResolveColumnConflict(
      RelNode mainNode, RelNode subqueryNode, CalcitePlanContext context) {
    // Use shared schema merging logic that handles type conflicts via field renaming
    List<RelNode> nodesToMerge = Arrays.asList(mainNode, subqueryNode);
    List<RelNode> projectedNodes =
        SchemaUnifier.buildUnifiedSchemaWithConflictResolution(nodesToMerge, context);

    // 4. Union the projected plans
    for (RelNode projectedNode : projectedNodes) {
      context.relBuilder.push(projectedNode);
    }
    context.relBuilder.union(true);
    return context.relBuilder.peek();
  }

  @Override
  public RelNode visitMultisearch(Multisearch node, CalcitePlanContext context) {
    List<RelNode> subsearchNodes = new ArrayList<>();
    for (UnresolvedPlan subsearch : node.getSubsearches()) {
      UnresolvedPlan prunedSubSearch = subsearch.accept(new EmptySourcePropagateVisitor(), null);
      prunedSubSearch.accept(this, context);
      subsearchNodes.add(context.relBuilder.build());
    }

    // Use shared schema merging logic that handles type conflicts via field renaming
    List<RelNode> alignedNodes =
        SchemaUnifier.buildUnifiedSchemaWithConflictResolution(subsearchNodes, context);

    for (RelNode alignedNode : alignedNodes) {
      context.relBuilder.push(alignedNode);
    }
    context.relBuilder.union(true, alignedNodes.size());

    RelDataType rowType = context.relBuilder.peek().getRowType();
    String timestampField = findTimestampField(rowType);
    if (timestampField != null) {
      RelDataTypeField timestampFieldRef = rowType.getField(timestampField, false, false);
      if (timestampFieldRef != null) {
        RexNode timestampRef =
            context.rexBuilder.makeInputRef(
                context.relBuilder.peek(), timestampFieldRef.getIndex());
        context.relBuilder.sort(context.relBuilder.desc(timestampRef));
      }
    }

    return context.relBuilder.peek();
  }

  /**
   * Finds the @timestamp field for multisearch ordering. Only @timestamp field is used for
   * timestamp interleaving. Other timestamp-like fields are ignored.
   *
   * @param rowType The row type to search for @timestamp field
   * @return "@timestamp" if the field exists, or null if not found
   */
  private String findTimestampField(RelDataType rowType) {
    RelDataTypeField field = rowType.getField("@timestamp", false, false);
    if (field != null) {
      return "@timestamp";
    }
    return null;
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
    ArgumentMap argumentMap = ArgumentMap.of(node.getArguments());
    String countFieldName = (String) argumentMap.get(RareTopN.Option.countField.name()).getValue();
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

    // if usenull=false, add a isNotNull before Aggregate and the hint to this Aggregate
    Boolean bucketNullable = (Boolean) argumentMap.get(RareTopN.Option.useNull.name()).getValue();
    boolean hintIgnoreNullBucket = false;
    if (!bucketNullable && !groupExprList.isEmpty()) {
      hintIgnoreNullBucket = true;
      // add isNotNull filter before aggregation to filter out null bucket
      List<RexNode> groupByList =
          groupExprList.stream().map(expr -> rexVisitor.analyze(expr, context)).toList();
      context.relBuilder.filter(
          PlanUtils.getSelectColumns(groupByList).stream()
              .map(context.relBuilder::field)
              .map(context.relBuilder::isNotNull)
              .toList());
    }
    aggregateWithTrimming(groupExprList, aggExprList, context, hintIgnoreNullBucket);

    // 2. add count() column with sort direction
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
        context.relBuilder.alias(rowNumberWindowOver, ROW_NUMBER_COLUMN_FOR_RARE_TOP));

    // 3. filter row_number() <= k in each partition
    int k = node.getNoOfResults();
    context.relBuilder.filter(
        context.relBuilder.lessThanOrEqual(
            context.relBuilder.field(ROW_NUMBER_COLUMN_FOR_RARE_TOP),
            context.relBuilder.literal(k)));

    // 4. project final output. the default output is group by list + field list
    Boolean showCount = (Boolean) argumentMap.get(RareTopN.Option.showCount.name()).getValue();
    if (showCount) {
      context.relBuilder.projectExcept(context.relBuilder.field(ROW_NUMBER_COLUMN_FOR_RARE_TOP));
    } else {
      context.relBuilder.projectExcept(
          context.relBuilder.field(ROW_NUMBER_COLUMN_FOR_RARE_TOP),
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

  /** Helper method to get the function name for proper column naming */
  private String getAggFieldAlias(UnresolvedExpression aggregateFunction) {
    if (aggregateFunction instanceof Alias) {
      return ((Alias) aggregateFunction).getName();
    }
    if (!(aggregateFunction instanceof AggregateFunction)) {
      return "value";
    }

    AggregateFunction aggFunc = (AggregateFunction) aggregateFunction;
    String funcName = aggFunc.getFuncName().toLowerCase();
    List<UnresolvedExpression> args = new ArrayList<>();
    if (aggFunc.getField() != null) {
      args.add(aggFunc.getField());
    }
    if (aggFunc.getArgList() != null) {
      args.addAll(aggFunc.getArgList());
    }

    if (args.isEmpty() || funcName.equals("count")) {
      // Special case for count() to show as just "count" instead of "count(AllFields())"
      return "count";
    }

    // Build the full function call string like "avg(cpu_usage)"
    StringBuilder sb = new StringBuilder(funcName).append("(");
    for (int i = 0; i < args.size(); i++) {
      if (i > 0) sb.append(", ");
      if (args.get(i) instanceof Field) {
        sb.append(((Field) args.get(i)).getField().toString());
      } else {
        sb.append(args.get(i).toString());
      }
    }
    sb.append(")");
    return sb.toString();
  }

  /** Transforms visitAddColTotals command into SQL-based operations. */
  @Override
  public RelNode visitAddColTotals(AddColTotals node, CalcitePlanContext context) {
    visitChildren(node, context);

    // Parse options from the AddTotals node
    Map<String, Literal> options = node.getOptions();
    String label = getOptionValue(options, "label", "Total");
    String labelField = getOptionValue(options, "labelfield", null);
    // Determine which fields to aggregate

    // Handle row=true option: add a new field that sums all specified fields for each row
    List<Field> fieldsToAggregate = node.getFieldList();
    return buildAddRowTotalAggregate(
        context, fieldsToAggregate, false, true, null, labelField, label);
  }

  /**
   * Cast integer sum to long, real/float to double to avoid ClassCastException
   *
   * @param context
   * @param fieldRef
   * @param fieldDataType
   * @return
   */
  public RexNode getAggregateDataTypeFieldRef(
      CalcitePlanContext context, RexNode fieldRef, RelDataTypeField fieldDataType) {
    RexNode castFieldRef = fieldRef;
    if (fieldDataType.getType().getSqlTypeName() == SqlTypeName.INTEGER) {
      castFieldRef = context.relBuilder.cast(fieldRef, SqlTypeName.BIGINT);
    } else if ((fieldDataType.getType().getSqlTypeName() == SqlTypeName.FLOAT)
        || (fieldDataType.getType().getSqlTypeName() == SqlTypeName.REAL)) {
      castFieldRef = context.relBuilder.cast(fieldRef, SqlTypeName.DOUBLE);
    }

    return castFieldRef;
  }

  public RelNode buildAddRowTotalAggregate(
      CalcitePlanContext context,
      List<Field> fieldsToAggregate,
      boolean addTotalsForEachRow,
      boolean addTotalsForEachColumn,
      String newColTotalsFieldName,
      String labelField,
      String label) {

    // Build aggregation calls for totals calculation
    boolean extraColTotalField = false;
    RexNode sumExpression = null;
    List<AggCall> aggCalls = new ArrayList<>();
    List<String> fieldNameToSum = new ArrayList<>();
    RelNode originalData = context.relBuilder.peek();
    List<String> fieldNames = originalData.getRowType().getFieldNames();
    boolean foundLabelField = false;
    int labelLength =
        (labelField != null) && (labelField.length() > label.length())
            ? labelField.length()
            : label.length();

    RelDataType labelVarcharType =
        context.relBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, labelLength);

    // If no specific fields specified, use all numeric fields
    if (fieldsToAggregate.isEmpty()) {
      fieldsToAggregate = getAllNumericFields(originalData, context);
    }
    List<RexNode> orginalDataProjectedFields = new ArrayList<>();
    List<RexNode> fieldsToSum = new ArrayList<>();
    java.util.List<org.apache.calcite.rel.type.RelDataTypeField> fieldList =
        originalData.getRowType().getFieldList();
    for (RelDataTypeField fieldDataType : fieldList) {
      RexNode fieldRef = context.relBuilder.field(fieldDataType.getName());
      boolean columnAddedToNewProject = false;
      if (shouldAggregateField(fieldDataType.getName(), fieldsToAggregate)) {

        if (isNumericField(fieldRef, context)) {
          fieldsToSum.add(fieldRef);
          if (addTotalsForEachColumn) {
            // Cast integer sum to long/double for int/float types to avoid ClassCastException
            RexNode castFieldRef = getAggregateDataTypeFieldRef(context, fieldRef, fieldDataType);
            orginalDataProjectedFields.add(castFieldRef);
            columnAddedToNewProject = true;

            AggCall sumCall = context.relBuilder.sum(castFieldRef).as(fieldDataType.getName());
            aggCalls.add(sumCall);
          }
          fieldNameToSum.add(fieldDataType.getName());
          if (addTotalsForEachRow) {
            // Use cast field for row totals to avoid ClassCastException
            RexNode rowCastFieldRef =
                getAggregateDataTypeFieldRef(context, fieldRef, fieldDataType);

            if (sumExpression == null) {
              sumExpression = rowCastFieldRef;
            } else {
              sumExpression =
                  context.relBuilder.call(
                      org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS,
                      sumExpression,
                      rowCastFieldRef);
            }
          }
        }
      }
      if (!columnAddedToNewProject) {
        orginalDataProjectedFields.add(fieldRef);
      }
      if (addTotalsForEachColumn && fieldDataType.getName().equals(labelField)) {
        // Use specified label field for the label
        foundLabelField = true;
      }
    }
    context.relBuilder.project(orginalDataProjectedFields, fieldNames);
    if (addTotalsForEachRow && !fieldsToSum.isEmpty()) {
      // Add the new column with the sum
      context.relBuilder.projectPlus(
          context.relBuilder.alias(sumExpression, newColTotalsFieldName));
      if (newColTotalsFieldName.equals(labelField)) {
        foundLabelField = true;
      }
    }
    if (addTotalsForEachColumn) {
      if (!foundLabelField && (labelField != null)) {
        context.relBuilder.projectPlus(
            context.relBuilder.alias(
                context.relBuilder.getRexBuilder().makeNullLiteral(labelVarcharType), labelField));
        extraColTotalField = true;
      }
    }

    originalData = context.relBuilder.build();
    context.relBuilder.push(originalData);
    if (addTotalsForEachColumn) {
      // Perform aggregation (no group by - single totals row)
      context.relBuilder.aggregate(
          context.relBuilder.groupKey(), // Empty group key for single totals row
          aggCalls);
      // 3. Build the totals row with proper field order and labels
      List<RexNode> selectList = new ArrayList<>();

      fieldList = originalData.getRowType().getFieldList();
      for (RelDataTypeField fieldDataType : fieldList) {
        if (fieldNameToSum.contains(fieldDataType.getName())) {
          selectList.add(
              context.relBuilder.alias(
                  context.relBuilder.field(fieldDataType.getName()), fieldDataType.getName()));

        } else if (fieldDataType.getName().equals(labelField)
            && (extraColTotalField
                || fieldDataType.getType().getFamily() == SqlTypeFamily.CHARACTER)) {
          // Use specified label field for the label - cast to match original field type
          RexNode labelLiteral =
              context.relBuilder.getRexBuilder().makeLiteral(label, fieldDataType.getType(), true);
          selectList.add(context.relBuilder.alias(labelLiteral, fieldDataType.getName()));

        } else {
          // Other fields get NULL in totals row - cast to match original field type
          selectList.add(
              context.relBuilder.alias(
                  context.relBuilder.getRexBuilder().makeNullLiteral(fieldDataType.getType()),
                  fieldDataType.getName()));
        }
      }

      // Project the totals row with proper field order and labels
      context.relBuilder.project(selectList);
      RelNode totalsRow = context.relBuilder.build();
      // 4. Union original data with totals row
      context.relBuilder.push(originalData);
      context.relBuilder.push(totalsRow);
      context.relBuilder.union(true); // Use UNION ALL to preserve order
    }
    return context.relBuilder.peek();
  }

  /** Transforms visitAddTotals command into SQL-based operations. */
  @Override
  public RelNode visitAddTotals(AddTotals node, CalcitePlanContext context) {
    // 1. Process child plan first
    visitChildren(node, context);

    // Parse options from the AddTotals node
    Map<String, Literal> options = node.getOptions();
    String label =
        getOptionValue(
            options, "label", "Total"); // when col=true , add summary event with this label
    String labelField =
        getOptionValue(
            options,
            "labelfield",
            null); // when col=true , add summary event with this label field at the end of rows
    String newColTotalsFieldName =
        getOptionValue(
            options, "fieldname", "Total"); // when row=true , add new field as new column
    boolean addTotalsForEachRow = getBooleanOptionValue(options, "row", true);
    boolean addTotalsForEachColumn =
        getBooleanOptionValue(options, "col", false); // when col=true/false check

    // Determine which fields to aggregate
    List<Field> fieldsToAggregate = node.getFieldList();

    // Handle row=true option: add a new field that sums all specified fields for each row
    return buildAddRowTotalAggregate(
        context,
        fieldsToAggregate,
        addTotalsForEachRow,
        addTotalsForEachColumn,
        newColTotalsFieldName,
        labelField,
        label);
  }

  private String getOptionValue(Map<String, Literal> options, String key, String defaultValue) {
    Literal literal = options.get(key);
    if (literal == null) {
      return defaultValue;
    }
    Object value = literal.getValue();
    if (value == null) {
      return defaultValue;
    }
    return value.toString();
  }

  /** Helper method to extract boolean option values */
  private boolean getBooleanOptionValue(
      Map<String, Literal> options, String key, boolean defaultValue) {
    if (options.containsKey(key)) {
      Object value = options.get(key).getValue();
      if (value instanceof Boolean) {
        return (Boolean) value;
      }
      if (value instanceof String) {
        return Boolean.parseBoolean((String) value);
      }
    }
    return defaultValue;
  }

  /** Get all numeric fields from the RelNode */
  private List<Field> getAllNumericFields(RelNode relNode, CalcitePlanContext context) {
    List<Field> numericFields = new ArrayList<>();
    for (String fieldName : relNode.getRowType().getFieldNames()) {
      if (isNumericFieldName(fieldName, relNode)) {
        numericFields.add(
            new Field(new org.opensearch.sql.ast.expression.QualifiedName(fieldName)));
      }
    }
    return numericFields;
  }

  /** Check if a field should be aggregated based on the field list */
  private boolean shouldAggregateField(String fieldName, List<Field> fieldsToAggregate) {
    if (fieldsToAggregate.isEmpty()) {
      return true; // Aggregate all fields when none specified
    }
    return fieldsToAggregate.stream()
        .anyMatch(field -> field.getField().toString().equals(fieldName));
  }

  /** Check if a RexNode represents a numeric field */
  private boolean isNumericField(RexNode rexNode, CalcitePlanContext context) {
    return rexNode.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC;
  }

  /** Check if a field name represents a numeric field in the RelNode */
  private boolean isNumericFieldName(String fieldName, RelNode relNode) {
    try {
      RelDataTypeField field = relNode.getRowType().getField(fieldName, false, false);
      return field != null && field.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public RelNode visitChart(Chart node, CalcitePlanContext context) {
    visitChildren(node, context);
    ArgumentMap argMap = ArgumentMap.of(node.getArguments());
    ChartConfig config = ChartConfig.fromArguments(argMap);
    List<UnresolvedExpression> groupExprList =
        Stream.of(node.getRowSplit(), node.getColumnSplit()).filter(Objects::nonNull).toList();
    Aggregation aggregation =
        new Aggregation(
            List.of(node.getAggregationFunction()), List.of(), groupExprList, null, List.of());
    BitSet nonNullGroupMask = new BitSet(groupExprList.size());
    // Rows without a row-split are always ignored
    if (config.useNull) {
      nonNullGroupMask.set(0);
    } else {
      nonNullGroupMask.set(0, groupExprList.size());
    }
    visitAggregation(aggregation, context, nonNullGroupMask, false, true);
    RelBuilder relBuilder = context.relBuilder;

    // If a second split does not present or limit equals 0, we go no further for limit, nullstr,
    // otherstr parameters because all truncating & renaming is performed on the column split
    if (node.getRowSplit() == null
        || node.getColumnSplit() == null
        || Objects.equals(config.limit, 0)) {
      // The output of chart is expected to be ordered by row split names
      relBuilder.sort(relBuilder.field(0));
      return relBuilder.peek();
    }

    // Convert the column split to string if necessary: column split was supposed to be pivoted to
    // column names. This guarantees that its type compatibility with useother and usenull
    RexNode colSplit = relBuilder.field(1);
    String columnSplitName = relBuilder.peek().getRowType().getFieldNames().get(1);
    if (!SqlTypeUtil.isCharacter(colSplit.getType())) {
      colSplit =
          relBuilder.alias(
              context.rexBuilder.makeCast(
                  UserDefinedFunctionUtils.NULLABLE_STRING, colSplit, true, true),
              columnSplitName);
    }
    relBuilder.project(relBuilder.field(0), colSplit, relBuilder.field(2));
    RelNode aggregated = relBuilder.peek();
    // 1: column-split, 2: agg
    RelNode ranked = rankByColumnSplit(context, 1, 2, config.top);

    relBuilder.push(aggregated);
    relBuilder.push(ranked);

    // on column-split = group key
    relBuilder.join(
        JoinRelType.LEFT, relBuilder.equals(relBuilder.field(2, 0, 1), relBuilder.field(2, 1, 0)));

    RexNode colSplitPostJoin = relBuilder.field(1);
    RexNode lteCondition =
        relBuilder.call(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            relBuilder.field(PlanUtils.ROW_NUMBER_COLUMN_FOR_CHART),
            relBuilder.literal(config.limit));
    if (!config.useOther) {
      relBuilder.filter(lteCondition);
    }
    RexNode nullCondition = relBuilder.isNull(colSplitPostJoin);

    RexNode columnSplitExpr;
    if (config.useNull) {
      columnSplitExpr =
          relBuilder.call(
              SqlStdOperatorTable.CASE,
              nullCondition,
              relBuilder.literal(config.nullStr),
              lteCondition,
              relBuilder.field(1), // col split
              relBuilder.literal(config.otherStr));
    } else {
      columnSplitExpr =
          relBuilder.call(
              SqlStdOperatorTable.CASE,
              lteCondition,
              relBuilder.field(1),
              relBuilder.literal(config.otherStr));
    }

    String aggFieldName = relBuilder.peek().getRowType().getFieldNames().get(2);
    relBuilder.project(
        relBuilder.field(0),
        relBuilder.alias(columnSplitExpr, columnSplitName),
        relBuilder.field(2));
    relBuilder.aggregate(
        relBuilder.groupKey(relBuilder.field(0), relBuilder.field(1)),
        buildAggCall(
                context.relBuilder,
                getAggFunctionName(node.getAggregationFunction()),
                relBuilder.field(2))
            .as(aggFieldName));
    // The output of chart is expected to be ordered by row and column split names
    relBuilder.sort(relBuilder.field(0), relBuilder.field(1));
    return relBuilder.peek();
  }

  /**
   * Aggregate by column split then rank by grand total (summed value of each category). The output
   * is <code>[col-split, grand-total, row-number]</code>
   */
  private RelNode rankByColumnSplit(
      CalcitePlanContext context, int columnSplitOrdinal, int aggOrdinal, boolean top) {
    RelBuilder relBuilder = context.relBuilder;

    relBuilder.project(relBuilder.field(columnSplitOrdinal), relBuilder.field(aggOrdinal));
    // Make sure that rows who don't have a column split not interfere grand total calculation
    relBuilder.filter(relBuilder.isNotNull(relBuilder.field(0)));
    final String GRAND_TOTAL_COL = "__grand_total__";
    relBuilder.aggregate(
        relBuilder.groupKey(relBuilder.field(0)),
        // Top-K semantic: Retain categories whose summed values are among the greatest
        relBuilder.sum(relBuilder.field(1)).as(GRAND_TOTAL_COL)); // results: group key, agg calls
    RexNode grandTotal = relBuilder.field(GRAND_TOTAL_COL);
    // Apply sorting: keep the max values if top is set
    if (top) {
      grandTotal = relBuilder.desc(grandTotal);
    }
    // Always set it to null last so that nulls don't interfere with top / bottom calculation
    grandTotal = relBuilder.nullsLast(grandTotal);
    RexNode rowNum =
        PlanUtils.makeOver(
            context,
            BuiltinFunctionName.ROW_NUMBER,
            relBuilder.literal(1), // dummy expression for row number calculation
            List.of(),
            List.of(),
            List.of(grandTotal),
            WindowFrame.toCurrentRow());
    relBuilder.projectPlus(relBuilder.alias(rowNum, PlanUtils.ROW_NUMBER_COLUMN_FOR_CHART));
    return relBuilder.build();
  }

  /**
   * Aggregate a field based on a given built-in aggregation function name.
   *
   * <p>It is intended for secondary aggregations in timechart and chart commands. Using it
   * elsewhere may lead to unintended results. It handles explicitly only MIN, MAX, AVG, COUNT,
   * DISTINCT_COUNT, EARLIEST, and LATEST. It sums the results for the rest aggregation types,
   * assuming them to be accumulative.
   */
  private AggCall buildAggCall(RelBuilder relBuilder, String aggFunctionName, RexNode node) {
    BuiltinFunctionName aggFunction =
        BuiltinFunctionName.ofAggregation(aggFunctionName)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        StringUtils.format(
                            "Unrecognized aggregation function: %s", aggFunctionName)));
    return switch (aggFunction) {
      case MIN, EARLIEST -> relBuilder.min(node);
      case MAX, LATEST -> relBuilder.max(node);
      case AVG -> relBuilder.avg(node);
      default -> relBuilder.sum(node);
    };
  }

  private String getAggFunctionName(UnresolvedExpression aggregateFunction) {
    if (aggregateFunction instanceof Alias alias) {
      return getAggFunctionName(alias.getDelegated());
    }
    return ((AggregateFunction) aggregateFunction).getFuncName();
  }

  @AllArgsConstructor
  private static class ChartConfig {
    private final int limit;
    private final boolean top;
    private final boolean useOther;
    private final boolean useNull;
    private final String otherStr;
    private final String nullStr;

    static ChartConfig fromArguments(ArgumentMap argMap) {
      int limit = (Integer) argMap.getOrDefault("limit", Chart.DEFAULT_LIMIT).getValue();
      boolean top = (Boolean) argMap.getOrDefault("top", Chart.DEFAULT_TOP).getValue();
      boolean useOther =
          (Boolean) argMap.getOrDefault("useother", Chart.DEFAULT_USE_OTHER).getValue();
      boolean useNull = (Boolean) argMap.getOrDefault("usenull", Chart.DEFAULT_USE_NULL).getValue();
      String otherStr =
          (String) argMap.getOrDefault("otherstr", Chart.DEFAULT_OTHER_STR).getValue();
      String nullStr = (String) argMap.getOrDefault("nullstr", Chart.DEFAULT_NULL_STR).getValue();
      return new ChartConfig(limit, top, useOther, useNull, otherStr, nullStr);
    }
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

  @Override
  public RelNode visitValues(Values values, CalcitePlanContext context) {
    if (values.getValues() == null || values.getValues().isEmpty()) {
      context.relBuilder.values(context.relBuilder.getTypeFactory().builder().build());
      return context.relBuilder.peek();
    } else {
      throw new CalciteUnsupportedException("Explicit values node is unsupported in Calcite");
    }
  }

  @Override
  public RelNode visitReplace(Replace node, CalcitePlanContext context) {
    visitChildren(node, context);

    List<String> fieldNames = context.relBuilder.peek().getRowType().getFieldNames();

    // Create a set of field names to replace for quick lookup
    Set<String> fieldsToReplace =
        node.getFieldList().stream().map(f -> f.getField().toString()).collect(Collectors.toSet());

    // Validate that all fields to replace exist by calling field() on each
    // This leverages relBuilder.field()'s built-in validation which throws
    // IllegalArgumentException if any field doesn't exist
    for (String fieldToReplace : fieldsToReplace) {
      context.relBuilder.field(fieldToReplace);
    }

    List<RexNode> projectList = new ArrayList<>();

    // Project all fields, replacing specified ones in-place
    for (String fieldName : fieldNames) {
      if (fieldsToReplace.contains(fieldName)) {
        // Replace this field in-place with all pattern/replacement pairs applied sequentially
        RexNode fieldRef = context.relBuilder.field(fieldName);

        // Apply all replacement pairs sequentially (nested REPLACE calls)
        for (ReplacePair pair : node.getReplacePairs()) {
          RexNode patternNode = rexVisitor.analyze(pair.getPattern(), context);
          RexNode replacementNode = rexVisitor.analyze(pair.getReplacement(), context);

          String patternStr = pair.getPattern().getValue().toString();
          String replacementStr = pair.getReplacement().getValue().toString();

          if (patternStr.contains("*")) {
            WildcardUtils.validateWildcardSymmetry(patternStr, replacementStr);

            String regexPattern = WildcardUtils.convertWildcardPatternToRegex(patternStr);
            String regexReplacement =
                WildcardUtils.convertWildcardReplacementToRegex(replacementStr);

            RexNode regexPatternNode =
                context.rexBuilder.makeLiteral(
                    regexPattern,
                    context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                    true);
            RexNode regexReplacementNode =
                context.rexBuilder.makeLiteral(
                    regexReplacement,
                    context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                    true);

            fieldRef =
                context.rexBuilder.makeCall(
                    org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE_3,
                    fieldRef,
                    regexPatternNode,
                    regexReplacementNode);
          } else {
            fieldRef =
                context.relBuilder.call(
                    SqlStdOperatorTable.REPLACE, fieldRef, patternNode, replacementNode);
          }
        }

        projectList.add(fieldRef);
      } else {
        // Keep original field unchanged
        projectList.add(context.relBuilder.field(fieldName));
      }
    }

    context.relBuilder.project(projectList, fieldNames);
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
      rexNodeList =
          ArrayUtils.add(
              rexNodeList,
              context.rexBuilder.makeLiteral(
                  "<*>",
                  context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                  true));
    } else {
      rexNodeList =
          ArrayUtils.add(
              rexNodeList,
              context.rexBuilder.makeLiteral(
                  parseMethod.getName(),
                  context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                  true));
    }
    List<RexNode> newFields = new ArrayList<>();
    for (String groupCandidate : groupCandidates) {
      RexNode innerRex =
          PPLFuncImpTable.INSTANCE.resolve(
              context.rexBuilder, ParseUtils.BUILTIN_FUNCTION_MAP.get(parseMethod), rexNodeList);
      if (!ParseMethod.PATTERNS.equals(parseMethod)) {
        newFields.add(
            PPLFuncImpTable.INSTANCE.resolve(
                context.rexBuilder,
                BuiltinFunctionName.INTERNAL_ITEM,
                innerRex,
                context.rexBuilder.makeLiteral(
                    groupCandidate,
                    context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                    true)));
      } else {
        RexNode emptyString =
            context.rexBuilder.makeLiteral(
                "", context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), true);
        RexNode isEmptyCondition =
            context.rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, sourceField, emptyString);
        RexNode isNullCondition =
            context.rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, sourceField);
        // Calcite regexp_replace(string, string, string) doesn't accept empty string.
        // So use case when condition here to handle corner cases
        newFields.add(
            context.rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, // case
                isNullCondition,
                emptyString, // when field is NULL then ''
                isEmptyCondition,
                emptyString, // when field = '' then ''
                innerRex // else regexp_replace(field, regex, replace_string)
                ));
      }
    }
    projectPlusOverriding(newFields, groupCandidates, context);
  }

  /**
   * CALCITE-6981 introduced a stricter type checking for Array type in {@link RexToLixTranslator}.
   * We defined a MAP(VARCHAR, ANY) in {@link UserDefinedFunctionUtils#nullablePatternAggList}, when
   * we convert the value type to ArraySqlType, it will check the source data type by {@link
   * RelDataType#getComponentType()} which will return null due to the source type is ANY.
   */
  private RexNode explicitMapType(
      CalcitePlanContext context, RexNode origin, SqlTypeName targetType) {
    MapSqlType originalMapType = (MapSqlType) origin.getType();
    ArraySqlType newValueType =
        new ArraySqlType(context.rexBuilder.getTypeFactory().createSqlType(targetType), true);
    MapSqlType newMapType = new MapSqlType(originalMapType.getKeyType(), newValueType, true);
    return new RexInputRef(((RexInputRef) origin).getIndex(), newMapType);
  }

  private void flattenParsedPattern(
      String originalPatternResultAlias,
      RexNode parsedNode,
      CalcitePlanContext context,
      boolean flattenPatternAggResult,
      Boolean showNumberedToken) {
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
    if (flattenPatternAggResult) {
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
    if (showNumberedToken) {
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
    }
    if (flattenPatternAggResult) {
      RexNode sampleLogsExpr =
          context.rexBuilder.makeCast(
              context
                  .rexBuilder
                  .getTypeFactory()
                  .createArrayType(
                      context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), -1),
              PPLFuncImpTable.INSTANCE.resolve(
                  context.rexBuilder,
                  BuiltinFunctionName.INTERNAL_ITEM,
                  explicitMapType(context, parsedNode, SqlTypeName.VARCHAR),
                  context.rexBuilder.makeLiteral(PatternUtils.SAMPLE_LOGS)),
              true,
              true);
      fattenedNodes.add(context.relBuilder.alias(sampleLogsExpr, PatternUtils.SAMPLE_LOGS));
      projectNames.add(PatternUtils.SAMPLE_LOGS);
    }
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

  /** Creates an optimized sed call using native Calcite functions */
  private RexNode createOptimizedSedCall(
      RexNode fieldRex, String sedExpression, CalcitePlanContext context) {
    if (sedExpression.startsWith("s/")) {
      return createOptimizedSubstitution(fieldRex, sedExpression, context);
    } else if (sedExpression.startsWith("y/")) {
      return createOptimizedTransliteration(fieldRex, sedExpression, context);
    } else {
      throw new RuntimeException("Unsupported sed pattern: " + sedExpression);
    }
  }

  /** Creates optimized substitution calls for s/pattern/replacement/flags syntax. */
  private RexNode createOptimizedSubstitution(
      RexNode fieldRex, String sedExpression, CalcitePlanContext context) {
    try {
      // Parse sed substitution: s/pattern/replacement/flags
      if (!sedExpression.matches("s/.+/.*/.*")) {
        throw new IllegalArgumentException("Invalid sed substitution format");
      }

      // Find the delimiters - sed format is s/pattern/replacement/flags
      int firstDelimiter = sedExpression.indexOf('/', 2); // First '/' after 's/'
      int secondDelimiter = sedExpression.indexOf('/', firstDelimiter + 1); // Second '/'
      int thirdDelimiter = sedExpression.indexOf('/', secondDelimiter + 1); // Third '/' (optional)

      if (firstDelimiter == -1 || secondDelimiter == -1) {
        throw new IllegalArgumentException("Invalid sed substitution format");
      }

      String pattern = sedExpression.substring(2, firstDelimiter);
      String replacement = sedExpression.substring(firstDelimiter + 1, secondDelimiter);
      String flags =
          secondDelimiter + 1 < sedExpression.length()
              ? sedExpression.substring(secondDelimiter + 1)
              : "";

      // Convert sed backreferences (\1, \2) to Java style ($1, $2)
      String javaReplacement = replacement.replaceAll("\\\\(\\d+)", "\\$$1");

      if (flags.isEmpty()) {
        // 3-parameter REGEXP_REPLACE
        return PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder,
            BuiltinFunctionName.REPLACE,
            fieldRex,
            context.rexBuilder.makeLiteral(pattern),
            context.rexBuilder.makeLiteral(javaReplacement));
      } else if (flags.matches("[gi]+")) {
        // 4-parameter REGEXP_REPLACE with flags
        return PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder,
            BuiltinFunctionName.INTERNAL_REGEXP_REPLACE_PG_4,
            fieldRex,
            context.rexBuilder.makeLiteral(pattern),
            context.rexBuilder.makeLiteral(javaReplacement),
            context.rexBuilder.makeLiteral(flags));
      } else if (flags.matches("\\d+")) {
        // 5-parameter REGEXP_REPLACE with occurrence
        int occurrence = Integer.parseInt(flags);
        return PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder,
            BuiltinFunctionName.INTERNAL_REGEXP_REPLACE_5,
            fieldRex,
            context.rexBuilder.makeLiteral(pattern),
            context.rexBuilder.makeLiteral(javaReplacement),
            context.relBuilder.literal(1), // start position
            context.relBuilder.literal(occurrence));
      } else {
        throw new RuntimeException(
            "Unsupported sed flags: " + flags + " in expression: " + sedExpression);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to optimize sed expression: " + sedExpression, e);
    }
  }

  /** Creates optimized transliteration calls for y/from/to/ syntax. */
  private RexNode createOptimizedTransliteration(
      RexNode fieldRex, String sedExpression, CalcitePlanContext context) {
    try {
      // Parse sed transliteration: y/from/to/
      if (!sedExpression.matches("y/.+/.*/.*")) {
        throw new IllegalArgumentException("Invalid sed transliteration format");
      }

      int firstSlash = sedExpression.indexOf('/', 1);
      int secondSlash = sedExpression.indexOf('/', firstSlash + 1);
      int thirdSlash = sedExpression.indexOf('/', secondSlash + 1);

      if (firstSlash == -1 || secondSlash == -1) {
        throw new IllegalArgumentException("Invalid sed transliteration format");
      }

      String from = sedExpression.substring(firstSlash + 1, secondSlash);
      String to =
          sedExpression.substring(
              secondSlash + 1, thirdSlash != -1 ? thirdSlash : sedExpression.length());

      // Use Calcite's native TRANSLATE3 function
      return PPLFuncImpTable.INSTANCE.resolve(
          context.rexBuilder,
          BuiltinFunctionName.INTERNAL_TRANSLATE3,
          fieldRex,
          context.rexBuilder.makeLiteral(from),
          context.rexBuilder.makeLiteral(to));
    } catch (Exception e) {
      throw new RuntimeException("Failed to optimize sed expression: " + sedExpression, e);
    }
  }
}
