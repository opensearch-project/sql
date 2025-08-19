/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import static org.opensearch.sql.calcite.utils.PlanUtils.getRelation;
import static org.opensearch.sql.calcite.utils.PlanUtils.transformPlanToAttachChild;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.AllFieldsExcludeMeta;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Argument.ArgumentMap;
import org.opensearch.sql.ast.expression.Between;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Map;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.AppendCol;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.DescribeRelation;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Expand;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Flatten;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Patterns;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Reverse;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Window;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalDedupe;
import org.opensearch.sql.planner.logical.LogicalEval;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRareTopN;
import org.opensearch.sql.planner.logical.LogicalRemove;
import org.opensearch.sql.planner.logical.LogicalRename;
import org.opensearch.sql.planner.logical.LogicalSort;

/** Utility class to mask sensitive information in incoming PPL queries. */
public class PPLQueryDataAnonymizer extends AbstractNodeVisitor<String, String> {

  private static final String MASK_LITERAL = "***";

  private final AnonymizerExpressionAnalyzer expressionAnalyzer;
  private final Settings settings;

  public PPLQueryDataAnonymizer(Settings settings) {
    this.expressionAnalyzer = new AnonymizerExpressionAnalyzer(this);
    this.settings = settings;
  }

  /**
   * This method is used to anonymize sensitive data in PPL query. Sensitive data includes user
   * data.
   *
   * @return ppl query string with all user data replace with "***"
   */
  public String anonymizeData(UnresolvedPlan plan) {
    return plan.accept(this, null);
  }

  public String anonymizeStatement(Statement plan) {
    return plan.accept(this, null);
  }

  /** Handle Query Statement. */
  @Override
  public String visitQuery(Query node, String context) {
    return node.getPlan().accept(this, null);
  }

  @Override
  public String visitExplain(Explain node, String context) {
    return StringUtils.format(
        "explain %s %s",
        node.getFormat().name().toLowerCase(Locale.ROOT), node.getStatement().accept(this, null));
  }

  @Override
  public String visitRelation(Relation node, String context) {
    if (node instanceof DescribeRelation) {
      // remove the system table suffix
      String systemTable = node.getTableQualifiedName().toString();
      return StringUtils.format(
          "describe %s", systemTable.substring(0, systemTable.lastIndexOf('.')));
    }
    return StringUtils.format("source=%s", node.getTableQualifiedName().toString());
  }

  @Override
  public String visitJoin(Join node, String context) {
    String left = node.getLeft().accept(this, context);
    String rightTableOrSubquery = node.getRight().accept(this, context);
    String right =
        rightTableOrSubquery.startsWith("source=")
            ? rightTableOrSubquery.substring("source=".length())
            : rightTableOrSubquery;
    String joinType = node.getJoinType().name().toLowerCase(Locale.ROOT);
    String leftAlias = node.getLeftAlias().map(l -> " left = " + l).orElse("");
    String rightAlias = node.getRightAlias().map(r -> " right = " + r).orElse("");
    String condition =
        node.getJoinCondition().map(c -> expressionAnalyzer.analyze(c, context)).orElse("true");
    return StringUtils.format(
        "%s | %s join%s%s on %s %s", left, joinType, leftAlias, rightAlias, condition, right);
  }

  @Override
  public String visitLookup(Lookup node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String lookupTable = ((Relation) node.getLookupRelation()).getTableQualifiedName().toString();
    String mappingFields = formatFieldAlias(node.getMappingAliasMap());
    String strategy =
        node.getOutputAliasMap().isEmpty()
            ? ""
            : String.format(" %s ", node.getOutputStrategy().toString().toLowerCase());
    String outputFields = formatFieldAlias(node.getOutputAliasMap());
    return StringUtils.format(
        "%s | lookup %s %s%s%s", child, lookupTable, mappingFields, strategy, outputFields);
  }

  private String formatFieldAlias(java.util.Map<String, String> fieldMap) {
    return fieldMap.entrySet().stream()
        .map(
            entry ->
                Objects.equals(entry.getKey(), entry.getValue())
                    ? entry.getKey()
                    : StringUtils.format("%s as %s", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining(", "));
  }

  @Override
  public String visitSubqueryAlias(SubqueryAlias node, String context) {
    Node childNode = node.getChild().get(0);
    String child = childNode.accept(this, context);
    if (childNode instanceof Project project
        && project.getProjectList().get(0) instanceof AllFields) {
      childNode = childNode.getChild().get(0);
    }
    // add "[]" only if its child is not a root
    String format = childNode.getChild().isEmpty() ? "%s as %s" : "[ %s ] as %s";
    return StringUtils.format(format, child, node.getAlias());
  }

  @Override
  public String visitTableFunction(TableFunction node, String context) {
    String arguments =
        node.getArguments().stream()
            .map(
                unresolvedExpression ->
                    this.expressionAnalyzer.analyze(unresolvedExpression, context))
            .collect(Collectors.joining(","));
    return StringUtils.format("source=%s(%s)", node.getFunctionName().toString(), arguments);
  }

  @Override
  public String visitFilter(Filter node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String condition = visitExpression(node.getCondition());
    return StringUtils.format("%s | where %s", child, condition);
  }

  /** Build {@link LogicalRename}. */
  @Override
  public String visitRename(Rename node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    ImmutableMap.Builder<String, String> renameMapBuilder = new ImmutableMap.Builder<>();
    for (Map renameMap : node.getRenameList()) {
      renameMapBuilder.put(
          visitExpression(renameMap.getOrigin()),
          ((Field) renameMap.getTarget()).getField().toString());
    }
    String renames =
        renameMapBuilder.build().entrySet().stream()
            .map(entry -> StringUtils.format("%s as %s", entry.getKey(), entry.getValue()))
            .collect(Collectors.joining(","));
    return StringUtils.format("%s | rename %s", child, renames);
  }

  /** Build {@link LogicalAggregation}. */
  @Override
  public String visitAggregation(Aggregation node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    UnresolvedExpression span = node.getSpan();
    List<UnresolvedExpression> groupByExprList = new ArrayList<>();
    if (!Objects.isNull(span)) {
      groupByExprList.add(span);
    }
    groupByExprList.addAll(node.getGroupExprList());
    final String group = visitExpressionList(groupByExprList);
    return StringUtils.format(
        "%s | stats %s",
        child, String.join(" ", visitExpressionList(node.getAggExprList()), groupBy(group)).trim());
  }

  @Override
  public String visitWindow(Window node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    return StringUtils.format(
        "%s | eventstats %s",
        child, String.join(" ", visitExpressionList(node.getWindowFunctionList())).trim());
  }

  /** Build {@link LogicalRareTopN}. */
  @Override
  public String visitRareTopN(RareTopN node, String context) {
    final String child = node.getChild().get(0).accept(this, context);
    ArgumentMap arguments = ArgumentMap.of(node.getArguments());
    Integer noOfResults = (Integer) arguments.get("noOfResults").getValue();
    String countField = (String) arguments.get("countField").getValue();
    Boolean showCount = (Boolean) arguments.get("showCount").getValue();
    String fields = visitFieldList(node.getFields());
    String group = visitExpressionList(node.getGroupExprList());
    String options =
        isCalciteEnabled(settings)
            ? StringUtils.format("countield='%s' showcount=%s ", countField, showCount)
            : "";
    return StringUtils.format(
        "%s | %s %d %s%s",
        child,
        node.getCommandType().name().toLowerCase(),
        noOfResults,
        options,
        String.join(" ", fields, groupBy(group)).trim());
  }

  /** Build {@link LogicalProject} or {@link LogicalRemove} from {@link Field}. */
  @Override
  public String visitProject(Project node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String arg = "+";
    String fields = visitExpressionList(node.getProjectList());

    if (Strings.isNullOrEmpty(fields)) {
      return child;
    }

    if (node.hasArgument()) {
      Argument argument = node.getArgExprList().get(0);
      Boolean exclude = (Boolean) argument.getValue().getValue();
      if (exclude) {
        arg = "-";
      }
    }
    return StringUtils.format("%s | fields %s %s", child, arg, fields);
  }

  /** Build {@link LogicalEval}. */
  @Override
  public String visitEval(Eval node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    ImmutableList.Builder<Pair<String, String>> expressionsBuilder = new ImmutableList.Builder<>();
    for (Let let : node.getExpressionList()) {
      String expression = visitExpression(let.getExpression());
      String target = let.getVar().getField().toString();
      expressionsBuilder.add(ImmutablePair.of(target, expression));
    }
    String expressions =
        expressionsBuilder.build().stream()
            .map(pair -> StringUtils.format("%s" + "=%s", pair.getLeft(), pair.getRight()))
            .collect(Collectors.joining(" "));
    return StringUtils.format("%s | eval %s", child, expressions);
  }

  @Override
  public String visitExpand(Expand node, String context) {
    String child = node.getChild().getFirst().accept(this, context);
    String field = visitExpression(node.getField());

    return StringUtils.format("%s | expand %s", child, field);
  }

  /** Build {@link LogicalSort}. */
  @Override
  public String visitSort(Sort node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    // the first options is {"count": "integer"}
    String sortList = visitFieldList(node.getSortList());
    return StringUtils.format("%s | sort %s", child, sortList);
  }

  /** Build {@link LogicalDedupe}. */
  @Override
  public String visitDedupe(Dedupe node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String fields = visitFieldList(node.getFields());
    List<Argument> options = node.getOptions();
    Integer allowedDuplication = (Integer) options.get(0).getValue().getValue();
    Boolean keepEmpty = (Boolean) options.get(1).getValue().getValue();
    Boolean consecutive = (Boolean) options.get(2).getValue().getValue();

    return StringUtils.format(
        "%s | dedup %s %d keepempty=%b consecutive=%b",
        child, fields, allowedDuplication, keepEmpty, consecutive);
  }

  @Override
  public String visitHead(Head node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    Integer size = node.getSize();
    return StringUtils.format("%s | head %d", child, size);
  }

  @Override
  public String visitReverse(Reverse node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    return StringUtils.format("%s | reverse", child);
  }

  @Override
  public String visitParse(Parse node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String source = visitExpression(node.getSourceField());
    String regex = node.getPattern().toString();
    String commandName;

    switch (node.getParseMethod()) {
      case ParseMethod.PATTERNS:
        commandName = "patterns";
        break;
      case ParseMethod.GROK:
        commandName = "grok";
        break;
      default:
        commandName = "parse";
        break;
    }
    return ParseMethod.PATTERNS.equals(node.getParseMethod()) && regex.isEmpty()
        ? StringUtils.format("%s | %s %s", child, commandName, source)
        : StringUtils.format("%s | %s %s '%s'", child, commandName, source, regex);
  }

  @Override
  public String visitFlatten(Flatten node, String context) {
    String child = node.getChild().getFirst().accept(this, context);
    String field = visitExpression(node.getField());
    return StringUtils.format("%s | flatten %s", child, field);
  }

  @Override
  public String visitTrendline(Trendline node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String computations = visitExpressionList(node.getComputations(), " ");
    return StringUtils.format("%s | trendline %s", child, computations);
  }

  @Override
  public String visitAppendCol(AppendCol node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    UnresolvedPlan relation = getRelation(node);
    transformPlanToAttachChild(node.getSubSearch(), relation);
    String subsearch = anonymizeData(node.getSubSearch());
    String subsearchWithoutRelation = subsearch.substring(subsearch.indexOf("|") + 1);
    return StringUtils.format(
        "%s | appendcol override=%s [%s ]", child, node.isOverride(), subsearchWithoutRelation);
  }

  private String visitFieldList(List<Field> fieldList) {
    return fieldList.stream().map(this::visitExpression).collect(Collectors.joining(","));
  }

  private String visitExpressionList(List<? extends UnresolvedExpression> expressionList) {
    return visitExpressionList(expressionList, ",");
  }

  private String visitExpressionList(
      List<? extends UnresolvedExpression> expressionList, String delimiter) {
    return expressionList.isEmpty()
        ? ""
        : expressionList.stream().map(this::visitExpression).collect(Collectors.joining(delimiter));
  }

  private String visitExpression(UnresolvedExpression expression) {
    return expressionAnalyzer.analyze(expression, null);
  }

  @Override
  public String visitFillNull(FillNull node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    List<Pair<Field, UnresolvedExpression>> fieldFills = node.getReplacementPairs();
    if (fieldFills.isEmpty()) {
      return StringUtils.format("%s | fillnull with %s", child, MASK_LITERAL);
    }
    final UnresolvedExpression firstReplacement = fieldFills.getFirst().getRight();
    if (fieldFills.stream().allMatch(n -> firstReplacement == n.getRight())) {
      return StringUtils.format(
          "%s | fillnull with %s in %s",
          child,
          MASK_LITERAL,
          node.getReplacementPairs().stream()
              .map(n -> visitExpression(n.getLeft()))
              .collect(Collectors.joining(", ")));
    } else {
      return StringUtils.format(
          "%s | fillnull using %s",
          child,
          node.getReplacementPairs().stream()
              .map(n -> StringUtils.format("%s = %s", visitExpression(n.getLeft()), MASK_LITERAL))
              .collect(Collectors.joining(", ")));
    }
  }

  @Override
  public String visitPatterns(Patterns node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String sourceField = visitExpression(node.getSourceField());
    StringBuilder builder = new StringBuilder();
    builder.append(child).append(" | patterns ").append(sourceField);
    if (!node.getPartitionByList().isEmpty()) {
      String partitionByList = visitExpressionList(node.getPartitionByList());
      builder.append(" by ").append(partitionByList);
    }
    builder.append(" method=").append(node.getPatternMethod().toString());
    builder.append(" mode=").append(node.getPatternMode().toString());
    builder.append(" max_sample_count=").append(visitExpression(node.getPatternMaxSampleCount()));
    builder.append(" buffer_limit=").append(visitExpression(node.getPatternBufferLimit()));
    builder.append(" new_field=").append(node.getAlias());
    if (!node.getArguments().isEmpty()) {
      for (java.util.Map.Entry<String, Literal> entry : node.getArguments().entrySet()) {
        builder.append(
            String.format(
                Locale.ROOT, " %s=%s", entry.getKey(), visitExpression(entry.getValue())));
      }
    }

    return builder.toString();
  }

  private String groupBy(String groupBy) {
    return Strings.isNullOrEmpty(groupBy) ? "" : StringUtils.format("by %s", groupBy);
  }

  private boolean isCalciteEnabled(Settings settings) {
    if (settings != null) {
      return settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
    } else {
      return false;
    }
  }

  /** Expression Anonymizer. */
  private static class AnonymizerExpressionAnalyzer extends AbstractNodeVisitor<String, String> {
    private final PPLQueryDataAnonymizer queryAnonymizer;

    public AnonymizerExpressionAnalyzer(PPLQueryDataAnonymizer queryAnonymizer) {
      this.queryAnonymizer = queryAnonymizer;
    }

    public String analyze(UnresolvedExpression unresolved, String context) {
      return unresolved.accept(this, context);
    }

    @Override
    public String visitLiteral(Literal node, String context) {
      return MASK_LITERAL;
    }

    @Override
    public String visitInterval(Interval node, String context) {
      String value = node.getValue().accept(this, context);
      String unit = node.getUnit().name();
      return StringUtils.format("INTERVAL %s %s", value, unit);
    }

    @Override
    public String visitAnd(And node, String context) {
      String left = node.getLeft().accept(this, context);
      String right = node.getRight().accept(this, context);
      return StringUtils.format("%s and %s", left, right);
    }

    @Override
    public String visitOr(Or node, String context) {
      String left = node.getLeft().accept(this, context);
      String right = node.getRight().accept(this, context);
      return StringUtils.format("%s or %s", left, right);
    }

    @Override
    public String visitXor(Xor node, String context) {
      String left = node.getLeft().accept(this, context);
      String right = node.getRight().accept(this, context);
      return StringUtils.format("%s xor %s", left, right);
    }

    @Override
    public String visitNot(Not node, String context) {
      String expr = node.getExpression().accept(this, context);
      return StringUtils.format("not %s", expr);
    }

    @Override
    public String visitAggregateFunction(AggregateFunction node, String context) {
      String arg = node.getField().accept(this, context);
      return StringUtils.format("%s(%s)", node.getFuncName(), arg);
    }

    @Override
    public String visitSpan(Span node, String context) {
      String field = analyze(node.getField(), context);
      String value = analyze(node.getValue(), context);
      return StringUtils.format("span(%s, %s %s)", field, value, node.getUnit().getName());
    }

    @Override
    public String visitFunction(Function node, String context) {
      String arguments =
          node.getFuncArgs().stream()
              .map(unresolvedExpression -> analyze(unresolvedExpression, context))
              .collect(Collectors.joining(","));
      return StringUtils.format("%s(%s)", node.getFuncName(), arguments);
    }

    @Override
    public String visitWindowFunction(WindowFunction node, String context) {
      String function = analyze(node.getFunction(), context);
      String partitions =
          node.getPartitionByList().stream()
              .map(p -> analyze(p, context))
              .collect(Collectors.joining(","));
      if (partitions.isEmpty()) {
        return StringUtils.format("%s", function);
      } else {
        return StringUtils.format("%s by %s", function, partitions);
      }
    }

    @Override
    public String visitCompare(Compare node, String context) {
      String left = analyze(node.getLeft(), context);
      String right = analyze(node.getRight(), context);
      return StringUtils.format("%s %s %s", left, node.getOperator(), right);
    }

    @Override
    public String visitBetween(Between node, String context) {
      String value = analyze(node.getValue(), context);
      String left = analyze(node.getLowerBound(), context);
      String right = analyze(node.getUpperBound(), context);
      return StringUtils.format("%s between %s and %s", value, left, right);
    }

    @Override
    public String visitIn(In node, String context) {
      String field = analyze(node.getField(), context);
      return StringUtils.format("%s in (%s)", field, MASK_LITERAL);
    }

    @Override
    public String visitField(Field node, String context) {
      return node.getField().toString();
    }

    @Override
    public String visitAllFields(AllFields node, String context) {
      return "";
    }

    @Override
    public String visitAllFieldsExcludeMeta(AllFieldsExcludeMeta node, String context) {
      return "";
    }

    @Override
    public String visitAlias(Alias node, String context) {
      String expr = node.getDelegated().accept(this, context);
      return StringUtils.format("%s", expr);
    }

    @Override
    public String visitTrendlineComputation(Trendline.TrendlineComputation node, String context) {
      final String dataField = node.getDataField().accept(this, context);
      final String aliasClause = " as " + node.getAlias();
      final String computationType = node.getComputationType().name().toLowerCase(Locale.ROOT);
      return StringUtils.format(
          "%s(%d, %s)%s", computationType, node.getNumberOfDataPoints(), dataField, aliasClause);
    }

    @Override
    public String visitInSubquery(InSubquery node, String context) {
      String nodes =
          node.getChild().stream().map(c -> analyze(c, context)).collect(Collectors.joining(","));
      String subquery = queryAnonymizer.anonymizeData(node.getQuery());
      return StringUtils.format("(%s) in [ %s ]", nodes, subquery);
    }

    @Override
    public String visitScalarSubquery(ScalarSubquery node, String context) {
      String subquery = queryAnonymizer.anonymizeData(node.getQuery());
      return StringUtils.format("[ %s ]", subquery);
    }

    @Override
    public String visitExistsSubquery(ExistsSubquery node, String context) {
      String subquery = queryAnonymizer.anonymizeData(node.getQuery());
      return StringUtils.format("exists [ %s ]", subquery);
    }

    @Override
    public String visitCase(Case node, String context) {
      StringBuilder builder = new StringBuilder();
      builder.append("cast(");
      for (When when : node.getWhenClauses()) {
        builder.append(analyze(when.getCondition(), context));
        builder.append(",");
        builder.append(analyze(when.getResult(), context));
        builder.append(",");
      }
      builder.deleteCharAt(builder.lastIndexOf(","));
      node.getElseClause()
          .ifPresent(
              elseClause -> {
                builder.append(" else ");
                builder.append(analyze(elseClause, context));
              });
      builder.append(")");
      return builder.toString();
    }

    @Override
    public String visitCast(Cast node, String context) {
      String expr = analyze(node.getExpression(), context);
      return StringUtils.format("cast(%s as %s)", expr, node.getConvertedType().toString());
    }
  }
}
