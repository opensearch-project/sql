/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Between;
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
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
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

  public PPLQueryDataAnonymizer() {
    this.expressionAnalyzer = new AnonymizerExpressionAnalyzer(this);
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
    return node.getStatement().accept(this, null);
  }

  @Override
  public String visitRelation(Relation node, String context) {
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
    String child = node.getChild().get(0).accept(this, context);
    if (node.getChild().get(0).getChild().isEmpty()) {
      return StringUtils.format("%s as %s", child, node.getAlias());
    } else {
      // add "[]" only if its child is not a root
      return StringUtils.format("[ %s ] as %s", child, node.getAlias());
    }
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
    final String group = visitExpressionList(node.getGroupExprList());
    return StringUtils.format(
        "%s | stats %s",
        child, String.join(" ", visitExpressionList(node.getAggExprList()), groupBy(group)).trim());
  }

  /** Build {@link LogicalRareTopN}. */
  @Override
  public String visitRareTopN(RareTopN node, String context) {
    final String child = node.getChild().get(0).accept(this, context);
    List<Argument> options = node.getNoOfResults();
    Integer noOfResults = (Integer) options.get(0).getValue().getValue();
    String fields = visitFieldList(node.getFields());
    String group = visitExpressionList(node.getGroupExprList());
    return StringUtils.format(
        "%s | %s %d %s",
        child,
        node.getCommandType().name().toLowerCase(),
        noOfResults,
        String.join(" ", fields, groupBy(group)).trim());
  }

  /** Build {@link LogicalProject} or {@link LogicalRemove} from {@link Field}. */
  @Override
  public String visitProject(Project node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String arg = "+";
    String fields = visitExpressionList(node.getProjectList());

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
  public String visitTrendline(Trendline node, String context) {
    String child = node.getChild().get(0).accept(this, context);
    String computations = visitExpressionList(node.getComputations(), " ");
    return StringUtils.format("%s | trendline %s", child, computations);
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
    List<FillNull.NullableFieldFill> fieldFills = node.getNullableFieldFills();
    final UnresolvedExpression firstReplacement = fieldFills.getFirst().getReplaceNullWithMe();
    if (fieldFills.stream().allMatch(n -> firstReplacement == n.getReplaceNullWithMe())) {
      return StringUtils.format(
          "%s | fillnull with %s in %s",
          child,
          firstReplacement,
          node.getNullableFieldFills().stream()
              .map(n -> visitExpression(n.getNullableFieldReference()))
              .collect(Collectors.joining(", ")));
    } else {
      return StringUtils.format(
          "%s | fillnull using %s",
          child,
          node.getNullableFieldFills().stream()
              .map(
                  n ->
                      StringUtils.format(
                          "%s = %s",
                          visitExpression(n.getNullableFieldReference()), n.getReplaceNullWithMe()))
              .collect(Collectors.joining(", ")));
    }
  }

  private String groupBy(String groupBy) {
    return Strings.isNullOrEmpty(groupBy) ? "" : StringUtils.format("by %s", groupBy);
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
    public String visitFunction(Function node, String context) {
      String arguments =
          node.getFuncArgs().stream()
              .map(unresolvedExpression -> analyze(unresolvedExpression, context))
              .collect(Collectors.joining(","));
      return StringUtils.format("%s(%s)", node.getFuncName(), arguments);
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
    public String visitCast(Cast node, String context) {
      String expr = analyze(node.getExpression(), context);
      return StringUtils.format("cast(%s as %s)", expr, node.getDataType().getCoreType().name());
    }
  }
}
