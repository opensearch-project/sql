/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.analysis;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.AstNodeUtils;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.PatternMode;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.AddColTotals;
import org.opensearch.sql.ast.tree.AddTotals;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Append;
import org.opensearch.sql.ast.tree.AppendCol;
import org.opensearch.sql.ast.tree.AppendPipe;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.ast.tree.Chart;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Expand;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Flatten;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Multisearch;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Patterns;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Regex;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Replace;
import org.opensearch.sql.ast.tree.Reverse;
import org.opensearch.sql.ast.tree.Rex;
import org.opensearch.sql.ast.tree.SPath;
import org.opensearch.sql.ast.tree.Search;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.StreamWindow;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Values;
import org.opensearch.sql.ast.tree.Window;
import org.opensearch.sql.calcite.utils.WildcardUtils;
import org.opensearch.sql.common.patterns.PatternUtils;
import org.opensearch.sql.expression.parse.PatternsExpression;
import org.opensearch.sql.expression.parse.RegexCommonUtils;

/**
 * Visitor to analyze and collect required fields from PPL AST using stack-based traversal. The
 * result is used to support `spath` command (Field Resolution-based Extraction) and schema-on-read
 * support in the future.
 */
public class FieldResolutionVisitor extends AbstractNodeVisitor<Node, FieldResolutionContext> {

  /**
   * Analyzes PPL query plan to determine required fields at each node.
   *
   * @param plan root node of the PPL query plan
   * @return map of plan nodes to their field requirements (regular fields and wildcard patterns)
   * @throws IllegalArgumentException if plan contains unsupported commands or spath with wildcards
   */
  public Map<UnresolvedPlan, FieldResolutionResult> analyze(UnresolvedPlan plan) {
    FieldResolutionContext context = new FieldResolutionContext();
    acceptAndVerifyNodeVisited(plan, context);
    return context.getResults();
  }

  @Override
  public Node visitChildren(Node node, FieldResolutionContext context) {
    for (Node child : node.getChild()) {
      acceptAndVerifyNodeVisited(child, context);
    }
    return null;
  }

  /**
   * Visit node and verify it returns same node. This ensures all the visit methods are implemented
   * in this class.
   */
  private void acceptAndVerifyNodeVisited(Node node, FieldResolutionContext context) {
    Node result = node.accept(this, context);
    if (result != node) {
      throw new IllegalArgumentException(
          "Unsupported command for field resolution: " + node.getClass().getSimpleName());
    }
  }

  @Override
  public Node visitProject(Project node, FieldResolutionContext context) {
    boolean isSelectAll =
        node.getProjectList().stream().anyMatch(expr -> expr instanceof AllFields);

    if (isSelectAll) {
      visitChildren(node, context);
    } else {
      Set<String> projectFields = new HashSet<>();
      Set<String> wildcardPatterns = new HashSet<>();
      for (UnresolvedExpression expr : node.getProjectList()) {
        extractFieldsFromExpression(expr)
            .forEach(
                field -> {
                  if (WildcardUtils.containsWildcard(field)) {
                    wildcardPatterns.add(field);
                  } else {
                    projectFields.add(field);
                  }
                });
      }

      FieldResolutionResult current = context.getCurrentRequirements();
      context.pushRequirements(
          current.and(new FieldResolutionResult(projectFields, wildcardPatterns)));
      visitChildren(node, context);
      context.popRequirements();
    }
    return node;
  }

  @Override
  public Node visitFilter(Filter node, FieldResolutionContext context) {
    Set<String> filterFields = extractFieldsFromExpression(node.getCondition());
    if (AstNodeUtils.containsSubqueryExpression(node.getCondition())) {
      // Does not support subquery as we cannot distinguish correl variable without static schema
      throw new IllegalArgumentException(
          "Filter by subquery is not supported with field resolution.");
    }

    context.pushRequirements(context.getCurrentRequirements().or(filterFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  @Override
  public Node visitAggregation(Aggregation node, FieldResolutionContext context) {
    Set<String> aggFields = new HashSet<>();
    for (UnresolvedExpression groupExpr : node.getGroupExprList()) {
      aggFields.addAll(extractFieldsFromExpression(groupExpr));
    }
    if (node.getSpan() != null) {
      aggFields.addAll(extractFieldsFromExpression(node.getSpan()));
    }
    for (UnresolvedExpression aggExpr : node.getAggExprList()) {
      aggFields.addAll(extractFieldsFromAggregation(aggExpr));
    }

    context.pushRequirements(new FieldResolutionResult(aggFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  @Override
  public Node visitSpath(SPath node, FieldResolutionContext context) {
    if (node.getPath() != null) {
      return visitEval(node.rewriteAsEval(), context);
    } else {
      // set requirements for spath command;
      context.setResult(node, context.getCurrentRequirements());
      FieldResolutionResult requirements = context.getCurrentRequirements();
      if (requirements.hasWildcards()) {
        throw new IllegalArgumentException(
            "Spath command cannot extract arbitrary fields. Please project fields explicitly by"
                + " fields command without wildcard or stats command.");
      }

      context.pushRequirements(context.getCurrentRequirements().or(Set.of(node.getInField())));
      visitChildren(node, context);
      context.popRequirements();
      return node;
    }
  }

  @Override
  public Node visitSort(Sort node, FieldResolutionContext context) {
    Set<String> sortFields = new HashSet<>();
    for (Field sortField : node.getSortList()) {
      sortFields.addAll(extractFieldsFromExpression(sortField));
    }

    context.pushRequirements(context.getCurrentRequirements().or(sortFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  @Override
  public Node visitEval(Eval node, FieldResolutionContext context) {
    Set<String> evalInputFields = new HashSet<>();
    Set<String> computedFields = new HashSet<>();

    for (Let letExpr : node.getExpressionList()) {
      evalInputFields.addAll(extractFieldsFromExpression(letExpr.getExpression()));
      computedFields.add(letExpr.getVar().getField().toString());
    }

    FieldResolutionResult currentReq = context.getCurrentRequirements();
    Set<String> allRequiredFields = new HashSet<>(currentReq.getRegularFields());
    allRequiredFields.removeAll(computedFields);
    allRequiredFields.addAll(evalInputFields);

    context.pushRequirements(
        new FieldResolutionResult(allRequiredFields, currentReq.getWildcard()));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  private Set<String> extractFieldsFromExpression(UnresolvedExpression expr) {
    Set<String> fields = new HashSet<>();
    if (expr == null) {
      return fields;
    }

    if (expr instanceof Field field) {
      fields.add(field.getField().toString());
    } else if (expr instanceof QualifiedName name) {
      fields.add(name.toString());
    } else if (expr instanceof Alias alias) {
      fields.addAll(extractFieldsFromExpression(alias.getDelegated()));
    } else if (expr instanceof Function function) {
      for (UnresolvedExpression arg : function.getFuncArgs()) {
        fields.addAll(extractFieldsFromExpression(arg));
      }
    } else if (expr instanceof Span span) {
      fields.addAll(extractFieldsFromExpression(span.getField()));
    } else if (expr instanceof Literal) {
      return fields;
    } else {
      for (Node child : expr.getChild()) {
        if (child instanceof UnresolvedExpression childExpr) {
          fields.addAll(extractFieldsFromExpression(childExpr));
        }
      }
    }
    return fields;
  }

  @Override
  public Node visitJoin(Join node, FieldResolutionContext context) {
    Set<String> joinFields = new HashSet<>();

    if (node.getJoinCondition().isPresent()) {
      joinFields.addAll(extractFieldsFromExpression(node.getJoinCondition().get()));
    }

    if (node.getJoinFields().isPresent()) {
      for (Field field : node.getJoinFields().get()) {
        joinFields.addAll(extractFieldsFromExpression(field));
      }
    }

    FieldResolutionResult currentReq = context.getCurrentRequirements();
    Set<String> baseRequiredFields = new HashSet<>(currentReq.getRegularFields());

    String leftAlias = node.getLeftAlias().orElse(null);
    String rightAlias = node.getRightAlias().orElse(null);

    Set<String> leftFields = collectFieldsByAlias(baseRequiredFields, leftAlias, rightAlias);
    leftFields.addAll(collectFieldsByAlias(joinFields, leftAlias, rightAlias));

    Set<String> rightFields = collectFieldsByAlias(baseRequiredFields, rightAlias, leftAlias);
    rightFields.addAll(collectFieldsByAlias(joinFields, rightAlias, leftAlias));

    if (node.getLeft() != null) {
      context.pushRequirements(new FieldResolutionResult(leftFields, currentReq.getWildcard()));
      node.getLeft().accept(this, context);
      context.popRequirements();
    }

    if (node.getRight() != null) {
      context.pushRequirements(new FieldResolutionResult(rightFields, currentReq.getWildcard()));
      node.getRight().accept(this, context);
      context.popRequirements();
    }

    return node;
  }

  /**
   * Return lambda which remove alias from the input field, do nothing if the input does not start
   * from the alias.
   */
  private static UnaryOperator<String> removeAlias(String alias) {
    return (field) -> hasAlias(field, alias) ? field.substring(alias.length() + 1) : field;
  }

  /** Return predicate to exclude the field which has the alias. */
  private static Predicate<String> excludeAlias(String alias) {
    return (field) -> !hasAlias(field, alias);
  }

  private Set<String> collectFieldsByAlias(Set<String> fields, String alias, String excludedAlias) {
    return fields.stream()
        .filter(excludeAlias(excludedAlias))
        .map(removeAlias(alias))
        .collect(Collectors.toSet());
  }

  private static boolean hasAlias(String field, String alias) {
    return alias != null && field.startsWith(alias + ".");
  }

  @Override
  public Node visitSubqueryAlias(SubqueryAlias node, FieldResolutionContext context) {
    visitChildren(node, context);
    return node;
  }

  @Override
  public Node visitRelation(Relation node, FieldResolutionContext context) {
    FieldResolutionResult currentReq = context.getCurrentRequirements();

    context.setResult(
        node, new FieldResolutionResult(currentReq.getRegularFields(), currentReq.getWildcard()));
    return node;
  }

  // Commands that don't modify field requirements - just pass through to children
  @Override
  public Node visitSearch(Search node, FieldResolutionContext context) {
    visitChildren(node, context);
    return node;
  }

  @Override
  public Node visitAppendPipe(AppendPipe node, FieldResolutionContext context) {
    visitChildren(node, context);
    return node;
  }

  @Override
  public Node visitRegex(Regex node, FieldResolutionContext context) {
    Set<String> regexFields = extractFieldsFromExpression(node.getField());
    context.pushRequirements(context.getCurrentRequirements().or(regexFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  @Override
  public Node visitRex(Rex node, FieldResolutionContext context) {
    Set<String> rexFields = extractFieldsFromExpression(node.getField());
    String patternStr = (String) node.getPattern().getValue();
    List<String> namedGroups = RegexCommonUtils.getNamedGroupCandidates(patternStr);

    context.pushRequirements(context.getCurrentRequirements().exclude(namedGroups).or(rexFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  @Override
  public Node visitBin(Bin node, FieldResolutionContext context) {
    Set<String> binFields = extractFieldsFromExpression(node.getField());
    context.pushRequirements(context.getCurrentRequirements().or(binFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  @Override
  public Node visitParse(Parse node, FieldResolutionContext context) {
    Set<String> parseFields = extractFieldsFromExpression(node.getSourceField());
    context.pushRequirements(context.getCurrentRequirements().or(parseFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  @Override
  public Node visitPatterns(Patterns node, FieldResolutionContext context) {
    Set<String> patternFields = extractFieldsFromExpression(node.getSourceField());
    for (UnresolvedExpression partitionBy : node.getPartitionByList()) {
      patternFields.addAll(extractFieldsFromExpression(partitionBy));
    }
    Set<String> addedFields = new HashSet<>();
    addedFields.add(
        node.getAlias() != null ? node.getAlias() : PatternsExpression.DEFAULT_NEW_FIELD);
    if (node.getPatternMode() == PatternMode.AGGREGATION) {
      addedFields.add(PatternUtils.PATTERN_COUNT);
      addedFields.add(PatternUtils.SAMPLE_LOGS);
    }
    if (node.getShowNumberedToken() != null) {
      boolean showNumberedToken = Boolean.parseBoolean(node.getShowNumberedToken().toString());
      if (showNumberedToken) {
        addedFields.add(PatternUtils.TOKENS);
      }
    }

    context.pushRequirements(
        context.getCurrentRequirements().exclude(addedFields).or(patternFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  @Override
  public Node visitReverse(Reverse node, FieldResolutionContext context) {
    visitChildren(node, context);
    return node;
  }

  @Override
  public Node visitHead(Head node, FieldResolutionContext context) {
    visitChildren(node, context);
    return node;
  }

  @Override
  public Node visitRename(Rename node, FieldResolutionContext context) {
    visitChildren(node, context);
    return node;
  }

  @Override
  public Node visitDedupe(Dedupe node, FieldResolutionContext context) {
    Set<String> dedupeFields = new HashSet<>();
    for (Field field : node.getFields()) {
      dedupeFields.addAll(extractFieldsFromExpression(field));
    }
    context.pushRequirements(context.getCurrentRequirements().or(dedupeFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  @Override
  public Node visitWindow(Window node, FieldResolutionContext context) {
    Set<String> windowFields = new HashSet<>();
    for (UnresolvedExpression windowFunc : node.getWindowFunctionList()) {
      windowFields.addAll(extractFieldsFromExpression(windowFunc));
    }
    if (node.getGroupList() != null) {
      for (UnresolvedExpression groupExpr : node.getGroupList()) {
        windowFields.addAll(extractFieldsFromExpression(groupExpr));
      }
    }
    context.pushRequirements(context.getCurrentRequirements().or(windowFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  @Override
  public Node visitStreamWindow(StreamWindow node, FieldResolutionContext context) {
    Set<String> streamWindowFields = new HashSet<>();
    for (UnresolvedExpression windowFunc : node.getWindowFunctionList()) {
      streamWindowFields.addAll(extractFieldsFromExpression(windowFunc));
    }
    if (node.getGroupList() != null) {
      for (UnresolvedExpression groupExpr : node.getGroupList()) {
        streamWindowFields.addAll(extractFieldsFromExpression(groupExpr));
      }
    }
    if (node.getResetBefore() != null) {
      streamWindowFields.addAll(extractFieldsFromExpression(node.getResetBefore()));
    }
    if (node.getResetAfter() != null) {
      streamWindowFields.addAll(extractFieldsFromExpression(node.getResetAfter()));
    }
    context.pushRequirements(context.getCurrentRequirements().or(streamWindowFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  @Override
  public Node visitFillNull(FillNull node, FieldResolutionContext context) {
    visitChildren(node, context);
    return node;
  }

  @Override
  public Node visitAppendCol(AppendCol node, FieldResolutionContext context) {
    visitChildren(node, context);
    return node;
  }

  @Override
  public Node visitAppend(Append node, FieldResolutionContext context) {
    visitChildren(node, context);
    return node;
  }

  @Override
  public Node visitMultisearch(Multisearch node, FieldResolutionContext context) {
    visitChildren(node, context);
    return node;
  }

  @Override
  public Node visitLookup(Lookup node, FieldResolutionContext context) {
    visitChildren(node, context);
    return node;
  }

  @Override
  public Node visitValues(Values node, FieldResolutionContext context) {
    visitChildren(node, context);
    return node;
  }

  @Override
  public Node visitReplace(Replace node, FieldResolutionContext context) {
    visitChildren(node, context);
    return node;
  }

  @Override
  public Node visitFlatten(Flatten node, FieldResolutionContext context) {
    Set<String> flattenFields = extractFieldsFromExpression(node.getField());
    context.pushRequirements(context.getCurrentRequirements().or(flattenFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  @Override
  public Node visitTrendline(Trendline node, FieldResolutionContext context) {
    Set<String> trendlineFields = new HashSet<>();
    for (Trendline.TrendlineComputation computation : node.getComputations()) {
      trendlineFields.addAll(extractFieldsFromExpression(computation.getDataField()));
    }
    if (node.getSortByField().isPresent()) {
      trendlineFields.addAll(extractFieldsFromExpression(node.getSortByField().get()));
    }
    context.pushRequirements(context.getCurrentRequirements().or(trendlineFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  @Override
  public Node visitChart(Chart node, FieldResolutionContext context) {
    Set<String> chartFields = extractFieldsFromAggregation(node.getAggregationFunction());
    if (node.getRowSplit() != null) {
      chartFields.addAll(extractFieldsFromExpression(node.getRowSplit()));
    }
    if (node.getColumnSplit() != null) {
      chartFields.addAll(extractFieldsFromExpression(node.getColumnSplit()));
    }
    context.pushRequirements(new FieldResolutionResult(chartFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  @Override
  public Node visitRareTopN(RareTopN node, FieldResolutionContext context) {
    Set<String> rareTopNFields = new HashSet<>();
    for (Field field : node.getFields()) {
      rareTopNFields.addAll(extractFieldsFromExpression(field));
    }
    for (UnresolvedExpression groupExpr : node.getGroupExprList()) {
      rareTopNFields.addAll(extractFieldsFromExpression(groupExpr));
    }
    context.pushRequirements(new FieldResolutionResult(rareTopNFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  @Override
  public Node visitAddTotals(AddTotals node, FieldResolutionContext context) {
    visitChildren(node, context);
    return node;
  }

  @Override
  public Node visitAddColTotals(AddColTotals node, FieldResolutionContext context) {
    visitChildren(node, context);
    return node;
  }

  @Override
  public Node visitExpand(Expand node, FieldResolutionContext context) {
    Set<String> expandFields = extractFieldsFromExpression(node.getField());
    context.pushRequirements(context.getCurrentRequirements().or(expandFields));
    visitChildren(node, context);
    context.popRequirements();
    return node;
  }

  private Set<String> extractFieldsFromAggregation(UnresolvedExpression expr) {
    Set<String> fields = new HashSet<>();
    if (expr instanceof Alias alias) {
      return extractFieldsFromAggregation(alias.getDelegated());
    } else if (expr instanceof AggregateFunction aggFunc) {
      if (aggFunc.getField() != null) {
        fields.addAll(extractFieldsFromExpression(aggFunc.getField()));
      }
      if (aggFunc.getArgList() != null) {
        for (UnresolvedExpression arg : aggFunc.getArgList()) {
          fields.addAll(extractFieldsFromExpression(arg));
        }
      }
    }
    return fields;
  }
}
