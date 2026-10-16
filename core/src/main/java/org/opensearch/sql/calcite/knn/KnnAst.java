/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.knn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.ast.KnnStep;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.KnnFunction;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Append;
import org.opensearch.sql.ast.tree.AppendCol;
import org.opensearch.sql.ast.tree.AppendPipe;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.ast.tree.Chart;
import org.opensearch.sql.ast.tree.Convert;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Foreach;
import org.opensearch.sql.ast.tree.GraphLookup;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Knn;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Patterns;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Regex;
import org.opensearch.sql.ast.tree.RelationSubquery;
import org.opensearch.sql.ast.tree.Rex;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.StreamWindow;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Window;
import org.opensearch.sql.ast.tree.Xyseries;

/**
 * AST helpers shared by the k-NN AST pass ({@link KnnLimitResolver}), the Calcite lowering and the
 * analytics-engine guard: recognising the {@code knn(...)} predicate in a {@code where} condition,
 * and walking every input of a plan, including the side inputs of joins, lookups, appends and
 * subsearches in expressions, which {@link Node#getChild()} does not return.
 */
public final class KnnAst {

  private KnnAst() {}

  /** Whether the expression is a call of the {@code knn} function, in any letter case. */
  public static boolean isKnnCall(@Nullable Node expression) {
    return expression instanceof Function function
        && KnnStep.NAME.equalsIgnoreCase(function.getFuncName());
  }

  /**
   * The {@code knn(...)} call as the k-NN step that carries the resolved k.
   *
   * @throws IllegalStateException if the call is a plain {@link Function} rather than a {@link
   *     KnnFunction}, which every parser must build for {@code knn(...)}
   */
  public static KnnStep knnStep(Function knn) {
    if (knn instanceof KnnFunction step) {
      return step;
    }
    throw new IllegalStateException(
        "A knn() call must be built as a KnnFunction to carry its k, got " + knn);
  }

  /** Whether the expression is or contains a {@code knn(...)} call. Subqueries are not entered. */
  public static boolean containsKnnCall(@Nullable Node expression) {
    if (expression == null) {
      return false;
    }
    if (isKnnCall(expression)) {
      return true;
    }
    for (Node child : expressionChildren(expression)) {
      if (containsKnnCall(child)) {
        return true;
      }
    }
    return false;
  }

  /**
   * The conjuncts of a condition: the operands of its top-level {@code and}s, in order.
   *
   * @param condition a {@code where} condition
   * @return the conjuncts; the condition itself if it is not an {@code and}
   */
  public static List<UnresolvedExpression> conjuncts(UnresolvedExpression condition) {
    List<UnresolvedExpression> conjuncts = new ArrayList<>();
    addConjuncts(condition, conjuncts);
    return conjuncts;
  }

  private static void addConjuncts(UnresolvedExpression condition, List<UnresolvedExpression> out) {
    if (condition instanceof And and) {
      addConjuncts(and.getLeft(), out);
      addConjuncts(and.getRight(), out);
    } else {
      out.add(condition);
    }
  }

  /**
   * The {@code knn(...)} predicate of a {@code where} condition.
   *
   * @param condition a {@code where} condition
   * @return the {@code knn(...)} call if it is a top-level conjunct; empty if the condition has
   *     none
   * @throws org.opensearch.sql.exception.SemanticCheckException <i>more than one knn</i> if two
   *     conjuncts are {@code knn(...)}, <i>knn under OR or NOT</i> if a {@code knn(...)} call is
   *     nested anywhere else in the condition
   */
  public static Optional<Function> topLevelKnn(UnresolvedExpression condition) {
    Function knn = null;
    for (UnresolvedExpression conjunct : conjuncts(condition)) {
      if (isKnnCall(conjunct)) {
        if (knn != null) {
          throw KnnErrors.moreThanOneKnn();
        }
        knn = (Function) conjunct;
        for (UnresolvedExpression argument : knn.getFuncArgs()) {
          if (containsKnnCall(argument)) {
            throw KnnErrors.knnUnderOrOrNot();
          }
        }
      } else if (containsKnnCall(conjunct)) {
        throw KnnErrors.knnUnderOrOrNot();
      }
    }
    return Optional.ofNullable(knn);
  }

  /**
   * The conjunction of {@code condition}'s conjuncts other than {@code knn}, the pre-filter of a
   * {@code where ... and knn(...)}.
   *
   * @return the remaining conjuncts joined by {@code and}, or empty if {@code knn} is the only one
   */
  public static Optional<UnresolvedExpression> withoutConjunct(
      UnresolvedExpression condition, UnresolvedExpression knn) {
    return conjuncts(condition).stream().filter(c -> c != knn).reduce(And::new);
  }

  /**
   * Every input plan of a node: its children and its side inputs (the right side of a join, a
   * lookup table, the subsearch of {@code append}, {@code appendcol} and {@code appendpipe}, the
   * table of {@code graphlookup}). Subsearches inside expressions are returned by {@link
   * #subqueryPlans(UnresolvedPlan)}.
   */
  public static List<UnresolvedPlan> inputs(@Nullable UnresolvedPlan node) {
    List<UnresolvedPlan> inputs = new ArrayList<>();
    if (node == null) {
      // An input pruned away, e.g. by the empty-source pass; the lowering reports it, not this.
      return inputs;
    }
    if (node instanceof Join join) {
      addIfPresent(inputs, join.getLeft());
      addIfPresent(inputs, join.getRight());
      return inputs;
    }
    List<? extends Node> children = node.getChild();
    if (children != null) {
      for (Node child : children) {
        if (child instanceof UnresolvedPlan plan) {
          inputs.add(plan);
        }
      }
    }
    switch (node) {
      case Lookup lookup -> addIfPresent(inputs, lookup.getLookupRelation());
      case Append append -> addIfPresent(inputs, append.getSubSearch());
      case AppendCol appendCol -> addIfPresent(inputs, appendCol.getSubSearch());
      case AppendPipe appendPipe -> addIfPresent(inputs, appendPipe.getSubQuery());
      case GraphLookup graphLookup -> addIfPresent(inputs, graphLookup.getFromTable());
      default -> {}
    }
    return inputs;
  }

  private static void addIfPresent(List<UnresolvedPlan> inputs, @Nullable UnresolvedPlan plan) {
    if (plan != null) {
      inputs.add(plan);
    }
  }

  /**
   * The expressions of a node that can hold a {@code knn(...)} call or a subsearch: the condition
   * of a {@code where}, the assignments of an {@code eval}, the list of a projection, sort keys,
   * aggregations and their group keys, window functions, join conditions, and the expressions of
   * the other commands that take them. Plain field references are left out where a command only
   * takes those.
   */
  public static List<Node> expressions(UnresolvedPlan node) {
    List<Node> expressions = new ArrayList<>();
    switch (node) {
      case Filter filter -> expressions.add(filter.getCondition());
      case Eval eval -> expressions.addAll(eval.getExpressionList());
      case Project project -> expressions.addAll(project.getProjectList());
      case Sort sort -> expressions.addAll(sort.getSortList());
      case Aggregation aggregation -> {
        expressions.addAll(aggregation.getAggExprList());
        expressions.addAll(aggregation.getSortExprList());
        expressions.addAll(aggregation.getGroupExprList());
        expressions.add(aggregation.getSpan());
      }
      case Window window -> {
        expressions.addAll(window.getWindowFunctionList());
        expressions.addAll(window.getGroupList());
      }
      case StreamWindow window -> {
        expressions.addAll(window.getWindowFunctionList());
        expressions.addAll(window.getGroupList());
        expressions.add(window.getResetBefore());
        expressions.add(window.getResetAfter());
      }
      case Join join -> join.getJoinCondition().ifPresent(expressions::add);
      case RareTopN rareTopN -> expressions.addAll(rareTopN.getGroupExprList());
      case Chart chart -> {
        expressions.add(chart.getRowSplit());
        expressions.add(chart.getColumnSplit());
        expressions.add(chart.getAggregationFunction());
      }
      case FillNull fillNull -> {
        fillNull.getReplacementForAll().ifPresent(expressions::add);
        fillNull.getReplacementPairs().forEach(pair -> expressions.add(pair.getRight()));
      }
      case Convert convert -> expressions.addAll(convert.getConversions());
      case Foreach foreach -> {
        expressions.add(foreach.getCollectionExpression());
        foreach.getEvalClauses().forEach(clause -> expressions.add(clause.getExpression()));
      }
      case GraphLookup graphLookup -> expressions.add(graphLookup.getFilter());
      case Parse parse -> expressions.add(parse.getSourceField());
      case Rex rex -> expressions.add(rex.getField());
      case Regex regex -> expressions.add(regex.getField());
      case Patterns patterns -> {
        expressions.add(patterns.getSourceField());
        expressions.addAll(patterns.getPartitionByList());
      }
      case Bin bin -> expressions.add(bin.getField());
      case Trendline trendline -> {
        trendline.getSortByField().ifPresent(expressions::add);
        expressions.addAll(trendline.getComputations());
      }
      case Xyseries xyseries -> {
        expressions.add(xyseries.getXField());
        expressions.add(xyseries.getYNameField());
        expressions.addAll(xyseries.getYDataFields());
      }
      default -> {}
    }
    expressions.removeIf(Objects::isNull);
    return expressions;
  }

  /**
   * Whether the sub-pipeline of an {@code appendpipe} has a k-NN step of its own. The sub-pipeline
   * continues the main search (the lowering attaches the main search below it), so a k-NN step in
   * it and one in the main search are two steps in one branch. The walk stops at the attached main
   * search, if any.
   */
  public static boolean subPipelineHasKnnStep(AppendPipe appendPipe) {
    UnresolvedPlan main = appendPipe.getChild().isEmpty() ? null : appendPipe.getChild().getFirst();
    UnresolvedPlan node = appendPipe.getSubQuery();
    while (node != null && node != main) {
      if (node instanceof Knn
          || (node instanceof Filter filter && containsKnnCall(filter.getCondition()))) {
        return true;
      }
      List<? extends Node> children = node.getChild();
      node =
          children == null
                  || children.isEmpty()
                  || !(children.getFirst() instanceof UnresolvedPlan p)
              ? null
              : p;
    }
    return false;
  }

  /** The plans of the subsearches ({@code in}, {@code exists}, scalar) in a node's expressions. */
  public static List<UnresolvedPlan> subqueryPlans(UnresolvedPlan node) {
    List<UnresolvedPlan> plans = new ArrayList<>();
    for (Node expression : expressions(node)) {
      addSubqueryPlans(expression, plans);
    }
    return plans;
  }

  private static void addSubqueryPlans(@Nullable Node expression, List<UnresolvedPlan> out) {
    if (expression == null) {
      return;
    }
    switch (expression) {
      case InSubquery in -> addIfPresent(out, in.getQuery());
      case ExistsSubquery exists -> addIfPresent(out, exists.getQuery());
      case ScalarSubquery scalar -> addIfPresent(out, scalar.getQuery());
      default -> {}
    }
    for (Node child : expressionChildren(expression)) {
      addSubqueryPlans(child, out);
    }
  }

  /** The operands of an expression, including those {@link Node#getChild()} leaves out. */
  private static List<? extends Node> expressionChildren(Node expression) {
    return switch (expression) {
      case Let let -> List.of(let.getExpression());
      case Alias alias -> alias.getDelegated() == null ? List.of() : List.of(alias.getDelegated());
      case In in -> {
        // In.getChild() has only the tested value, not the list it is tested against.
        List<Node> operands = new ArrayList<>();
        operands.add(in.getField());
        operands.addAll(in.getValueList());
        yield operands;
      }
      case Trendline.TrendlineComputation computation -> List.of(computation.getDataField());
      case AggregateFunction aggregate -> {
        List<Node> operands = new ArrayList<>();
        operands.add(aggregate.getField());
        if (aggregate.getArgList() != null) {
          operands.addAll(aggregate.getArgList());
        }
        operands.add(aggregate.condition());
        operands.removeIf(Objects::isNull);
        yield operands;
      }
      case WindowFunction window -> {
        List<Node> operands = new ArrayList<>();
        operands.add(window.getFunction());
        operands.addAll(window.getPartitionByList());
        window.getSortList().forEach(pair -> operands.add(pair.getRight()));
        yield operands;
      }
      default -> expression.getChild() == null ? List.of() : expression.getChild();
    };
  }

  /**
   * Whether lowering {@code plan} as a query branch gives the k-NN {@code _score}: it has a k-NN
   * step itself or in an input whose rows it combines (a join side, a lookup table, an append or
   * appendcol subsearch, an appendpipe, multisearch or union branch). A subsearch in an expression
   * or a graphlookup table does not count. This mirrors how {@code CalciteRelNodeVisitor}
   * propagates its branch flag, for decisions it must take before lowering an input.
   */
  public static boolean producesKnnScore(@Nullable UnresolvedPlan plan) {
    if (plan instanceof Knn
        || (plan instanceof Filter filter && containsKnnCall(filter.getCondition()))) {
      return true;
    }
    for (UnresolvedPlan input : inputs(plan)) {
      if (!isSeparateBranch(plan, input) && producesKnnScore(input)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether {@code input} of {@code node} is a branch whose k-NN step does not reach {@code node}'s
   * output: the table of a graphlookup, which is nested into an output column.
   */
  public static boolean isSeparateBranch(UnresolvedPlan node, UnresolvedPlan input) {
    return node instanceof GraphLookup graphLookup && input == graphLookup.getFromTable();
  }

  /**
   * The PPL command name of a node, as error messages show it, e.g. {@code where} or {@code stats}.
   */
  public static String commandName(UnresolvedPlan node) {
    return switch (node) {
      case Filter filter -> "where";
      case Aggregation aggregation -> "stats";
      case Dedupe dedupe -> "dedup";
      case RareTopN rareTopN -> rareTopN.getCommandType().name().toLowerCase(Locale.ROOT);
      case Window window -> "eventstats";
      case StreamWindow streamWindow -> "streamstats";
      case AppendCol appendCol -> "appendcol";
      case AppendPipe appendPipe -> "appendpipe";
      case Parse parse ->
          parse.getParseMethod().getName().equals("regex")
              ? "parse"
              : parse.getParseMethod().getName();
      case RelationSubquery subquery -> "subquery";
      case Bin bin -> "bin";
      default -> node.getClass().getSimpleName().toLowerCase(Locale.ROOT);
    };
  }

  /**
   * Every k-NN step of a statement: {@code knn} commands and {@code knn(...)} conjuncts of {@code
   * where}, in any branch or subsearch. Each node is visited once, as lowering may attach nodes
   * under others.
   */
  public static List<KnnStep> steps(UnresolvedPlan statement) {
    List<KnnStep> steps = new ArrayList<>();
    addSteps(statement, steps, Collections.newSetFromMap(new IdentityHashMap<>()));
    return steps;
  }

  private static void addSteps(UnresolvedPlan plan, List<KnnStep> steps, Set<Node> visited) {
    if (!visited.add(plan)) {
      return;
    }
    if (plan instanceof Knn knn) {
      steps.add(knn);
    } else if (plan instanceof Filter filter
        && containsKnnCall(filter.getCondition())
        && topLevelKnn(filter.getCondition()).orElse(null) instanceof KnnFunction knn) {
      steps.add(knn);
    }
    subqueryPlans(plan).forEach(subquery -> addSteps(subquery, steps, visited));
    inputs(plan).forEach(input -> addSteps(input, steps, visited));
  }

  /**
   * Whether a statement contains a k-NN step anywhere: a {@code knn} command, or a {@code knn(...)}
   * call in a {@code where}, {@code eval} or projection, in any branch or subsearch.
   */
  public static boolean containsKnnStep(UnresolvedPlan plan) {
    if (plan instanceof Knn) {
      return true;
    }
    for (Node expression : expressions(plan)) {
      if (containsKnnCall(expression)) {
        return true;
      }
    }
    for (UnresolvedPlan subquery : subqueryPlans(plan)) {
      if (containsKnnStep(subquery)) {
        return true;
      }
    }
    for (UnresolvedPlan input : inputs(plan)) {
      if (containsKnnStep(input)) {
        return true;
      }
    }
    return false;
  }
}
