/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.analysis;

import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_FIRST;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_LAST;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.ASC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.DESC;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Map;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.RelationSubquery;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Values;
import org.opensearch.sql.data.model.ExprMissingValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.Aggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalDedupe;
import org.opensearch.sql.planner.logical.LogicalEval;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRareTopN;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalRemove;
import org.opensearch.sql.planner.logical.LogicalRename;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.planner.logical.LogicalValues;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

/**
 * Analyze the {@link UnresolvedPlan} in the {@link AnalysisContext} to construct the {@link
 * LogicalPlan}.
 */
public class Analyzer extends AbstractNodeVisitor<LogicalPlan, AnalysisContext> {

  private final ExpressionAnalyzer expressionAnalyzer;

  private final SelectExpressionAnalyzer selectExpressionAnalyzer;

  private final NamedExpressionAnalyzer namedExpressionAnalyzer;

  private final StorageEngine storageEngine;

  /**
   * Constructor.
   */
  public Analyzer(
      ExpressionAnalyzer expressionAnalyzer,
      StorageEngine storageEngine) {
    this.expressionAnalyzer = expressionAnalyzer;
    this.storageEngine = storageEngine;
    this.selectExpressionAnalyzer = new SelectExpressionAnalyzer(expressionAnalyzer);
    this.namedExpressionAnalyzer = new NamedExpressionAnalyzer(expressionAnalyzer);
  }

  public LogicalPlan analyze(UnresolvedPlan unresolved, AnalysisContext context) {
    return unresolved.accept(this, context);
  }

  @Override
  public LogicalPlan visitRelation(Relation node, AnalysisContext context) {
    context.push();
    TypeEnvironment curEnv = context.peek();
    Table table = storageEngine.getTable(node.getTableName());
    table.getFieldTypes().forEach((k, v) -> curEnv.define(new Symbol(Namespace.FIELD_NAME, k), v));

    // Put index name or its alias in index namespace on type environment so qualifier
    // can be removed when analyzing qualified name. The value (expr type) here doesn't matter.
    curEnv.define(new Symbol(Namespace.INDEX_NAME, node.getTableNameOrAlias()), STRUCT);

    return new LogicalRelation(node.getTableName());
  }

  @Override
  public LogicalPlan visitRelationSubquery(RelationSubquery node, AnalysisContext context) {
    LogicalPlan subquery = analyze(node.getChild().get(0), context);
    // inherit the parent environment to keep the subquery fields in current environment
    TypeEnvironment curEnv = context.peek();

    // Put subquery alias in index namespace so the qualifier can be removed
    // when analyzing qualified name in the subquery layer
    curEnv.define(new Symbol(Namespace.INDEX_NAME, node.getAliasAsTableName()), STRUCT);
    return subquery;
  }

  @Override
  public LogicalPlan visitLimit(Limit node, AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);
    return new LogicalLimit(child, node.getLimit(), node.getOffset());
  }

  @Override
  public LogicalPlan visitFilter(Filter node, AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);
    Expression condition = expressionAnalyzer.analyze(node.getCondition(), context);

    ExpressionReferenceOptimizer optimizer =
        new ExpressionReferenceOptimizer(expressionAnalyzer.getRepository(), child);
    Expression optimized = optimizer.optimize(condition, context);
    return new LogicalFilter(child, optimized);
  }

  /**
   * Build {@link LogicalRename}.
   */
  @Override
  public LogicalPlan visitRename(Rename node, AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);
    ImmutableMap.Builder<ReferenceExpression, ReferenceExpression> renameMapBuilder =
        new ImmutableMap.Builder<>();
    for (Map renameMap : node.getRenameList()) {
      Expression origin = expressionAnalyzer.analyze(renameMap.getOrigin(), context);
      // We should define the new target field in the context instead of analyze it.
      if (renameMap.getTarget() instanceof Field) {
        ReferenceExpression target =
            new ReferenceExpression(((Field) renameMap.getTarget()).getField().toString(),
                origin.type());
        ReferenceExpression originExpr = DSL.ref(origin.toString(), origin.type());
        TypeEnvironment curEnv = context.peek();
        curEnv.remove(originExpr);
        curEnv.define(target);
        renameMapBuilder.put(originExpr, target);
      } else {
        throw new SemanticCheckException(
            String.format("the target expected to be field, but is %s", renameMap.getTarget()));
      }
    }

    return new LogicalRename(child, renameMapBuilder.build());
  }

  /**
   * Build {@link LogicalAggregation}.
   */
  @Override
  public LogicalPlan visitAggregation(Aggregation node, AnalysisContext context) {
    final LogicalPlan child = node.getChild().get(0).accept(this, context);
    ImmutableList.Builder<NamedAggregator> aggregatorBuilder = new ImmutableList.Builder<>();
    for (UnresolvedExpression expr : node.getAggExprList()) {
      NamedExpression aggExpr = namedExpressionAnalyzer.analyze(expr, context);
      aggregatorBuilder
          .add(new NamedAggregator(aggExpr.getNameOrAlias(), (Aggregator) aggExpr.getDelegated()));
    }

    ImmutableList.Builder<NamedExpression> groupbyBuilder = new ImmutableList.Builder<>();
    // Span should be first expression if exist.
    if (node.getSpan() != null) {
      groupbyBuilder.add(namedExpressionAnalyzer.analyze(node.getSpan(), context));
    }

    for (UnresolvedExpression expr : node.getGroupExprList()) {
      groupbyBuilder.add(namedExpressionAnalyzer.analyze(expr, context));
    }
    ImmutableList<NamedExpression> groupBys = groupbyBuilder.build();

    ImmutableList<NamedAggregator> aggregators = aggregatorBuilder.build();
    // new context
    context.push();
    TypeEnvironment newEnv = context.peek();
    aggregators.forEach(aggregator -> newEnv.define(new Symbol(Namespace.FIELD_NAME,
        aggregator.getName()), aggregator.type()));
    groupBys.forEach(group -> newEnv.define(new Symbol(Namespace.FIELD_NAME,
        group.getNameOrAlias()), group.type()));
    return new LogicalAggregation(child, aggregators, groupBys);
  }

  /**
   * Build {@link LogicalRareTopN}.
   */
  @Override
  public LogicalPlan visitRareTopN(RareTopN node, AnalysisContext context) {
    final LogicalPlan child = node.getChild().get(0).accept(this, context);

    ImmutableList.Builder<Expression> groupbyBuilder = new ImmutableList.Builder<>();
    for (UnresolvedExpression expr : node.getGroupExprList()) {
      groupbyBuilder.add(expressionAnalyzer.analyze(expr, context));
    }
    ImmutableList<Expression> groupBys = groupbyBuilder.build();

    ImmutableList.Builder<Expression> fieldsBuilder = new ImmutableList.Builder<>();
    for (Field f : node.getFields()) {
      fieldsBuilder.add(expressionAnalyzer.analyze(f, context));
    }
    ImmutableList<Expression> fields = fieldsBuilder.build();

    // new context
    context.push();
    TypeEnvironment newEnv = context.peek();
    groupBys.forEach(group -> newEnv.define(new Symbol(Namespace.FIELD_NAME,
        group.toString()), group.type()));
    fields.forEach(field -> newEnv.define(new Symbol(Namespace.FIELD_NAME,
        field.toString()), field.type()));

    List<Argument> options = node.getNoOfResults();
    Integer noOfResults = (Integer) options.get(0).getValue().getValue();

    return new LogicalRareTopN(child, node.getCommandType(), noOfResults, fields, groupBys);
  }

  /**
   * Build {@link LogicalProject} or {@link LogicalRemove} from {@link Field}.
   *
   * <p>Todo, the include/exclude fields should change the env definition. The cons of current
   * implementation is even the query contain the field reference which has been excluded from
   * fields command. There is no {@link SemanticCheckException} will be thrown. Instead, the during
   * runtime evaluation, the not exist field will be resolve to {@link ExprMissingValue} which will
   * not impact the correctness.
   *
   * <p>Postpone the implementation when finding more use case.
   */
  @Override
  public LogicalPlan visitProject(Project node, AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);

    if (node.hasArgument()) {
      Argument argument = node.getArgExprList().get(0);
      Boolean exclude = (Boolean) argument.getValue().getValue();
      if (exclude) {
        TypeEnvironment curEnv = context.peek();
        List<ReferenceExpression> referenceExpressions =
            node.getProjectList().stream()
                .map(expr -> (ReferenceExpression) expressionAnalyzer.analyze(expr, context))
                .collect(Collectors.toList());
        referenceExpressions.forEach(ref -> curEnv.remove(ref));
        return new LogicalRemove(child, ImmutableSet.copyOf(referenceExpressions));
      }
    }

    // For each unresolved window function, analyze it by "insert" a window and sort operator
    // between project and its child.
    for (UnresolvedExpression expr : node.getProjectList()) {
      WindowExpressionAnalyzer windowAnalyzer =
          new WindowExpressionAnalyzer(expressionAnalyzer, child);
      child = windowAnalyzer.analyze(expr, context);
    }

    List<NamedExpression> namedExpressions =
        selectExpressionAnalyzer.analyze(node.getProjectList(), context,
            new ExpressionReferenceOptimizer(expressionAnalyzer.getRepository(), child));
    // new context
    context.push();
    TypeEnvironment newEnv = context.peek();
    namedExpressions.forEach(expr -> newEnv.define(new Symbol(Namespace.FIELD_NAME,
        expr.getNameOrAlias()), expr.type()));
    return new LogicalProject(child, namedExpressions);
  }

  /**
   * Build {@link LogicalEval}.
   */
  @Override
  public LogicalPlan visitEval(Eval node, AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);
    ImmutableList.Builder<Pair<ReferenceExpression, Expression>> expressionsBuilder =
        new Builder<>();
    for (Let let : node.getExpressionList()) {
      Expression expression = expressionAnalyzer.analyze(let.getExpression(), context);
      ReferenceExpression ref = DSL.ref(let.getVar().getField().toString(), expression.type());
      expressionsBuilder.add(ImmutablePair.of(ref, expression));
      TypeEnvironment typeEnvironment = context.peek();
      // define the new reference in type env.
      typeEnvironment.define(ref);
    }
    return new LogicalEval(child, expressionsBuilder.build());
  }

  /**
   * Build {@link LogicalSort}.
   */
  @Override
  public LogicalPlan visitSort(Sort node, AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);
    ExpressionReferenceOptimizer optimizer =
        new ExpressionReferenceOptimizer(expressionAnalyzer.getRepository(), child);

    List<Pair<SortOption, Expression>> sortList =
        node.getSortList().stream()
            .map(
                sortField -> {
                  Expression expression = optimizer.optimize(
                      expressionAnalyzer.analyze(sortField.getField(), context), context);
                  return ImmutablePair.of(analyzeSortOption(sortField.getFieldArgs()), expression);
                })
            .collect(Collectors.toList());
    return new LogicalSort(child, sortList);
  }

  /**
   * Build {@link LogicalDedupe}.
   */
  @Override
  public LogicalPlan visitDedupe(Dedupe node, AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);
    List<Argument> options = node.getOptions();
    // Todo, refactor the option.
    Integer allowedDuplication = (Integer) options.get(0).getValue().getValue();
    Boolean keepEmpty = (Boolean) options.get(1).getValue().getValue();
    Boolean consecutive = (Boolean) options.get(2).getValue().getValue();

    return new LogicalDedupe(
        child,
        node.getFields().stream()
            .map(f -> expressionAnalyzer.analyze(f, context))
            .collect(Collectors.toList()),
        allowedDuplication,
        keepEmpty,
        consecutive);
  }

  /**
   * Logical head is identical to {@link LogicalLimit}.
   */
  public LogicalPlan visitHead(Head node, AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);
    return new LogicalLimit(child, node.getSize(), 0);
  }

  @Override
  public LogicalPlan visitValues(Values node, AnalysisContext context) {
    List<List<Literal>> values = node.getValues();
    List<List<LiteralExpression>> valueExprs = new ArrayList<>();
    for (List<Literal> value : values) {
      valueExprs.add(value.stream()
          .map(val -> (LiteralExpression) expressionAnalyzer.analyze(val, context))
          .collect(Collectors.toList()));
    }
    return new LogicalValues(valueExprs);
  }

  /**
   * The first argument is always "asc", others are optional.
   * Given nullFirst argument, use its value. Otherwise just use DEFAULT_ASC/DESC.
   */
  private SortOption analyzeSortOption(List<Argument> fieldArgs) {
    Boolean asc = (Boolean) fieldArgs.get(0).getValue().getValue();
    Optional<Argument> nullFirst = fieldArgs.stream()
        .filter(option -> "nullFirst".equals(option.getArgName())).findFirst();

    if (nullFirst.isPresent()) {
      Boolean isNullFirst = (Boolean) nullFirst.get().getValue().getValue();
      return new SortOption((asc ? ASC : DESC), (isNullFirst ? NULL_FIRST : NULL_LAST));
    }
    return asc ? SortOption.DEFAULT_ASC : SortOption.DEFAULT_DESC;
  }

}
