/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_FIRST;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_LAST;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.ASC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.DESC;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.utils.MLCommonsConstants.RCF_ANOMALOUS;
import static org.opensearch.sql.utils.MLCommonsConstants.RCF_ANOMALY_GRADE;
import static org.opensearch.sql.utils.MLCommonsConstants.RCF_SCORE;
import static org.opensearch.sql.utils.MLCommonsConstants.TIME_FIELD;
import static org.opensearch.sql.utils.SystemIndexUtils.DATASOURCES_TABLE_NAME;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.AD;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.CloseCursor;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FetchCursor;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Flatten;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Kmeans;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.ML;
import org.opensearch.sql.ast.tree.Paginate;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.RelationSubquery;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Values;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprMissingValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.Aggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.TableFunctionImplementation;
import org.opensearch.sql.expression.parse.ParseExpression;
import org.opensearch.sql.planner.logical.LogicalAD;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalCloseCursor;
import org.opensearch.sql.planner.logical.LogicalDedupe;
import org.opensearch.sql.planner.logical.LogicalEval;
import org.opensearch.sql.planner.logical.LogicalFetchCursor;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalFlatten;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalML;
import org.opensearch.sql.planner.logical.LogicalMLCommons;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRareTopN;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalRemove;
import org.opensearch.sql.planner.logical.LogicalRename;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.planner.logical.LogicalTrendline;
import org.opensearch.sql.planner.logical.LogicalValues;
import org.opensearch.sql.planner.physical.datasource.DataSourceTable;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.utils.ParseUtils;

/**
 * Analyze the {@link UnresolvedPlan} in the {@link AnalysisContext} to construct the {@link
 * LogicalPlan}.
 */
public class Analyzer extends AbstractNodeVisitor<LogicalPlan, AnalysisContext> {

  private final ExpressionAnalyzer expressionAnalyzer;

  private final SelectExpressionAnalyzer selectExpressionAnalyzer;

  private final NamedExpressionAnalyzer namedExpressionAnalyzer;

  private final DataSourceService dataSourceService;

  private final BuiltinFunctionRepository repository;

  /** Constructor. */
  public Analyzer(
      ExpressionAnalyzer expressionAnalyzer,
      DataSourceService dataSourceService,
      BuiltinFunctionRepository repository) {
    this.expressionAnalyzer = expressionAnalyzer;
    this.dataSourceService = dataSourceService;
    this.selectExpressionAnalyzer = new SelectExpressionAnalyzer(expressionAnalyzer);
    this.namedExpressionAnalyzer = new NamedExpressionAnalyzer(expressionAnalyzer);
    this.repository = repository;
  }

  public LogicalPlan analyze(UnresolvedPlan unresolved, AnalysisContext context) {
    return unresolved.accept(this, context);
  }

  @Override
  public LogicalPlan visitRelation(Relation node, AnalysisContext context) {
    QualifiedName qualifiedName = node.getTableQualifiedName();
    DataSourceSchemaIdentifierNameResolver dataSourceSchemaIdentifierNameResolver =
        new DataSourceSchemaIdentifierNameResolver(dataSourceService, qualifiedName.getParts());
    String tableName = dataSourceSchemaIdentifierNameResolver.getIdentifierName();
    context.push();
    TypeEnvironment curEnv = context.peek();
    Table table;
    if (DATASOURCES_TABLE_NAME.equals(tableName)) {
      table = new DataSourceTable(dataSourceService);
    } else {
      table =
          dataSourceService
              .getDataSource(dataSourceSchemaIdentifierNameResolver.getDataSourceName())
              .getStorageEngine()
              .getTable(
                  new DataSourceSchemaName(
                      dataSourceSchemaIdentifierNameResolver.getDataSourceName(),
                      dataSourceSchemaIdentifierNameResolver.getSchemaName()),
                  dataSourceSchemaIdentifierNameResolver.getIdentifierName());
    }
    table.getFieldTypes().forEach((k, v) -> curEnv.define(new Symbol(Namespace.FIELD_NAME, k), v));
    table
        .getReservedFieldTypes()
        .forEach((k, v) -> curEnv.define(new Symbol(Namespace.HIDDEN_FIELD_NAME, k), v));

    // Put index name or its alias in index namespace on type environment so qualifier
    // can be removed when analyzing qualified name. The value (expr type) here doesn't matter.
    curEnv.define(
        new Symbol(Namespace.INDEX_NAME, (node.getAlias() == null) ? tableName : node.getAlias()),
        STRUCT);

    return new LogicalRelation(tableName, table);
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
  public LogicalPlan visitTableFunction(TableFunction node, AnalysisContext context) {
    QualifiedName qualifiedName = node.getFunctionName();
    DataSourceSchemaIdentifierNameResolver dataSourceSchemaIdentifierNameResolver =
        new DataSourceSchemaIdentifierNameResolver(
            this.dataSourceService, qualifiedName.getParts());

    FunctionName functionName =
        FunctionName.of(dataSourceSchemaIdentifierNameResolver.getIdentifierName());
    List<Expression> arguments =
        node.getArguments().stream()
            .map(
                unresolvedExpression ->
                    this.expressionAnalyzer.analyze(unresolvedExpression, context))
            .collect(Collectors.toList());
    TableFunctionImplementation tableFunctionImplementation =
        (TableFunctionImplementation)
            repository.compile(
                context.getFunctionProperties(),
                dataSourceService
                    .getDataSource(dataSourceSchemaIdentifierNameResolver.getDataSourceName())
                    .getStorageEngine()
                    .getFunctions(),
                functionName,
                arguments);
    context.push();
    TypeEnvironment curEnv = context.peek();
    Table table = tableFunctionImplementation.applyArguments();
    table.getFieldTypes().forEach((k, v) -> curEnv.define(new Symbol(Namespace.FIELD_NAME, k), v));
    table
        .getReservedFieldTypes()
        .forEach((k, v) -> curEnv.define(new Symbol(Namespace.HIDDEN_FIELD_NAME, k), v));
    curEnv.define(
        new Symbol(
            Namespace.INDEX_NAME, dataSourceSchemaIdentifierNameResolver.getIdentifierName()),
        STRUCT);
    return new LogicalRelation(
        dataSourceSchemaIdentifierNameResolver.getIdentifierName(),
        tableFunctionImplementation.applyArguments());
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
   * Ensure NESTED function is not used in GROUP BY, and HAVING clauses. Fallback to legacy engine.
   * Can remove when support is added for NESTED function in WHERE, GROUP BY, ORDER BY, and HAVING
   * clauses.
   *
   * @param condition : Filter condition
   */
  private void verifySupportsCondition(Expression condition) {
    if (condition instanceof FunctionExpression) {
      if (((FunctionExpression) condition)
          .getFunctionName()
          .getFunctionName()
          .equalsIgnoreCase(BuiltinFunctionName.NESTED.name())) {
        throw new SyntaxCheckException(
            "Falling back to legacy engine. Nested function is not supported in WHERE,"
                + " GROUP BY, and HAVING clauses.");
      }
      ((FunctionExpression) condition)
          .getArguments().stream().forEach(e -> verifySupportsCondition(e));
    }
  }

  /** Build {@link LogicalRename}. */
  @Override
  public LogicalPlan visitRename(Rename node, AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);
    ImmutableMap.Builder<ReferenceExpression, ReferenceExpression> renameMapBuilder =
        new ImmutableMap.Builder<>();
    for (org.opensearch.sql.ast.expression.Map renameMap : node.getRenameList()) {
      Expression origin = expressionAnalyzer.analyze(renameMap.getOrigin(), context);
      // We should define the new target field in the context instead of analyze it.
      if (renameMap.getTarget() instanceof Field) {
        ReferenceExpression target =
            new ReferenceExpression(
                ((Field) renameMap.getTarget()).getField().toString(), origin.type());
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

  /** Build {@link LogicalAggregation}. */
  @Override
  public LogicalPlan visitAggregation(Aggregation node, AnalysisContext context) {
    final LogicalPlan child = node.getChild().get(0).accept(this, context);
    ImmutableList.Builder<NamedAggregator> aggregatorBuilder = new ImmutableList.Builder<>();
    for (UnresolvedExpression expr : node.getAggExprList()) {
      NamedExpression aggExpr = namedExpressionAnalyzer.analyze(expr, context);
      aggregatorBuilder.add(
          new NamedAggregator(aggExpr.getNameOrAlias(), (Aggregator) aggExpr.getDelegated()));
    }

    ImmutableList.Builder<NamedExpression> groupbyBuilder = new ImmutableList.Builder<>();
    // Span should be first expression if exist.
    if (node.getSpan() != null) {
      groupbyBuilder.add(namedExpressionAnalyzer.analyze(node.getSpan(), context));
    }

    for (UnresolvedExpression expr : node.getGroupExprList()) {
      NamedExpression resolvedExpr = namedExpressionAnalyzer.analyze(expr, context);
      verifySupportsCondition(resolvedExpr.getDelegated());
      groupbyBuilder.add(resolvedExpr);
    }
    ImmutableList<NamedExpression> groupBys = groupbyBuilder.build();

    ImmutableList<NamedAggregator> aggregators = aggregatorBuilder.build();
    // new context
    context.push();
    TypeEnvironment newEnv = context.peek();
    aggregators.forEach(
        aggregator ->
            newEnv.define(
                new Symbol(Namespace.FIELD_NAME, aggregator.getName()), aggregator.type()));
    groupBys.forEach(
        group ->
            newEnv.define(new Symbol(Namespace.FIELD_NAME, group.getNameOrAlias()), group.type()));
    return new LogicalAggregation(child, aggregators, groupBys);
  }

  /** Build {@link LogicalRareTopN}. */
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
    groupBys.forEach(
        group -> newEnv.define(new Symbol(Namespace.FIELD_NAME, group.toString()), group.type()));
    fields.forEach(
        field -> newEnv.define(new Symbol(Namespace.FIELD_NAME, field.toString()), field.type()));

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

    for (UnresolvedExpression expr : node.getProjectList()) {
      HighlightAnalyzer highlightAnalyzer = new HighlightAnalyzer(expressionAnalyzer, child);
      child = highlightAnalyzer.analyze(expr, context);
    }

    List<NamedExpression> namedExpressions =
        selectExpressionAnalyzer.analyze(
            node.getProjectList(),
            context,
            new ExpressionReferenceOptimizer(expressionAnalyzer.getRepository(), child));

    for (UnresolvedExpression expr : node.getProjectList()) {
      NestedAnalyzer nestedAnalyzer =
          new NestedAnalyzer(namedExpressions, expressionAnalyzer, child);
      child = nestedAnalyzer.analyze(expr, context);
    }

    // new context
    context.push();
    TypeEnvironment newEnv = context.peek();
    namedExpressions.forEach(
        expr ->
            newEnv.define(new Symbol(Namespace.FIELD_NAME, expr.getNameOrAlias()), expr.type()));
    List<NamedExpression> namedParseExpressions = context.getNamedParseExpressions();
    return new LogicalProject(child, namedExpressions, namedParseExpressions);
  }

  /** Build {@link LogicalEval}. */
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
   * Builds and returns a {@link org.opensearch.sql.planner.logical.LogicalFlatten} corresponding to
   * the given flatten node.
   */
  @Override
  public LogicalPlan visitFlatten(Flatten node, AnalysisContext context) {
    LogicalPlan child = node.getChild().getFirst().accept(this, context);

    ReferenceExpression fieldExpr =
        (ReferenceExpression) expressionAnalyzer.analyze(node.getField(), context);
    String fieldName = fieldExpr.getAttr();
    ExprType fieldType = fieldExpr.type();

    // [A] Determine fields to add
    // ---------------------------

    // Iterate over all the fields defined in the type environment. Find all those that are
    // descended from field that is being flattened. Determine the new path to add and remove the
    // existing path. When determining the new path, we need to preserve the portion of the
    // path corresponding to the flattened field's parent, if one exists, in order to support
    // flattening nested structs - see example below.
    //
    // Input Data:
    //
    // [
    //    struct: {
    //      integer: 0,
    //      nested_struct: { string: "value" }
    //    }
    // ]
    //
    // Example 1: 'flatten struct'
    //
    // [
    //   integer: 0,
    //   nested_struct: { string: "value" }
    // ]
    //
    // Example 2: 'flatten struct.nested_struct'
    //
    // [
    //    struct: {
    //      integer: 0,
    //      string: "value"
    //    {
    // ]

    Map<String, ExprType> addFieldsMap = new HashMap<>();

    TypeEnvironment env = context.peek();
    Map<String, ExprType> fieldsMap = env.lookupAllTupleFields(Namespace.FIELD_NAME);

    final String pathSeparator = ".";
    final String fieldDescendantPath = fieldName + pathSeparator;
    final Optional<String> fieldParentPath =
        fieldName.contains(pathSeparator)
            ? Optional.of(fieldName.substring(0, fieldName.lastIndexOf(pathSeparator)))
            : Optional.empty();

    for (String path : fieldsMap.keySet()) {

      // Verify that the path is descended from the flattened field.
      if (!path.startsWith(fieldDescendantPath)) {
        continue;
      }

      // Build the new path.
      String newPath = path.substring(fieldDescendantPath.length());
      if (fieldParentPath.isPresent()) {
        newPath = fieldParentPath.get() + pathSeparator + newPath;
      }

      ExprType type = fieldsMap.get(path);
      addFieldsMap.put(newPath, type);
    }

    // [B] Update environment
    // ----------------------

    for (Map.Entry<String, ExprType> entry : addFieldsMap.entrySet()) {
      String name = entry.getKey();
      ExprType type = entry.getValue();

      // Verify that new field does not overwrite an existing field.
      if (fieldsMap.containsKey(name)) {
        throw new IllegalArgumentException(
            StringUtils.format("Flatten command cannot overwrite field '%s'", name));
      }

      env.define(DSL.ref(name, type));
    }

    return new LogicalFlatten(child, DSL.ref(fieldName, STRUCT));
  }

  /** Build {@link ParseExpression} to context and skip to child nodes. */
  @Override
  public LogicalPlan visitParse(Parse node, AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);
    Expression sourceField = expressionAnalyzer.analyze(node.getSourceField(), context);
    ParseMethod parseMethod = node.getParseMethod();
    java.util.Map<String, Literal> arguments = node.getArguments();
    String pattern = (String) node.getPattern().getValue();
    Expression patternExpression = DSL.literal(pattern);

    TypeEnvironment curEnv = context.peek();
    ParseUtils.getNamedGroupCandidates(parseMethod, pattern, arguments)
        .forEach(
            group -> {
              ParseExpression expr =
                  ParseUtils.createParseExpression(
                      parseMethod, sourceField, patternExpression, DSL.literal(group));
              curEnv.define(new Symbol(Namespace.FIELD_NAME, group), expr.type());
              context.getNamedParseExpressions().add(new NamedExpression(group, expr));
            });
    return child;
  }

  /** Build {@link LogicalSort}. */
  @Override
  public LogicalPlan visitSort(Sort node, AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);
    return buildSort(child, context, node.getSortList());
  }

  /** Build {@link LogicalDedupe}. */
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

  /** Logical head is identical to {@link LogicalLimit}. */
  public LogicalPlan visitHead(Head node, AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);
    return new LogicalLimit(child, node.getSize(), node.getFrom());
  }

  @Override
  public LogicalPlan visitValues(Values node, AnalysisContext context) {
    List<List<Literal>> values = node.getValues();
    List<List<LiteralExpression>> valueExprs = new ArrayList<>();
    for (List<Literal> value : values) {
      valueExprs.add(
          value.stream()
              .map(val -> (LiteralExpression) expressionAnalyzer.analyze(val, context))
              .collect(Collectors.toList()));
    }
    return new LogicalValues(valueExprs);
  }

  /** Build {@link LogicalMLCommons} for Kmeans command. */
  @Override
  public LogicalPlan visitKmeans(Kmeans node, AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);
    java.util.Map<String, Literal> options = node.getArguments();

    TypeEnvironment currentEnv = context.peek();
    currentEnv.define(new Symbol(Namespace.FIELD_NAME, "ClusterID"), ExprCoreType.INTEGER);

    return new LogicalMLCommons(child, "kmeans", options);
  }

  /** Build {@link LogicalAD} for AD command. */
  @Override
  public LogicalPlan visitAD(AD node, AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);
    java.util.Map<String, Literal> options = node.getArguments();

    TypeEnvironment currentEnv = context.peek();

    currentEnv.define(new Symbol(Namespace.FIELD_NAME, RCF_SCORE), ExprCoreType.DOUBLE);
    if (Objects.isNull(node.getArguments().get(TIME_FIELD))) {
      currentEnv.define(new Symbol(Namespace.FIELD_NAME, RCF_ANOMALOUS), ExprCoreType.BOOLEAN);
    } else {
      currentEnv.define(new Symbol(Namespace.FIELD_NAME, RCF_ANOMALY_GRADE), ExprCoreType.DOUBLE);
      currentEnv.define(
          new Symbol(Namespace.FIELD_NAME, (String) node.getArguments().get(TIME_FIELD).getValue()),
          ExprCoreType.TIMESTAMP);
    }
    return new LogicalAD(child, options);
  }

  /** Build {@link LogicalEval} for fillnull command. */
  @Override
  public LogicalPlan visitFillNull(final FillNull node, final AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);

    ImmutableList.Builder<Pair<ReferenceExpression, Expression>> expressionsBuilder =
        new Builder<>();
    for (FillNull.NullableFieldFill fieldFill : node.getNullableFieldFills()) {
      Expression fieldExpr =
          expressionAnalyzer.analyze(fieldFill.getNullableFieldReference(), context);
      ReferenceExpression ref =
          DSL.ref(fieldFill.getNullableFieldReference().getField().toString(), fieldExpr.type());
      FunctionExpression ifNullFunction =
          DSL.ifnull(ref, expressionAnalyzer.analyze(fieldFill.getReplaceNullWithMe(), context));
      expressionsBuilder.add(new ImmutablePair<>(ref, ifNullFunction));
      TypeEnvironment typeEnvironment = context.peek();
      // define the new reference in type env.
      typeEnvironment.define(ref);
    }

    return new LogicalEval(child, expressionsBuilder.build());
  }

  /** Build {@link LogicalML} for ml command. */
  @Override
  public LogicalPlan visitML(ML node, AnalysisContext context) {
    LogicalPlan child = node.getChild().get(0).accept(this, context);
    TypeEnvironment currentEnv = context.peek();
    node.getOutputSchema(currentEnv).entrySet().stream()
        .forEach(
            v -> currentEnv.define(new Symbol(Namespace.FIELD_NAME, v.getKey()), v.getValue()));

    return new LogicalML(child, node.getArguments());
  }

  /** Build {@link LogicalTrendline} for Trendline command. */
  @Override
  public LogicalPlan visitTrendline(Trendline node, AnalysisContext context) {
    final LogicalPlan child = node.getChild().get(0).accept(this, context);

    final TypeEnvironment currEnv = context.peek();
    final List<Trendline.TrendlineComputation> computations = node.getComputations();
    final ImmutableList.Builder<Pair<Trendline.TrendlineComputation, ExprCoreType>>
        computationsAndTypes = ImmutableList.builder();
    computations.forEach(
        computation -> {
          final Expression resolvedField =
              expressionAnalyzer.analyze(computation.getDataField(), context);
          final ExprCoreType averageType;
          // Duplicate the semantics of AvgAggregator#create():
          // - All numerical types have the DOUBLE type for the moving average.
          // - All datetime types have the same datetime type for the moving average.
          if (ExprCoreType.numberTypes().contains(resolvedField.type())) {
            averageType = ExprCoreType.DOUBLE;
          } else {
            switch (resolvedField.type()) {
              case DATE:
              case TIME:
              case TIMESTAMP:
                averageType = (ExprCoreType) resolvedField.type();
                break;
              default:
                throw new SemanticCheckException(
                    String.format(
                        "Invalid field used for trendline computation %s. Source field %s had type"
                            + " %s but must be a numerical or datetime field.",
                        computation.getAlias(),
                        computation.getDataField().getChild().get(0),
                        resolvedField.type().typeName()));
            }
          }
          currEnv.define(new Symbol(Namespace.FIELD_NAME, computation.getAlias()), averageType);
          computationsAndTypes.add(Pair.of(computation, averageType));
        });

    if (node.getSortByField().isEmpty()) {
      return new LogicalTrendline(child, computationsAndTypes.build());
    }

    return new LogicalTrendline(
        buildSort(child, context, Collections.singletonList(node.getSortByField().get())),
        computationsAndTypes.build());
  }

  @Override
  public LogicalPlan visitPaginate(Paginate paginate, AnalysisContext context) {
    LogicalPlan child = paginate.getChild().get(0).accept(this, context);
    return new LogicalPaginate(paginate.getPageSize(), List.of(child));
  }

  @Override
  public LogicalPlan visitFetchCursor(FetchCursor cursor, AnalysisContext context) {
    return new LogicalFetchCursor(
        cursor.getCursor(),
        dataSourceService.getDataSource(DEFAULT_DATASOURCE_NAME).getStorageEngine());
  }

  @Override
  public LogicalPlan visitCloseCursor(CloseCursor closeCursor, AnalysisContext context) {
    return new LogicalCloseCursor(closeCursor.getChild().get(0).accept(this, context));
  }

  private LogicalSort buildSort(
      LogicalPlan child, AnalysisContext context, List<Field> sortFields) {
    ExpressionReferenceOptimizer optimizer =
        new ExpressionReferenceOptimizer(expressionAnalyzer.getRepository(), child);

    List<Pair<SortOption, Expression>> sortList =
        sortFields.stream()
            .map(
                sortField -> {
                  var analyzed = expressionAnalyzer.analyze(sortField.getField(), context);
                  if (analyzed == null) {
                    throw new UnsupportedOperationException(
                        String.format("Invalid use of expression %s", sortField.getField()));
                  }
                  Expression expression = optimizer.optimize(analyzed, context);
                  return ImmutablePair.of(analyzeSortOption(sortField.getFieldArgs()), expression);
                })
            .collect(Collectors.toList());
    return new LogicalSort(child, sortList);
  }

  /**
   * The first argument is always "asc", others are optional. Given nullFirst argument, use its
   * value. Otherwise just use DEFAULT_ASC/DESC.
   */
  private SortOption analyzeSortOption(List<Argument> fieldArgs) {
    Boolean asc = (Boolean) fieldArgs.get(0).getValue().getValue();
    Optional<Argument> nullFirst =
        fieldArgs.stream().filter(option -> "nullFirst".equals(option.getArgName())).findFirst();

    if (nullFirst.isPresent()) {
      Boolean isNullFirst = (Boolean) nullFirst.get().getValue().getValue();
      return new SortOption((asc ? ASC : DESC), (isNullFirst ? NULL_FIRST : NULL_LAST));
    }
    return asc ? SortOption.DEFAULT_ASC : SortOption.DEFAULT_DESC;
  }
}
