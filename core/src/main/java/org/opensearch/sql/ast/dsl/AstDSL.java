/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.dsl;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Between;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.EqualTo;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.HighlightFunction;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Map;
import org.opensearch.sql.ast.expression.NestedAllTupleFields;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.PatternMethod;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.ScoreFunction;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedAttribute;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.DescribeRelation;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Patterns;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.RelationSubquery;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Values;

/** Class of static methods to create specific node instances. */
@UtilityClass
public class AstDSL {

  public static UnresolvedPlan filter(UnresolvedPlan input, UnresolvedExpression expression) {
    return new Filter(expression).attach(input);
  }

  public UnresolvedPlan relation(String tableName) {
    return new Relation(qualifiedName(tableName));
  }

  public UnresolvedPlan relation(List<String> tableNames) {
    return new Relation(
        tableNames.stream().map(AstDSL::qualifiedName).collect(Collectors.toList()));
  }

  public UnresolvedPlan relation(QualifiedName tableName) {
    return new Relation(tableName);
  }

  public UnresolvedPlan relation(String tableName, String alias) {
    return new SubqueryAlias(alias, new Relation(qualifiedName(tableName)));
  }

  public UnresolvedPlan describe(String tableName) {
    return new DescribeRelation(qualifiedName(tableName));
  }

  public UnresolvedPlan subqueryAlias(UnresolvedPlan child, String alias) {
    return new SubqueryAlias(child, alias);
  }

  public UnresolvedPlan tableFunction(List<String> functionName, UnresolvedExpression... args) {
    return new TableFunction(new QualifiedName(functionName), Arrays.asList(args));
  }

  public static UnresolvedPlan project(UnresolvedPlan input, UnresolvedExpression... projectList) {
    return new Project(Arrays.asList(projectList)).attach(input);
  }

  public static Eval eval(UnresolvedPlan input, Let... projectList) {
    return new Eval(Arrays.asList(projectList)).attach(input);
  }

  public static UnresolvedPlan projectWithArg(
      UnresolvedPlan input, List<Argument> argList, UnresolvedExpression... projectList) {
    return new Project(Arrays.asList(projectList), argList).attach(input);
  }

  public static UnresolvedPlan agg(
      UnresolvedPlan input,
      List<UnresolvedExpression> aggList,
      List<UnresolvedExpression> sortList,
      List<UnresolvedExpression> groupList,
      List<Argument> argList) {
    return new Aggregation(aggList, sortList, groupList, null, argList).attach(input);
  }

  public static UnresolvedPlan agg(
      UnresolvedPlan input,
      List<UnresolvedExpression> aggList,
      List<UnresolvedExpression> sortList,
      List<UnresolvedExpression> groupList,
      UnresolvedExpression span,
      List<Argument> argList) {
    return new Aggregation(aggList, sortList, groupList, span, argList).attach(input);
  }

  public static UnresolvedPlan rename(UnresolvedPlan input, Map... maps) {
    return new Rename(Arrays.asList(maps), input);
  }

  /**
   * Initialize Values node by rows of literals.
   *
   * @param values rows in which each row is a list of literal values
   * @return Values node
   */
  @SafeVarargs
  public UnresolvedPlan values(List<Literal>... values) {
    return new Values(Arrays.asList(values));
  }

  public static QualifiedName qualifiedName(String... parts) {
    return new QualifiedName(Arrays.asList(parts));
  }

  public static UnresolvedExpression equalTo(
      UnresolvedExpression left, UnresolvedExpression right) {
    return new EqualTo(left, right);
  }

  public static UnresolvedExpression unresolvedAttr(String attr) {
    return new UnresolvedAttribute(attr);
  }

  public static UnresolvedPlan relationSubquery(UnresolvedPlan subquery, String subqueryAlias) {
    return new RelationSubquery(subquery, subqueryAlias);
  }

  private static Literal literal(Object value, DataType type) {
    return new Literal(value, type);
  }

  public static Let let(Field var, UnresolvedExpression expression) {
    return new Let(var, expression);
  }

  public static Literal intLiteral(Integer value) {
    return literal(value, DataType.INTEGER);
  }

  public static Literal longLiteral(Long value) {
    return literal(value, DataType.LONG);
  }

  public static Literal shortLiteral(Short value) {
    return literal(value, DataType.SHORT);
  }

  public static Literal floatLiteral(Float value) {
    return literal(value, DataType.FLOAT);
  }

  public static Literal dateLiteral(String value) {
    return literal(value, DataType.DATE);
  }

  public static Literal timeLiteral(String value) {
    return literal(value, DataType.TIME);
  }

  public static Literal timestampLiteral(String value) {
    return literal(value, DataType.TIMESTAMP);
  }

  public static Literal doubleLiteral(Double value) {
    return literal(value, DataType.DOUBLE);
  }

  public static Literal stringLiteral(String value) {
    return literal(value, DataType.STRING);
  }

  public static Literal booleanLiteral(Boolean value) {
    return literal(value, DataType.BOOLEAN);
  }

  public static Interval intervalLiteral(Object value, DataType type, String unit) {
    return new Interval(literal(value, type), unit);
  }

  public static Literal nullLiteral() {
    return literal(null, DataType.NULL);
  }

  public static Map map(String origin, String target) {
    return new Map(field(origin), field(target));
  }

  public static Map map(UnresolvedExpression origin, UnresolvedExpression target) {
    return new Map(origin, target);
  }

  public static UnresolvedExpression aggregate(String func, UnresolvedExpression field) {
    return new AggregateFunction(func, field);
  }

  public static UnresolvedExpression aggregate(
      String func, UnresolvedExpression field, UnresolvedExpression... args) {
    return new AggregateFunction(func, field, Arrays.asList(args));
  }

  public static UnresolvedExpression filteredAggregate(
      String func, UnresolvedExpression field, UnresolvedExpression condition) {
    return new AggregateFunction(func, field).condition(condition);
  }

  public static UnresolvedExpression distinctAggregate(String func, UnresolvedExpression field) {
    return new AggregateFunction(func, field, true);
  }

  public static UnresolvedExpression filteredDistinctCount(
      String func, UnresolvedExpression field, UnresolvedExpression condition) {
    return new AggregateFunction(func, field, true).condition(condition);
  }

  public static Function function(String funcName, UnresolvedExpression... funcArgs) {
    return new Function(funcName, Arrays.asList(funcArgs));
  }

  /**
   *
   *
   * <pre>
   * CASE
   *    WHEN search_condition THEN result_expr<br>
   *    [WHEN search_condition THEN result_expr] ...
   *    [ELSE result_expr]
   * END
   * </pre>
   */
  public UnresolvedExpression caseWhen(UnresolvedExpression elseClause, When... whenClauses) {
    return caseWhen(null, elseClause, whenClauses);
  }

  /**
   *
   *
   * <pre>
   * CASE case_value_expr
   *     WHEN compare_expr THEN result_expr
   *     [WHEN compare_expr THEN result_expr] ...
   *     [ELSE result_expr]
   * END
   * </pre>
   */
  public UnresolvedExpression caseWhen(
      UnresolvedExpression caseValueExpr, UnresolvedExpression elseClause, When... whenClauses) {
    return new Case(caseValueExpr, Arrays.asList(whenClauses), Optional.ofNullable(elseClause));
  }

  public UnresolvedExpression cast(UnresolvedExpression expr, Literal type) {
    return new Cast(expr, type);
  }

  public When when(UnresolvedExpression condition, UnresolvedExpression result) {
    return new When(condition, result);
  }

  public UnresolvedExpression highlight(
      UnresolvedExpression fieldName, java.util.Map<String, Literal> arguments) {
    return new HighlightFunction(fieldName, arguments);
  }

  public UnresolvedExpression score(
      UnresolvedExpression relevanceQuery, Literal relevanceFieldWeight) {
    return new ScoreFunction(relevanceQuery, relevanceFieldWeight);
  }

  public UnresolvedExpression window(
      UnresolvedExpression function,
      List<UnresolvedExpression> partitionByList,
      List<Pair<SortOption, UnresolvedExpression>> sortList) {
    return new WindowFunction(function, partitionByList, sortList);
  }

  public static UnresolvedExpression not(UnresolvedExpression expression) {
    return new Not(expression);
  }

  public static UnresolvedExpression or(UnresolvedExpression left, UnresolvedExpression right) {
    return new Or(left, right);
  }

  public static UnresolvedExpression and(UnresolvedExpression left, UnresolvedExpression right) {
    return new And(left, right);
  }

  public static UnresolvedExpression xor(UnresolvedExpression left, UnresolvedExpression right) {
    return new Xor(left, right);
  }

  public static UnresolvedExpression in(
      UnresolvedExpression field, UnresolvedExpression... valueList) {
    return new In(field, Arrays.asList(valueList));
  }

  public static UnresolvedExpression in(
      UnresolvedExpression field, List<UnresolvedExpression> valueList) {
    return new In(field, valueList);
  }

  public static UnresolvedExpression compare(
      String operator, UnresolvedExpression left, UnresolvedExpression right) {
    return new Compare(operator, left, right);
  }

  public static UnresolvedExpression between(
      UnresolvedExpression value,
      UnresolvedExpression lowerBound,
      UnresolvedExpression upperBound) {
    return new Between(value, lowerBound, upperBound);
  }

  public static Argument argument(String argName, Literal argValue) {
    return new Argument(argName, argValue);
  }

  public static UnresolvedArgument unresolvedArg(String argName, UnresolvedExpression argValue) {
    return new UnresolvedArgument(argName, argValue);
  }

  public AllFields allFields() {
    return AllFields.of();
  }

  public Field field(UnresolvedExpression field) {
    return new Field(field);
  }

  public Field field(UnresolvedExpression field, Argument... fieldArgs) {
    return field(field, Arrays.asList(fieldArgs));
  }

  public Field field(String field) {
    return field(qualifiedName(field));
  }

  public Field field(String field, Argument... fieldArgs) {
    return field(field, Arrays.asList(fieldArgs));
  }

  public Field field(UnresolvedExpression field, List<Argument> fieldArgs) {
    return new Field(field, fieldArgs);
  }

  public Field field(String field, List<Argument> fieldArgs) {
    return field(qualifiedName(field), fieldArgs);
  }

  public Alias alias(String name, UnresolvedExpression expr) {
    return new Alias(name, expr);
  }

  @Deprecated
  public Alias alias(String name, UnresolvedExpression expr, String alias) {
    return new Alias(name, expr, alias);
  }

  public NestedAllTupleFields nestedAllTupleFields(String path) {
    return new NestedAllTupleFields(path);
  }

  public static List<UnresolvedExpression> exprList(UnresolvedExpression... exprList) {
    return Arrays.asList(exprList);
  }

  public static List<Argument> exprList(Argument... exprList) {
    return Arrays.asList(exprList);
  }

  public static List<UnresolvedArgument> unresolvedArgList(UnresolvedArgument... exprList) {
    return Arrays.asList(exprList);
  }

  public static List<Argument> defaultFieldsArgs() {
    return exprList(argument("exclude", booleanLiteral(false)));
  }

  /** Default Stats Command Args. */
  public static List<Argument> defaultStatsArgs() {
    return exprList(
        argument("partitions", intLiteral(1)),
        argument("allnum", booleanLiteral(false)),
        argument("delim", stringLiteral(" ")),
        argument("dedupsplit", booleanLiteral(false)));
  }

  /** Default Dedup Command Args. */
  public static List<Argument> defaultDedupArgs() {
    return exprList(
        argument("number", intLiteral(1)),
        argument("keepempty", booleanLiteral(false)),
        argument("consecutive", booleanLiteral(false)));
  }

  public static List<Argument> sortOptions() {
    return exprList(argument("desc", booleanLiteral(false)));
  }

  public static List<Argument> defaultSortFieldArgs() {
    return exprList(argument("asc", booleanLiteral(true)), argument("type", nullLiteral()));
  }

  public static Span span(UnresolvedExpression field, UnresolvedExpression value, SpanUnit unit) {
    return new Span(field, value, unit);
  }

  public static Sort sort(UnresolvedPlan input, Field... sorts) {
    return new Sort(input, Arrays.asList(sorts));
  }

  public static Dedupe dedupe(UnresolvedPlan input, List<Argument> options, Field... fields) {
    return new Dedupe(input, options, Arrays.asList(fields));
  }

  public static Head head(UnresolvedPlan input, Integer size, Integer from) {
    return new Head(input, size, from);
  }

  public static List<Argument> defaultTopArgs() {
    return exprList(argument("noOfResults", intLiteral(10)));
  }

  public static RareTopN rareTopN(
      UnresolvedPlan input,
      CommandType commandType,
      List<Argument> noOfResults,
      List<UnresolvedExpression> groupList,
      Field... fields) {
    return new RareTopN(input, commandType, noOfResults, Arrays.asList(fields), groupList)
        .attach(input);
  }

  public static Limit limit(UnresolvedPlan input, Integer limit, Integer offset) {
    return new Limit(limit, offset).attach(input);
  }

  public static Trendline trendline(
      UnresolvedPlan input,
      Optional<Field> sortField,
      Trendline.TrendlineComputation... computations) {
    return new Trendline(sortField, Arrays.asList(computations)).attach(input);
  }

  public static Trendline.TrendlineComputation computation(
      Integer numDataPoints, Field dataField, String alias, Trendline.TrendlineType type) {
    return new Trendline.TrendlineComputation(numDataPoints, dataField, alias, type);
  }

  public static Parse parse(
      UnresolvedPlan input,
      ParseMethod parseMethod,
      UnresolvedExpression sourceField,
      Literal pattern,
      java.util.Map<String, Literal> arguments) {
    return new Parse(parseMethod, sourceField, pattern, arguments, input);
  }

  public static Patterns patterns(
      UnresolvedPlan input,
      PatternMethod patternMethod,
      UnresolvedExpression sourceField,
      String alias,
      List<Argument> arguments) {
    List<UnresolvedExpression> funArgs = new ArrayList<>();
    funArgs.add(sourceField);
    funArgs.addAll(arguments);
    return new Patterns(
        new Alias(
            alias,
            new WindowFunction(
                new Function(patternMethod.name().toLowerCase(Locale.ROOT), funArgs),
                List.of(),
                List.of()),
            alias),
        input);
  }

  public static FillNull fillNull(UnresolvedExpression replaceNullWithMe, Field... fields) {
    return new FillNull(
        FillNull.ContainNullableFieldFill.ofSameValue(
            replaceNullWithMe, ImmutableList.copyOf(fields)));
  }

  public static FillNull fillNull(
      List<ImmutablePair<Field, UnresolvedExpression>> fieldAndReplacements) {
    ImmutableList.Builder<FillNull.NullableFieldFill> replacementsBuilder = ImmutableList.builder();
    for (ImmutablePair<Field, UnresolvedExpression> fieldAndReplacement : fieldAndReplacements) {
      replacementsBuilder.add(
          new FillNull.NullableFieldFill(
              fieldAndReplacement.getLeft(), fieldAndReplacement.getRight()));
    }
    return new FillNull(
        FillNull.ContainNullableFieldFill.ofVariousValue(replacementsBuilder.build()));
  }
}
