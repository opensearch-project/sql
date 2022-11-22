/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.storage.Table;

/**
 * Logical Plan DSL.
 */
@UtilityClass
public class LogicalPlanDSL {

  public static LogicalPlan write(LogicalPlan input, Table table) {
    return new LogicalWrite(input, table);
  }

  public static LogicalPlan aggregation(
      LogicalPlan input, List<NamedAggregator> aggregatorList, List<NamedExpression> groupByList) {
    return new LogicalAggregation(input, aggregatorList, groupByList);
  }

  public static LogicalPlan filter(LogicalPlan input, Expression expression) {
    return new LogicalFilter(input, expression);
  }

  public static LogicalPlan relation(String tableName, Table table) {
    return new LogicalRelation(tableName, table);
  }

  public static LogicalPlan rename(
      LogicalPlan input, Map<ReferenceExpression, ReferenceExpression> renameMap) {
    return new LogicalRename(input, renameMap);
  }

  public static LogicalPlan project(LogicalPlan input, NamedExpression... fields) {
    return new LogicalProject(input, Arrays.asList(fields), ImmutableList.of());
  }

  public static LogicalPlan project(LogicalPlan input, List<NamedExpression> fields,
                                    List<NamedExpression> namedParseExpressions) {
    return new LogicalProject(input, fields, namedParseExpressions);
  }

  public LogicalPlan window(LogicalPlan input,
                            NamedExpression windowFunction,
                            WindowDefinition windowDefinition) {
    return new LogicalWindow(input, windowFunction, windowDefinition);
  }

  public LogicalPlan highlight(LogicalPlan input, Expression field,
      Map<String, Literal> arguments) {
    return new LogicalHighlight(input, field, arguments);
  }

  public static LogicalPlan remove(LogicalPlan input, ReferenceExpression... fields) {
    return new LogicalRemove(input, ImmutableSet.copyOf(fields));
  }

  public static LogicalPlan eval(
      LogicalPlan input, Pair<ReferenceExpression, Expression>... expressions) {
    return new LogicalEval(input, Arrays.asList(expressions));
  }

  public static LogicalPlan sort(LogicalPlan input, Pair<SortOption, Expression>... sorts) {
    return new LogicalSort(input, Arrays.asList(sorts));
  }

  public static LogicalPlan dedupe(LogicalPlan input, Expression... fields) {
    return dedupe(input, 1, false, false, fields);
  }

  public static LogicalPlan dedupe(
      LogicalPlan input,
      int allowedDuplication,
      boolean keepEmpty,
      boolean consecutive,
      Expression... fields) {
    return new LogicalDedupe(
        input, Arrays.asList(fields), allowedDuplication, keepEmpty, consecutive);
  }

  public static LogicalPlan rareTopN(LogicalPlan input, CommandType commandType,
                                     List<Expression> groupByList, Expression... fields) {
    return rareTopN(input, commandType, 10, groupByList, fields);
  }

  public static LogicalPlan rareTopN(LogicalPlan input, CommandType commandType, int noOfResults,
                                     List<Expression> groupByList, Expression... fields) {
    return new LogicalRareTopN(input, commandType, noOfResults, Arrays.asList(fields), groupByList);
  }

  @SafeVarargs
  public LogicalPlan values(List<LiteralExpression>... values) {
    return new LogicalValues(Arrays.asList(values));
  }

  public static LogicalPlan limit(LogicalPlan input, Integer limit, Integer offset) {
    return new LogicalLimit(input, limit, offset);
  }

}
