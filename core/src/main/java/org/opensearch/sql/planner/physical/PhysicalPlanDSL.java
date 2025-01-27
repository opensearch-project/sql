/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.tree.RareTopN.CommandType;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.window.WindowDefinition;

/** Physical Plan DSL. */
@UtilityClass
public class PhysicalPlanDSL {

  public static AggregationOperator agg(
      PhysicalPlan input, List<NamedAggregator> aggregators, List<NamedExpression> groups) {
    return new AggregationOperator(input, aggregators, groups);
  }

  public static FilterOperator filter(PhysicalPlan input, Expression condition) {
    return new FilterOperator(input, condition);
  }

  public static RenameOperator rename(
      PhysicalPlan input, Map<ReferenceExpression, ReferenceExpression> renameMap) {
    return new RenameOperator(input, renameMap);
  }

  public static ProjectOperator project(PhysicalPlan input, NamedExpression... fields) {
    return new ProjectOperator(input, Arrays.asList(fields), ImmutableList.of());
  }

  public static ProjectOperator project(
      PhysicalPlan input,
      List<NamedExpression> fields,
      List<NamedExpression> namedParseExpressions) {
    return new ProjectOperator(input, fields, namedParseExpressions);
  }

  public static RemoveOperator remove(PhysicalPlan input, ReferenceExpression... fields) {
    return new RemoveOperator(input, ImmutableSet.copyOf(fields));
  }

  public static EvalOperator eval(
      PhysicalPlan input, Pair<ReferenceExpression, Expression>... expressions) {
    return new EvalOperator(input, Arrays.asList(expressions));
  }

  public FlattenOperator flatten(PhysicalPlan input, ReferenceExpression field) {
    return new FlattenOperator(input, field);
  }

  public static SortOperator sort(PhysicalPlan input, Pair<SortOption, Expression>... sorts) {
    return new SortOperator(input, Arrays.asList(sorts));
  }

  public static TakeOrderedOperator takeOrdered(
      PhysicalPlan input, Integer limit, Integer offset, Pair<SortOption, Expression>... sorts) {
    return new TakeOrderedOperator(input, limit, offset, Arrays.asList(sorts));
  }

  public static DedupeOperator dedupe(PhysicalPlan input, Expression... expressions) {
    return new DedupeOperator(input, Arrays.asList(expressions));
  }

  public static DedupeOperator dedupe(
      PhysicalPlan input,
      int allowedDuplication,
      boolean keepEmpty,
      boolean consecutive,
      Expression... expressions) {
    return new DedupeOperator(
        input, Arrays.asList(expressions), allowedDuplication, keepEmpty, consecutive);
  }

  public WindowOperator window(
      PhysicalPlan input, NamedExpression windowFunction, WindowDefinition windowDefinition) {
    return new WindowOperator(input, windowFunction, windowDefinition);
  }

  public static RareTopNOperator rareTopN(
      PhysicalPlan input,
      CommandType commandType,
      List<Expression> groups,
      Expression... expressions) {
    return new RareTopNOperator(input, commandType, Arrays.asList(expressions), groups);
  }

  public static RareTopNOperator rareTopN(
      PhysicalPlan input,
      CommandType commandType,
      int noOfResults,
      List<Expression> groups,
      Expression... expressions) {
    return new RareTopNOperator(
        input, commandType, noOfResults, Arrays.asList(expressions), groups);
  }

  @SafeVarargs
  public ValuesOperator values(List<LiteralExpression>... values) {
    return new ValuesOperator(Arrays.asList(values));
  }

  public static LimitOperator limit(PhysicalPlan input, Integer limit, Integer offset) {
    return new LimitOperator(input, limit, offset);
  }

  public static NestedOperator nested(
      PhysicalPlan input, Set<String> args, Map<String, List<String>> groupedFieldsByPath) {
    return new NestedOperator(input, args, groupedFieldsByPath);
  }
}
