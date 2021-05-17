/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.planner.logical;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

/**
 * Logical Plan DSL.
 */
@UtilityClass
public class LogicalPlanDSL {

  public static LogicalPlan aggregation(
      LogicalPlan input, List<NamedAggregator> aggregatorList, List<NamedExpression> groupByList) {
    return new LogicalAggregation(input, aggregatorList, groupByList);
  }

  public static LogicalPlan filter(LogicalPlan input, Expression expression) {
    return new LogicalFilter(input, expression);
  }

  public static LogicalPlan relation(String tableName) {
    return new LogicalRelation(tableName);
  }

  public static LogicalPlan rename(
      LogicalPlan input, Map<ReferenceExpression, ReferenceExpression> renameMap) {
    return new LogicalRename(input, renameMap);
  }

  public static LogicalPlan project(LogicalPlan input, NamedExpression... fields) {
    return new LogicalProject(input, Arrays.asList(fields));
  }

  public LogicalPlan window(LogicalPlan input,
                            NamedExpression windowFunction,
                            WindowDefinition windowDefinition) {
    return new LogicalWindow(input, windowFunction, windowDefinition);
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
