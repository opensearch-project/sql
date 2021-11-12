/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.analysis;

import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_ASC;
import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_DESC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.ASC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.DESC;

import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.planner.logical.LogicalWindow;

/**
 * Window expression analyzer that analyzes window function expression in expression list
 * in project operator.
 */
@RequiredArgsConstructor
public class WindowExpressionAnalyzer extends AbstractNodeVisitor<LogicalPlan, AnalysisContext> {

  /**
   * Expression analyzer.
   */
  private final ExpressionAnalyzer expressionAnalyzer;

  /**
   * Child node to be wrapped by a new window operator.
   */
  private final LogicalPlan child;

  /**
   * Analyze the given project item and return window operator (with child node inside)
   * if the given project item is a window function.
   * @param projectItem   project item
   * @param context       analysis context
   * @return              window operator or original child if not windowed
   */
  public LogicalPlan analyze(UnresolvedExpression projectItem, AnalysisContext context) {
    LogicalPlan window = projectItem.accept(this, context);
    return (window == null) ? child : window;
  }

  @Override
  public LogicalPlan visitAlias(Alias node, AnalysisContext context) {
    if (!(node.getDelegated() instanceof WindowFunction)) {
      return null;
    }

    WindowFunction unresolved = (WindowFunction) node.getDelegated();
    Expression windowFunction = expressionAnalyzer.analyze(unresolved, context);
    List<Expression> partitionByList = analyzePartitionList(unresolved, context);
    List<Pair<SortOption, Expression>> sortList = analyzeSortList(unresolved, context);

    WindowDefinition windowDefinition = new WindowDefinition(partitionByList, sortList);
    NamedExpression namedWindowFunction =
        new NamedExpression(node.getName(), windowFunction, node.getAlias());
    List<Pair<SortOption, Expression>> allSortItems = windowDefinition.getAllSortItems();

    if (allSortItems.isEmpty()) {
      return new LogicalWindow(child, namedWindowFunction, windowDefinition);
    }
    return new LogicalWindow(
        new LogicalSort(child, allSortItems),
        namedWindowFunction,
        windowDefinition);
  }

  private List<Expression> analyzePartitionList(WindowFunction node, AnalysisContext context) {
    return node.getPartitionByList()
               .stream()
               .map(expr -> expressionAnalyzer.analyze(expr, context))
               .collect(Collectors.toList());
  }

  private List<Pair<SortOption, Expression>> analyzeSortList(WindowFunction node,
                                                             AnalysisContext context) {
    return node.getSortList()
               .stream()
               .map(pair -> ImmutablePair
                   .of(analyzeSortOption(pair.getLeft()),
                       expressionAnalyzer.analyze(pair.getRight(), context)))
               .collect(Collectors.toList());
  }

  /**
   * Frontend creates sort option from query directly which means sort or null order may be null.
   * The final and default value for each is determined here during expression analysis.
   */
  private SortOption analyzeSortOption(SortOption option) {
    if (option.getNullOrder() == null) {
      return (option.getSortOrder() == DESC) ? DEFAULT_DESC : DEFAULT_ASC;
    }
    return new SortOption(
        (option.getSortOrder() == DESC) ? DESC : ASC,
        option.getNullOrder());
  }

}
