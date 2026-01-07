/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.QueryExpression;
import org.opensearch.sql.opensearch.response.agg.FilterParser;
import org.opensearch.sql.opensearch.response.agg.MetricParser;

/** Analyzer for converting aggregate filter conditions into OpenSearch filter aggregations. */
@RequiredArgsConstructor
public class AggregateFilterAnalyzer {

  /** Helper containing row type, field types, and cluster context for analysis. */
  private final AggregateAnalyzer.AggregateBuilderHelper helper;

  /** Project containing filter expressions referenced by aggregate calls. */
  private final Project project;

  /**
   * Analyzes and applies filter to aggregation if the AggregateCall has a filter condition.
   *
   * @param aggResult the base aggregation and parser to potentially wrap with filter
   * @param aggCall the aggregate call which may contain filter information
   * @param aggName name for the filtered aggregation
   * @return wrapped aggregation with filter if present, otherwise the original result
   * @throws PredicateAnalyzer.ExpressionNotAnalyzableException if filter condition cannot be
   *     analyzed
   */
  public Pair<AggregationBuilder, MetricParser> analyze(
      Pair<AggregationBuilder, MetricParser> aggResult, AggregateCall aggCall, String aggName)
      throws PredicateAnalyzer.ExpressionNotAnalyzableException {
    if (project == null || !aggCall.hasFilter()) {
      return aggResult;
    }

    QueryExpression queryExpression = analyzeAggregateFilter(aggCall);
    return Pair.of(
        buildFilterAggregation(aggResult.getLeft(), aggName, queryExpression),
        buildFilterParser(aggResult.getRight(), aggName));
  }

  private QueryExpression analyzeAggregateFilter(AggregateCall aggCall)
      throws PredicateAnalyzer.ExpressionNotAnalyzableException {
    RexNode filterCondition = project.getProjects().get(aggCall.filterArg);
    return PredicateAnalyzer.analyzeExpression(
        filterCondition,
        helper.rowType.getFieldNames(),
        helper.fieldTypes,
        helper.rowType,
        helper.cluster);
  }

  private AggregationBuilder buildFilterAggregation(
      AggregationBuilder aggBuilder, String aggName, QueryExpression queryExpression) {
    return AggregationBuilders.filter(aggName, queryExpression.builder())
        .subAggregation(aggBuilder);
  }

  private MetricParser buildFilterParser(MetricParser aggParser, String aggName) {
    return FilterParser.builder().name(aggName).metricsParser(aggParser).build();
  }
}
