/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation.dsl;

import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStats;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.opensearch.response.agg.FilterParser;
import org.opensearch.sql.opensearch.response.agg.MetricParser;
import org.opensearch.sql.opensearch.response.agg.SingleValueParser;
import org.opensearch.sql.opensearch.response.agg.StatsParser;
import org.opensearch.sql.opensearch.response.agg.TopHitsParser;
import org.opensearch.sql.opensearch.storage.script.filter.FilterQueryBuilder;
import org.opensearch.sql.opensearch.storage.serialization.ExpressionSerializer;

/** Build the Metric Aggregation and List of {@link MetricParser} from {@link NamedAggregator}. */
public class MetricAggregationBuilder
    extends ExpressionNodeVisitor<Pair<AggregationBuilder, MetricParser>, Object> {

  private final AggregationBuilderHelper helper;
  private final FilterQueryBuilder filterBuilder;

  /** Constructor. */
  public MetricAggregationBuilder(ExpressionSerializer serializer) {
    this.helper = new AggregationBuilderHelper(serializer);
    this.filterBuilder = new FilterQueryBuilder(serializer);
  }

  /**
   * Build AggregatorFactories.Builder from {@link NamedAggregator}.
   *
   * @param aggregatorList aggregator list
   * @return AggregatorFactories.Builder
   */
  public Pair<AggregatorFactories.Builder, List<MetricParser>> build(
      List<NamedAggregator> aggregatorList) {
    AggregatorFactories.Builder builder = new AggregatorFactories.Builder();
    List<MetricParser> metricParserList = new ArrayList<>();
    for (NamedAggregator aggregator : aggregatorList) {
      Pair<AggregationBuilder, MetricParser> pair = aggregator.accept(this, null);
      builder.addAggregator(pair.getLeft());
      metricParserList.add(pair.getRight());
    }
    return Pair.of(builder, metricParserList);
  }

  @Override
  public Pair<AggregationBuilder, MetricParser> visitNamedAggregator(
      NamedAggregator node, Object context) {
    Expression expression = node.getArguments().get(0);
    Expression condition = node.getDelegated().condition();
    Boolean distinct = node.getDelegated().distinct();
    String name = node.getName();
    String functionName = node.getFunctionName().getFunctionName().toLowerCase(Locale.ROOT);

    if (distinct) {
      switch (functionName) {
        case "count":
          return make(
              AggregationBuilders.cardinality(name),
              expression,
              condition,
              name,
              new SingleValueParser(name));
        default:
          throw new IllegalStateException(
              String.format(
                  "unsupported distinct aggregator %s", node.getFunctionName().getFunctionName()));
      }
    }

    switch (functionName) {
      case "avg":
        return make(
            AggregationBuilders.avg(name),
            expression,
            condition,
            name,
            new SingleValueParser(name));
      case "sum":
        return make(
            AggregationBuilders.sum(name),
            expression,
            condition,
            name,
            new SingleValueParser(name));
      case "count":
        return make(
            AggregationBuilders.count(name),
            replaceStarOrLiteral(expression),
            condition,
            name,
            new SingleValueParser(name));
      case "min":
        return make(
            AggregationBuilders.min(name),
            expression,
            condition,
            name,
            new SingleValueParser(name));
      case "max":
        return make(
            AggregationBuilders.max(name),
            expression,
            condition,
            name,
            new SingleValueParser(name));
      case "var_samp":
        return make(
            AggregationBuilders.extendedStats(name),
            expression,
            condition,
            name,
            new StatsParser(ExtendedStats::getVarianceSampling, name));
      case "var_pop":
        return make(
            AggregationBuilders.extendedStats(name),
            expression,
            condition,
            name,
            new StatsParser(ExtendedStats::getVariancePopulation, name));
      case "stddev_samp":
        return make(
            AggregationBuilders.extendedStats(name),
            expression,
            condition,
            name,
            new StatsParser(ExtendedStats::getStdDeviationSampling, name));
      case "stddev_pop":
        return make(
            AggregationBuilders.extendedStats(name),
            expression,
            condition,
            name,
            new StatsParser(ExtendedStats::getStdDeviationPopulation, name));
      case "take":
        return make(
            AggregationBuilders.topHits(name),
            expression,
            node.getArguments().get(1),
            condition,
            name,
            new TopHitsParser(name));
      default:
        throw new IllegalStateException(
            String.format("unsupported aggregator %s", node.getFunctionName().getFunctionName()));
    }
  }

  private Pair<AggregationBuilder, MetricParser> make(
      ValuesSourceAggregationBuilder<?> builder,
      Expression expression,
      Expression condition,
      String name,
      MetricParser parser) {
    ValuesSourceAggregationBuilder aggregationBuilder =
        helper.build(expression, builder::field, builder::script);
    if (condition != null) {
      return Pair.of(
          makeFilterAggregation(aggregationBuilder, condition, name),
          FilterParser.builder().name(name).metricsParser(parser).build());
    }
    return Pair.of(aggregationBuilder, parser);
  }

  /** Make {@link CardinalityAggregationBuilder} for distinct count aggregations. */
  private Pair<AggregationBuilder, MetricParser> make(
      CardinalityAggregationBuilder builder,
      Expression expression,
      Expression condition,
      String name,
      MetricParser parser) {
    CardinalityAggregationBuilder aggregationBuilder =
        helper.build(expression, builder::field, builder::script);
    if (condition != null) {
      return Pair.of(
          makeFilterAggregation(aggregationBuilder, condition, name),
          FilterParser.builder().name(name).metricsParser(parser).build());
    }
    return Pair.of(aggregationBuilder, parser);
  }

  /** Make {@link TopHitsAggregationBuilder} for take aggregations. */
  private Pair<AggregationBuilder, MetricParser> make(
      TopHitsAggregationBuilder builder,
      Expression expression,
      Expression size,
      Expression condition,
      String name,
      MetricParser parser) {
    String fieldName = ((ReferenceExpression) expression).getAttr();
    builder.fetchSource(fieldName, null);
    builder.size(size.valueOf().integerValue());
    builder.from(0);
    if (condition != null) {
      return Pair.of(
          makeFilterAggregation(builder, condition, name),
          FilterParser.builder().name(name).metricsParser(parser).build());
    }
    return Pair.of(builder, parser);
  }

  /**
   * Replace star or literal with OpenSearch metadata field "_index". Because: 1) Analyzer already
   * converts * to string literal, literal check here can handle both COUNT(*) and COUNT(1). 2)
   * Value count aggregation on _index counts all docs (after filter), therefore it has same
   * semantics as COUNT(*) or COUNT(1).
   *
   * @param countArg count function argument
   * @return Reference to _index if literal, otherwise return original argument expression
   */
  private Expression replaceStarOrLiteral(Expression countArg) {
    if (countArg instanceof LiteralExpression) {
      return new ReferenceExpression("_index", INTEGER);
    }
    return countArg;
  }

  /**
   * Make builder to build FilterAggregation for aggregations with filter in the bucket.
   *
   * @param subAggBuilder AggregationBuilder instance which the filter is applied to.
   * @param condition Condition expression in the filter.
   * @param name Name of the FilterAggregation instance to build.
   * @return {@link FilterAggregationBuilder}.
   */
  private FilterAggregationBuilder makeFilterAggregation(
      AggregationBuilder subAggBuilder, Expression condition, String name) {
    return AggregationBuilders.filter(name, filterBuilder.build(condition))
        .subAggregation(subAggBuilder);
  }
}
