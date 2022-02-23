/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.aggregation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.opensearch.response.agg.CompositeAggregationParser;
import org.opensearch.sql.opensearch.response.agg.MetricParser;
import org.opensearch.sql.opensearch.response.agg.NoBucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.storage.script.aggregation.dsl.BucketAggregationBuilder;
import org.opensearch.sql.opensearch.storage.script.aggregation.dsl.MetricAggregationBuilder;
import org.opensearch.sql.opensearch.storage.serialization.ExpressionSerializer;

/**
 * Build the AggregationBuilder from the list of {@link NamedAggregator}
 * and list of {@link NamedExpression}.
 */
@RequiredArgsConstructor
public class AggregationQueryBuilder extends ExpressionNodeVisitor<AggregationBuilder, Object> {

  /**
   * How many composite buckets should be returned.
   */
  public static final int AGGREGATION_BUCKET_SIZE = 1000;

  /**
   * Bucket Aggregation builder.
   */
  private final BucketAggregationBuilder bucketBuilder;

  /**
   * Metric Aggregation builder.
   */
  private final MetricAggregationBuilder metricBuilder;

  /**
   * Aggregation Query Builder Constructor.
   */
  public AggregationQueryBuilder(
      ExpressionSerializer serializer) {
    this.bucketBuilder = new BucketAggregationBuilder(serializer);
    this.metricBuilder = new MetricAggregationBuilder(serializer);
  }

  /** Build AggregationBuilder. */
  public Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser>
      buildAggregationBuilder(
          List<NamedAggregator> namedAggregatorList,
          List<NamedExpression> groupByList,
          List<Pair<Sort.SortOption, Expression>> sortList) {

    final Pair<AggregatorFactories.Builder, List<MetricParser>> metrics =
        metricBuilder.build(namedAggregatorList);

    if (groupByList.isEmpty()) {
      // no bucket
      return Pair.of(
          ImmutableList.copyOf(metrics.getLeft().getAggregatorFactories()),
          new NoBucketAggregationParser(metrics.getRight()));
    } else {
      GroupSortOrder groupSortOrder = new GroupSortOrder(sortList);
      return Pair.of(
          Collections.singletonList(
              AggregationBuilders.composite(
                      "composite_buckets",
                      bucketBuilder.build(
                          groupByList.stream()
                              .sorted(groupSortOrder)
                              .map(expr -> Pair.of(expr, groupSortOrder.apply(expr)))
                              .collect(Collectors.toList())))
                  .subAggregations(metrics.getLeft())
                  .size(AGGREGATION_BUCKET_SIZE)),
          new CompositeAggregationParser(metrics.getRight()));
    }
  }

  /**
   * Build ElasticsearchExprValueFactory.
   */
  public Map<String, ExprType> buildTypeMapping(
      List<NamedAggregator> namedAggregatorList,
      List<NamedExpression> groupByList) {
    ImmutableMap.Builder<String, ExprType> builder = new ImmutableMap.Builder<>();
    namedAggregatorList.forEach(agg -> builder.put(agg.getName(), agg.type()));
    groupByList.forEach(group -> builder.put(group.getNameOrAlias(), group.type()));
    return builder.build();
  }

  @VisibleForTesting
  public static class GroupSortOrder implements Comparator<NamedExpression>,
      Function<NamedExpression, SortOrder> {

    /**
     * The default order of group field.
     * The order is ASC NULL_FIRST.
     * The field should be the last one in the group list.
     */
    private static final Pair<SortOrder, Integer> DEFAULT_ORDER =
        Pair.of(SortOrder.ASC, Integer.MAX_VALUE);

    /**
     * The mapping betwen {@link Sort.SortOption} and {@link SortOrder}.
     */
    private static final Map<Sort.SortOption, SortOrder> SORT_MAP =
        new ImmutableMap.Builder<Sort.SortOption, SortOrder>()
            .put(Sort.SortOption.DEFAULT_ASC, SortOrder.ASC)
            .put(Sort.SortOption.DEFAULT_DESC, SortOrder.DESC).build();

    private final Map<String, Pair<SortOrder, Integer>> map = new HashMap<>();

    /**
     * Constructor of GroupSortOrder.
     */
    public GroupSortOrder(List<Pair<Sort.SortOption, Expression>> sortList) {
      if (null == sortList) {
        return;
      }
      int pos = 0;
      for (Pair<Sort.SortOption, Expression> sortPair : sortList) {
        map.put(((ReferenceExpression) sortPair.getRight()).getAttr(),
            Pair.of(SORT_MAP.getOrDefault(sortPair.getLeft(), SortOrder.ASC), pos++));
      }
    }

    /**
     * Compare the two expressions. The comparison is based on the pos in the sort list.
     * If the expression is defined in the sort list. then the order of the expression is the pos
     * in sort list.
     * If the expression isn't defined in the sort list. the the order of the expression is the
     * Integer.MAX_VALUE. you can think it is at the end of the sort list.
     *
     * @param o1 NamedExpression
     * @param o2 NamedExpression
     * @return -1, o1 before o2. 1, o1 after o2. 0 o1, o2 has same position.
     */
    @Override
    public int compare(NamedExpression o1, NamedExpression o2) {
      final Pair<SortOrder, Integer> o1Value =
          map.getOrDefault(o1.getName(), DEFAULT_ORDER);
      final Pair<SortOrder, Integer> o2Value =
          map.getOrDefault(o2.getName(), DEFAULT_ORDER);
      return o1Value.getRight().compareTo(o2Value.getRight());
    }

    /**
     * Get the {@link SortOrder} for expression.
     * By default, the {@link SortOrder} is ASC.
     */
    @Override
    public SortOrder apply(NamedExpression expression) {
      return map.getOrDefault(expression.getName(), DEFAULT_ORDER).getLeft();
    }
  }
}
