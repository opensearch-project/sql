/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation;

import static java.util.Collections.singletonList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.response.agg.CompositeAggregationParser;
import org.opensearch.sql.opensearch.response.agg.MetricParser;
import org.opensearch.sql.opensearch.response.agg.NoBucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.storage.script.aggregation.dsl.BucketAggregationBuilder;
import org.opensearch.sql.opensearch.storage.script.aggregation.dsl.MetricAggregationBuilder;
import org.opensearch.sql.opensearch.storage.serialization.ExpressionSerializer;

/**
 * Build the AggregationBuilder from the list of {@link NamedAggregator} and list of {@link
 * NamedExpression}.
 */
@RequiredArgsConstructor
public class AggregationQueryBuilder extends ExpressionNodeVisitor<AggregationBuilder, Object> {

  /** How many composite buckets should be returned. */
  public static final int AGGREGATION_BUCKET_SIZE = 1000;

  /** Bucket Aggregation builder. */
  private final BucketAggregationBuilder bucketBuilder;

  /** Metric Aggregation builder. */
  private final MetricAggregationBuilder metricBuilder;

  /** Aggregation Query Builder Constructor. */
  public AggregationQueryBuilder(ExpressionSerializer serializer) {
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
          singletonList(
              AggregationBuilders.composite(
                      "composite_buckets",
                      bucketBuilder.build(
                          groupByList.stream()
                              .sorted(groupSortOrder)
                              .map(
                                  expr ->
                                      Triple.of(
                                          expr,
                                          groupSortOrder.sortOrder(expr),
                                          groupSortOrder.missingOrder(expr)))
                              .collect(Collectors.toList())))
                  .subAggregations(metrics.getLeft())
                  .size(AGGREGATION_BUCKET_SIZE)),
          new CompositeAggregationParser(metrics.getRight()));
    }
  }

  /** Build mapping for OpenSearchExprValueFactory. */
  public Map<String, OpenSearchDataType> buildTypeMapping(
      List<NamedAggregator> namedAggregatorList, List<NamedExpression> groupByList) {
    ImmutableMap.Builder<String, OpenSearchDataType> builder = new ImmutableMap.Builder<>();
    namedAggregatorList.forEach(
        agg -> builder.put(agg.getName(), OpenSearchDataType.of(agg.type())));
    groupByList.forEach(
        group -> builder.put(group.getNameOrAlias(), OpenSearchDataType.of(group.type())));
    return builder.build();
  }

  /** Group By field sort order. */
  @VisibleForTesting
  public static class GroupSortOrder implements Comparator<NamedExpression> {

    /**
     * The default order of group field. The order is ASC NULL_FIRST. The field should be the last
     * one in the group list.
     */
    private static final Pair<Sort.SortOption, Integer> DEFAULT_ORDER =
        Pair.of(Sort.SortOption.DEFAULT_ASC, Integer.MAX_VALUE);

    /** The mapping between {@link Sort.SortOrder} and {@link SortOrder}. */
    private static final Map<Sort.SortOrder, SortOrder> SORT_MAP =
        new ImmutableMap.Builder<Sort.SortOrder, SortOrder>()
            .put(Sort.SortOrder.ASC, SortOrder.ASC)
            .put(Sort.SortOrder.DESC, SortOrder.DESC)
            .build();

    /** The mapping between {@link Sort.NullOrder} and {@link MissingOrder}. */
    private static final Map<Sort.NullOrder, MissingOrder> NULL_MAP =
        new ImmutableMap.Builder<Sort.NullOrder, MissingOrder>()
            .put(Sort.NullOrder.NULL_FIRST, MissingOrder.FIRST)
            .put(Sort.NullOrder.NULL_LAST, MissingOrder.LAST)
            .build();

    private final Map<String, Pair<Sort.SortOption, Integer>> map = new HashMap<>();

    /** Constructor of GroupSortOrder. */
    public GroupSortOrder(List<Pair<Sort.SortOption, Expression>> sortList) {
      if (null == sortList) {
        return;
      }
      int pos = 0;
      for (Pair<Sort.SortOption, Expression> sortPair : sortList) {
        map.put(
            ((ReferenceExpression) sortPair.getRight()).getAttr(),
            Pair.of(sortPair.getLeft(), pos++));
      }
    }

    /**
     * Compare the two expressions. The comparison is based on the pos in the sort list. If the
     * expression is defined in the sort list. then the order of the expression is the pos in sort
     * list. If the expression isn't defined in the sort list. the the order of the expression is
     * the Integer.MAX_VALUE. you can think it is at the end of the sort list.
     *
     * @param o1 NamedExpression
     * @param o2 NamedExpression
     * @return -1, o1 before o2. 1, o1 after o2. 0 o1, o2 has same position.
     */
    @Override
    public int compare(NamedExpression o1, NamedExpression o2) {
      final Pair<Sort.SortOption, Integer> o1Value = map.getOrDefault(o1.getName(), DEFAULT_ORDER);
      final Pair<Sort.SortOption, Integer> o2Value = map.getOrDefault(o2.getName(), DEFAULT_ORDER);
      return o1Value.getRight().compareTo(o2Value.getRight());
    }

    /** Get the {@link SortOrder} for expression. By default, the {@link SortOrder} is ASC. */
    public SortOrder sortOrder(NamedExpression expression) {
      return SORT_MAP.get(sortOption(expression).getSortOrder());
    }

    /**
     * Get the {@link MissingOrder} for expression. By default, the {@link MissingOrder} is ASC
     * missing first / DESC missing last.
     */
    public MissingOrder missingOrder(NamedExpression expression) {
      return NULL_MAP.get(sortOption(expression).getNullOrder());
    }

    private Sort.SortOption sortOption(NamedExpression expression) {
      return map.getOrDefault(expression.getName(), DEFAULT_ORDER).getLeft();
    }
  }
}
