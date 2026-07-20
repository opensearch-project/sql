/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.aggregation;

import java.io.IOException;
import java.util.Map;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.metrics.MetricAggregatorSupplier;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

/** Factory for the native checked BIGINT sum aggregation. */
class CheckedLongSumAggregatorFactory extends ValuesSourceAggregatorFactory {

  CheckedLongSumAggregatorFactory(
      String name,
      ValuesSourceConfig config,
      QueryShardContext queryShardContext,
      AggregatorFactory parent,
      AggregatorFactories.Builder subFactoriesBuilder,
      Map<String, Object> metadata)
      throws IOException {
    super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
  }

  @Override
  protected Aggregator createUnmapped(
      SearchContext searchContext, Aggregator parent, Map<String, Object> metadata)
      throws IOException {
    return new CheckedLongSumAggregator(name, config, searchContext, parent, metadata);
  }

  @Override
  protected Aggregator doCreateInternal(
      SearchContext searchContext,
      Aggregator parent,
      CardinalityUpperBound bucketCardinality,
      Map<String, Object> metadata)
      throws IOException {
    MetricAggregatorSupplier supplier =
        queryShardContext
            .getValuesSourceRegistry()
            .getAggregator(CheckedLongSumAggregationBuilder.REGISTRY_KEY, config);
    return supplier.build(name, config, searchContext, parent, metadata);
  }

  @Override
  protected boolean supportsConcurrentSegmentSearch() {
    return true;
  }
}
