/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.aggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.metrics.MetricAggregatorSupplier;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.aggregations.support.ValuesSourceType;

/** Builder for the native checked BIGINT sum aggregation. */
public class CheckedLongSumAggregationBuilder
    extends ValuesSourceAggregationBuilder.LeafOnly<
        ValuesSource.Numeric, CheckedLongSumAggregationBuilder> {

  public static final String NAME = "checked_long_sum";
  public static final ValuesSourceRegistry.RegistryKey<MetricAggregatorSupplier> REGISTRY_KEY =
      new ValuesSourceRegistry.RegistryKey<>(NAME, MetricAggregatorSupplier.class);

  public static final ObjectParser<CheckedLongSumAggregationBuilder, String> PARSER =
      ObjectParser.fromBuilder(NAME, CheckedLongSumAggregationBuilder::new);

  static {
    ValuesSourceAggregationBuilder.declareFields(PARSER, false, false, false);
  }

  public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
    builder.register(
        REGISTRY_KEY, List.of(CoreValuesSourceType.NUMERIC), CheckedLongSumAggregator::new, true);
  }

  public CheckedLongSumAggregationBuilder(String name) {
    super(name);
  }

  public CheckedLongSumAggregationBuilder(StreamInput in) throws IOException {
    super(in);
  }

  private CheckedLongSumAggregationBuilder(
      CheckedLongSumAggregationBuilder clone,
      AggregatorFactories.Builder factoriesBuilder,
      Map<String, Object> metadata) {
    super(clone, factoriesBuilder, metadata);
  }

  @Override
  protected ValuesSourceType defaultValueSourceType() {
    return CoreValuesSourceType.NUMERIC;
  }

  @Override
  protected AggregationBuilder shallowCopy(
      AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
    return new CheckedLongSumAggregationBuilder(this, factoriesBuilder, metadata);
  }

  @Override
  protected void innerWriteTo(StreamOutput out) {}

  @Override
  protected CheckedLongSumAggregatorFactory innerBuild(
      QueryShardContext queryShardContext,
      ValuesSourceConfig config,
      AggregatorFactory parent,
      AggregatorFactories.Builder subFactoriesBuilder)
      throws IOException {
    return new CheckedLongSumAggregatorFactory(
        name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
  }

  @Override
  public XContentBuilder doXContentBody(XContentBuilder builder, Params params) {
    return builder;
  }

  @Override
  public String getType() {
    return NAME;
  }

  @Override
  protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
    return REGISTRY_KEY;
  }
}
