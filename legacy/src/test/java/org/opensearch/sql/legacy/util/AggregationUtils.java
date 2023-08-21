/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ParsedAvg;
import org.opensearch.search.aggregations.metrics.ParsedMax;
import org.opensearch.search.aggregations.metrics.ParsedMin;
import org.opensearch.search.aggregations.metrics.ParsedSum;
import org.opensearch.search.aggregations.metrics.ParsedValueCount;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.ParsedPercentilesBucket;
import org.opensearch.search.aggregations.pipeline.PercentilesBucketPipelineAggregationBuilder;

public class AggregationUtils {
  private static final List<NamedXContentRegistry.Entry> entryList =
      new ImmutableMap.Builder<String, ContextParser<Object, ? extends Aggregation>>()
              .put(MinAggregationBuilder.NAME, (p, c) -> ParsedMin.fromXContent(p, (String) c))
              .put(MaxAggregationBuilder.NAME, (p, c) -> ParsedMax.fromXContent(p, (String) c))
              .put(SumAggregationBuilder.NAME, (p, c) -> ParsedSum.fromXContent(p, (String) c))
              .put(AvgAggregationBuilder.NAME, (p, c) -> ParsedAvg.fromXContent(p, (String) c))
              .put(StringTerms.NAME, (p, c) -> ParsedStringTerms.fromXContent(p, (String) c))
              .put(LongTerms.NAME, (p, c) -> ParsedLongTerms.fromXContent(p, (String) c))
              .put(DoubleTerms.NAME, (p, c) -> ParsedDoubleTerms.fromXContent(p, (String) c))
              .put(
                  ValueCountAggregationBuilder.NAME,
                  (p, c) -> ParsedValueCount.fromXContent(p, (String) c))
              .put(
                  PercentilesBucketPipelineAggregationBuilder.NAME,
                  (p, c) -> ParsedPercentilesBucket.fromXContent(p, (String) c))
              .put(
                  DateHistogramAggregationBuilder.NAME,
                  (p, c) -> ParsedDateHistogram.fromXContent(p, (String) c))
              .build()
              .entrySet()
              .stream()
              .map(
                  entry ->
                      new NamedXContentRegistry.Entry(
                          Aggregation.class, new ParseField(entry.getKey()), entry.getValue()))
              .collect(Collectors.toList());
  private static final NamedXContentRegistry namedXContentRegistry =
      new NamedXContentRegistry(entryList);

  /**
   * Populate {@link Aggregations} from JSON string.
   *
   * @param json json string
   * @return {@link Aggregations}
   */
  public static Aggregations fromJson(String json) {
    try {
      XContentParser xContentParser =
          new JsonXContentParser(
              namedXContentRegistry,
              LoggingDeprecationHandler.INSTANCE,
              new JsonFactory().createParser(json));
      xContentParser.nextToken();
      return Aggregations.fromXContent(xContentParser);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
