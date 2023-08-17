/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.opensearch.response.AggregationResponseUtils.fromJson;
import static org.opensearch.sql.opensearch.response.agg.Utils.handleNanInfValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.search.aggregations.metrics.ExtendedStats;
import org.opensearch.sql.opensearch.response.agg.CompositeAggregationParser;
import org.opensearch.sql.opensearch.response.agg.FilterParser;
import org.opensearch.sql.opensearch.response.agg.NoBucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.response.agg.SingleValueParser;
import org.opensearch.sql.opensearch.response.agg.StatsParser;
import org.opensearch.sql.opensearch.response.agg.TopHitsParser;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchAggregationResponseParserTest {

  /** SELECT MAX(age) as max FROM accounts. */
  @Test
  void no_bucket_one_metric_should_pass() {
    String response =
        "{\n"
            + "  \"max#max\": {\n"
            + "    \"value\": 40\n"
            + "  }\n"
            + "}";
    NoBucketAggregationParser parser = new NoBucketAggregationParser(new SingleValueParser("max"));
    assertThat(parse(parser, response), contains(entry("max", 40d)));
  }

  /** SELECT MAX(age) as max, MIN(age) as min FROM accounts. */
  @Test
  void no_bucket_two_metric_should_pass() {
    String response =
        "{\n"
            + "  \"max#max\": {\n"
            + "    \"value\": 40\n"
            + "  },\n"
            + "  \"min#min\": {\n"
            + "    \"value\": 20\n"
            + "  }\n"
            + "}";
    NoBucketAggregationParser parser =
        new NoBucketAggregationParser(new SingleValueParser("max"), new SingleValueParser("min"));
    assertThat(parse(parser, response), contains(entry("max", 40d, "min", 20d)));
  }

  @Test
  void one_bucket_one_metric_should_pass() {
    String response =
        "{\n"
            + "  \"composite#composite_buckets\": {\n"
            + "    \"after_key\": {\n"
            + "      \"type\": \"sale\"\n"
            + "    },\n"
            + "    \"buckets\": [\n"
            + "      {\n"
            + "        \"key\": {\n"
            + "          \"type\": \"cost\"\n"
            + "        },\n"
            + "        \"doc_count\": 2,\n"
            + "        \"avg#avg\": {\n"
            + "          \"value\": 20\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"key\": {\n"
            + "          \"type\": \"sale\"\n"
            + "        },\n"
            + "        \"doc_count\": 2,\n"
            + "        \"avg#avg\": {\n"
            + "          \"value\": 105\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";

    OpenSearchAggregationResponseParser parser =
        new CompositeAggregationParser(new SingleValueParser("avg"));
    assertThat(
        parse(parser, response),
        containsInAnyOrder(
            ImmutableMap.of("type", "cost", "avg", 20d),
            ImmutableMap.of("type", "sale", "avg", 105d)));
  }

  @Test
  void two_bucket_one_metric_should_pass() {
    String response =
        "{\n"
            + "  \"composite#composite_buckets\": {\n"
            + "    \"after_key\": {\n"
            + "      \"type\": \"sale\",\n"
            + "      \"region\": \"us\"\n"
            + "    },\n"
            + "    \"buckets\": [\n"
            + "      {\n"
            + "        \"key\": {\n"
            + "          \"type\": \"cost\",\n"
            + "          \"region\": \"us\"\n"
            + "        },\n"
            + "        \"avg#avg\": {\n"
            + "          \"value\": 20\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"key\": {\n"
            + "          \"type\": \"sale\",\n"
            + "          \"region\": \"uk\"\n"
            + "        },\n"
            + "        \"avg#avg\": {\n"
            + "          \"value\": 130\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    OpenSearchAggregationResponseParser parser =
        new CompositeAggregationParser(new SingleValueParser("avg"));
    assertThat(
        parse(parser, response),
        containsInAnyOrder(
            ImmutableMap.of("type", "cost", "region", "us", "avg", 20d),
            ImmutableMap.of("type", "sale", "region", "uk", "avg", 130d)));
  }

  @Test
  void unsupported_aggregation_should_fail() {
    String response =
        "{\n"
            + "  \"date_histogram#date_histogram\": {\n"
            + "    \"value\": 40\n"
            + "  }\n"
            + "}";
    NoBucketAggregationParser parser = new NoBucketAggregationParser(new SingleValueParser("max"));
    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> parse(parser, response));
    assertEquals(
        "couldn't parse field date_histogram in aggregation response", exception.getMessage());
  }

  @Test
  void nan_value_should_return_null() {
    assertNull(handleNanInfValue(Double.NaN));
    assertNull(handleNanInfValue(Double.NEGATIVE_INFINITY));
    assertNull(handleNanInfValue(Double.POSITIVE_INFINITY));
  }

  @Test
  void filter_aggregation_should_pass() {
    String response =
        "{\n"
            + "    \"filter#filtered\" : {\n"
            + "      \"doc_count\" : 3,\n"
            + "      \"avg#filtered\" : {\n"
            + "        \"value\" : 37.0\n"
            + "      }\n"
            + "    }\n"
            + "  }";
    OpenSearchAggregationResponseParser parser =
        new NoBucketAggregationParser(
            FilterParser.builder()
                .name("filtered")
                .metricsParser(new SingleValueParser("filtered"))
                .build());
    assertThat(parse(parser, response), contains(entry("filtered", 37.0)));
  }

  @Test
  void filter_aggregation_group_by_should_pass() {
    String response =
        "{\n"
            + "  \"composite#composite_buckets\":{\n"
            + "    \"after_key\":{\n"
            + "      \"gender\":\"m\"\n"
            + "    },\n"
            + "    \"buckets\":[\n"
            + "      {\n"
            + "        \"key\":{\n"
            + "          \"gender\":\"f\"\n"
            + "        },\n"
            + "        \"doc_count\":3,\n"
            + "        \"filter#filter\":{\n"
            + "          \"doc_count\":1,\n"
            + "          \"avg#avg\":{\n"
            + "            \"value\":39.0\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"key\":{\n"
            + "          \"gender\":\"m\"\n"
            + "        },\n"
            + "        \"doc_count\":4,\n"
            + "        \"filter#filter\":{\n"
            + "          \"doc_count\":2,\n"
            + "          \"avg#avg\":{\n"
            + "            \"value\":36.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    OpenSearchAggregationResponseParser parser =
        new CompositeAggregationParser(
            FilterParser.builder()
                .name("filter")
                .metricsParser(new SingleValueParser("avg"))
                .build());
    assertThat(
        parse(parser, response),
        containsInAnyOrder(entry("gender", "f", "avg", 39.0), entry("gender", "m", "avg", 36.0)));
  }

  /** SELECT MAX(age) as max, STDDEV(age) as min FROM accounts. */
  @Test
  void no_bucket_max_and_extended_stats() {
    String response =
        "{\n"
            + "  \"extended_stats#esField\": {\n"
            + "    \"count\": 2033,\n"
            + "    \"min\": 0,\n"
            + "    \"max\": 360,\n"
            + "    \"avg\": 45.47958681751107,\n"
            + "    \"sum\": 92460,\n"
            + "    \"sum_of_squares\": 22059450,\n"
            + "    \"variance\": 8782.295820390027,\n"
            + "    \"variance_population\": 8782.295820390027,\n"
            + "    \"variance_sampling\": 8786.61781636463,\n"
            + "    \"std_deviation\": 93.71390409320287,\n"
            + "    \"std_deviation_population\": 93.71390409320287,\n"
            + "    \"std_deviation_sampling\": 93.73696078049805,\n"
            + "    \"std_deviation_bounds\": {\n"
            + "      \"upper\": 232.9073950039168,\n"
            + "      \"lower\": -141.94822136889468,\n"
            + "      \"upper_population\": 232.9073950039168,\n"
            + "      \"lower_population\": -141.94822136889468,\n"
            + "      \"upper_sampling\": 232.95350837850717,\n"
            + "      \"lower_sampling\": -141.99433474348504\n"
            + "    }\n"
            + "  },\n"
            + "  \"max#maxField\": {\n"
            + "    \"value\": 360\n"
            + "  }\n"
            + "}";

    NoBucketAggregationParser parser =
        new NoBucketAggregationParser(
            new SingleValueParser("maxField"),
            new StatsParser(ExtendedStats::getStdDeviation, "esField"));
    assertThat(
        parse(parser, response), contains(entry("esField", 93.71390409320287, "maxField", 360D)));
  }

  @Test
  void top_hits_aggregation_should_pass() {
    String response =
        "{\n"
            + "  \"composite#composite_buckets\": {\n"
            + "    \"buckets\": [\n"
            + "      {\n"
            + "        \"key\": {\n"
            + "          \"type\": \"take\"\n"
            + "        },\n"
            + "        \"doc_count\": 2,\n"
            + "        \"top_hits#take\": {\n"
            + "          \"hits\": {\n"
            + "            \"total\": { \"value\": 2, \"relation\": \"eq\" },\n"
            + "            \"max_score\": 1.0,\n"
            + "            \"hits\": [\n"
            + "              {\n"
            + "                \"_index\": \"accounts\",\n"
            + "                \"_id\": \"1\",\n"
            + "                \"_score\": 1.0,\n"
            + "                \"_source\": {\n"
            + "                  \"gender\": \"m\"\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"_index\": \"accounts\",\n"
            + "                \"_id\": \"2\",\n"
            + "                \"_score\": 1.0,\n"
            + "                \"_source\": {\n"
            + "                  \"gender\": \"f\"\n"
            + "                }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    OpenSearchAggregationResponseParser parser =
        new CompositeAggregationParser(new TopHitsParser("take"));
    assertThat(
        parse(parser, response),
        contains(ImmutableMap.of("type", "take", "take", ImmutableList.of("m", "f"))));
  }

  public List<Map<String, Object>> parse(OpenSearchAggregationResponseParser parser, String json) {
    return parser.parse(fromJson(json));
  }

  public Map<String, Object> entry(String name, Object value) {
    return ImmutableMap.of(name, value);
  }

  public Map<String, Object> entry(String name, Object value, String name2, Object value2) {
    return ImmutableMap.of(name, value, name2, value2);
  }
}
