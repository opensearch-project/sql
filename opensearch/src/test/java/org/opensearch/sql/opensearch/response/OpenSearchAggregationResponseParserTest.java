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
import org.opensearch.sql.opensearch.response.agg.BucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.CompositeAggregationParser;
import org.opensearch.sql.opensearch.response.agg.FilterParser;
import org.opensearch.sql.opensearch.response.agg.NoBucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.response.agg.PercentilesParser;
import org.opensearch.sql.opensearch.response.agg.SinglePercentileParser;
import org.opensearch.sql.opensearch.response.agg.SingleValueParser;
import org.opensearch.sql.opensearch.response.agg.StatsParser;
import org.opensearch.sql.opensearch.response.agg.TopHitsParser;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchAggregationResponseParserTest {

  /** SELECT MAX(age) as max FROM accounts. */
  @Test
  void no_bucket_one_metric_should_pass() {
    String response = "{\n" + "  \"max#max\": {\n" + "    \"value\": 40\n" + "  }\n" + "}";
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
            + "  \"sterms#type\": {\n"
            + "    \"doc_count_error_upper_bound\": 0,\n"
            + "    \"sum_other_doc_count\": 0,\n"
            + "    \"buckets\": [\n"
            + "      {\n"
            + "        \"key\": \"cost\",\n"
            + "        \"doc_count\": 2,\n"
            + "        \"avg#avg\": {\n"
            + "          \"value\": 20\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"key\": \"sale\",\n"
            + "        \"doc_count\": 2,\n"
            + "        \"avg#avg\": {\n"
            + "          \"value\": 105\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";

    OpenSearchAggregationResponseParser parser =
        new BucketAggregationParser(new SingleValueParser("avg"));
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
        contains(
            ImmutableMap.of("type", "cost", "region", "us"), ImmutableMap.of("avg", 20d),
            ImmutableMap.of("type", "sale", "region", "uk"), ImmutableMap.of("avg", 130d)));
  }

  @Test
  void unsupported_aggregation_should_fail() {
    String response =
        "{\n" + "  \"date_histogram#date_histogram\": {\n" + "    \"value\": 40\n" + "  }\n" + "}";
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
            + "  \"sterms#gender\": {\n"
            + "    \"doc_count_error_upper_bound\": 0,\n"
            + "    \"sum_other_doc_count\": 0,\n"
            + "    \"buckets\":[\n"
            + "      {\n"
            + "        \"key\":\"f\",\n"
            + "        \"doc_count\":3,\n"
            + "        \"filter#filter\":{\n"
            + "          \"doc_count\":1,\n"
            + "          \"avg#avg\":{\n"
            + "            \"value\":39.0\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"key\":\"m\",\n"
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
        new BucketAggregationParser(
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
            + "                \"fields\": {\n"
            + "                  \"gender\": [\"m\"]\n"
            + "                }\n"
            + "              },\n"
            + "              {\n"
            + "                \"_index\": \"accounts\",\n"
            + "                \"_id\": \"2\",\n"
            + "                \"_score\": 1.0,\n"
            + "                \"fields\": {\n"
            + "                  \"gender\": [\"f\"]\n"
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
        new CompositeAggregationParser(new TopHitsParser("take", false, true));
    assertThat(
        parse(parser, response),
        contains(
            ImmutableMap.of("type", "take"), ImmutableMap.of("take", ImmutableList.of("m", "f"))));
  }

  /** SELECT PERCENTILE(age, 50) FROM accounts. */
  @Test
  void no_bucket_one_metric_percentile_should_pass() {
    String response =
        "{\n"
            + "      \"percentiles#percentile\": {\n"
            + "        \"values\": {\n"
            + "          \"50.0\": 35.0\n"
            + "        }\n"
            + "      }\n"
            + "    }";
    NoBucketAggregationParser parser =
        new NoBucketAggregationParser(new SinglePercentileParser("percentile"));
    assertThat(parse(parser, response), contains(entry("percentile", 35.0)));
  }

  /** SELECT PERCENTILE(age, 50), MAX(age) FROM accounts. */
  @Test
  void no_bucket_two_metric_percentile_should_pass() {
    String response =
        "{\n"
            + "      \"percentiles#percentile\": {\n"
            + "        \"values\": {\n"
            + "          \"50.0\": 35.0\n"
            + "        }\n"
            + "      },\n"
            + "      \"max#max\": {\n"
            + "        \"value\": 40\n"
            + "      }\n"
            + "    }";
    NoBucketAggregationParser parser =
        new NoBucketAggregationParser(
            new SinglePercentileParser("percentile"), new SingleValueParser("max"));
    assertThat(parse(parser, response), contains(entry("percentile", 35.0, "max", 40.0)));
  }

  /** SELECT PERCENTILE(age, 50) FROM accounts GROUP BY type. */
  @Test
  void one_bucket_one_metric_percentile_should_pass() {
    String response =
        "{\n"
            + "  \"sterms#type\": {\n"
            + "    \"doc_count_error_upper_bound\": 0,\n"
            + "    \"sum_other_doc_count\": 0,\n"
            + "    \"buckets\": [\n"
            + "      {\n"
            + "        \"key\": \"cost\",\n"
            + "        \"doc_count\": 2,\n"
            + "        \"percentiles#percentile\": {\n"
            + "          \"values\": {\n"
            + "              \"50.0\": 40.0\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"key\": \"sale\",\n"
            + "        \"doc_count\": 2,\n"
            + "        \"percentiles#percentile\": {\n"
            + "          \"values\": {\n"
            + "              \"50.0\": 100.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";

    OpenSearchAggregationResponseParser parser =
        new BucketAggregationParser(new SinglePercentileParser("percentile"));
    assertThat(
        parse(parser, response),
        containsInAnyOrder(
            ImmutableMap.of("type", "cost", "percentile", 40d),
            ImmutableMap.of("type", "sale", "percentile", 100d)));
  }

  /** SELECT PERCENTILE(age, 50) FROM accounts GROUP BY type, region. */
  @Test
  void two_bucket_one_metric_percentile_should_pass() {
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
            + "        \"doc_count\": 2,\n"
            + "        \"percentiles#percentile\": {\n"
            + "          \"values\": {\n"
            + "              \"50.0\": 40.0\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"key\": {\n"
            + "          \"type\": \"sale\",\n"
            + "          \"region\": \"uk\"\n"
            + "        },\n"
            + "        \"doc_count\": 2,\n"
            + "        \"percentiles#percentile\": {\n"
            + "          \"values\": {\n"
            + "              \"50.0\": 100.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";

    OpenSearchAggregationResponseParser parser =
        new CompositeAggregationParser(
            new SinglePercentileParser("percentile"), new SingleValueParser("max"));
    assertThat(
        parse(parser, response),
        contains(
            ImmutableMap.of("type", "cost", "region", "us"),
            ImmutableMap.of("percentile", 40d),
            ImmutableMap.of("type", "sale", "region", "uk"),
            ImmutableMap.of("percentile", 100d)));
  }

  /** SELECT PERCENTILES(age) FROM accounts. */
  @Test
  void no_bucket_percentiles_should_pass() {
    String response =
        "{\n"
            + "      \"percentiles#percentiles\": {\n"
            + "        \"values\": {\n"
            + "          \"1.0\": 21.0,\n"
            + "          \"5.0\": 27.0,\n"
            + "          \"25.0\": 30.0,\n"
            + "          \"50.0\": 35.0,\n"
            + "          \"75.0\": 55.0,\n"
            + "          \"95.0\": 58.0,\n"
            + "          \"99.0\": 60.0\n"
            + "        }\n"
            + "      }\n"
            + "    }";
    NoBucketAggregationParser parser =
        new NoBucketAggregationParser(new PercentilesParser("percentiles"));
    assertThat(
        parse(parser, response),
        contains(entry("percentiles", List.of(21.0, 27.0, 30.0, 35.0, 55.0, 58.0, 60.0))));
  }

  /** SELECT PERCENTILES(age) FROM accounts GROUP BY type. */
  @Test
  void one_bucket_percentiles_should_pass() {
    String response =
        "{\n"
            + "  \"sterms#type\": {\n"
            + "    \"doc_count_error_upper_bound\": 0,\n"
            + "    \"sum_other_doc_count\": 0,\n"
            + "    \"buckets\": [\n"
            + "      {\n"
            + "        \"key\": \"cost\",\n"
            + "        \"doc_count\": 2,\n"
            + "        \"percentiles#percentiles\": {\n"
            + "          \"values\": {\n"
            + "            \"1.0\": 21.0,\n"
            + "            \"5.0\": 27.0,\n"
            + "            \"25.0\": 30.0,\n"
            + "            \"50.0\": 35.0,\n"
            + "            \"75.0\": 55.0,\n"
            + "            \"95.0\": 58.0,\n"
            + "            \"99.0\": 60.0\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"key\": \"sale\",\n"
            + "        \"doc_count\": 2,\n"
            + "        \"percentiles#percentiles\": {\n"
            + "          \"values\": {\n"
            + "            \"1.0\": 21.0,\n"
            + "            \"5.0\": 27.0,\n"
            + "            \"25.0\": 30.0,\n"
            + "            \"50.0\": 35.0,\n"
            + "            \"75.0\": 55.0,\n"
            + "            \"95.0\": 58.0,\n"
            + "            \"99.0\": 60.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";

    OpenSearchAggregationResponseParser parser =
        new BucketAggregationParser(new PercentilesParser("percentiles"));
    assertThat(
        parse(parser, response),
        containsInAnyOrder(
            ImmutableMap.of(
                "type", "cost", "percentiles", List.of(21.0, 27.0, 30.0, 35.0, 55.0, 58.0, 60.0)),
            ImmutableMap.of(
                "type", "sale", "percentiles", List.of(21.0, 27.0, 30.0, 35.0, 55.0, 58.0, 60.0))));
  }

  /** SELECT PERCENTILES(age) FROM accounts GROUP BY type, region. */
  @Test
  void two_bucket_percentiles_should_pass() {
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
            + "        \"doc_count\": 2,\n"
            + "        \"percentiles#percentiles\": {\n"
            + "          \"values\": {\n"
            + "            \"1.0\": 21.0,\n"
            + "            \"5.0\": 27.0,\n"
            + "            \"25.0\": 30.0,\n"
            + "            \"50.0\": 35.0,\n"
            + "            \"75.0\": 55.0,\n"
            + "            \"95.0\": 58.0,\n"
            + "            \"99.0\": 60.0\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"key\": {\n"
            + "          \"type\": \"sale\",\n"
            + "          \"region\": \"uk\"\n"
            + "        },\n"
            + "        \"doc_count\": 2,\n"
            + "        \"percentiles#percentiles\": {\n"
            + "          \"values\": {\n"
            + "            \"1.0\": 21.0,\n"
            + "            \"5.0\": 27.0,\n"
            + "            \"25.0\": 30.0,\n"
            + "            \"50.0\": 35.0,\n"
            + "            \"75.0\": 55.0,\n"
            + "            \"95.0\": 58.0,\n"
            + "            \"99.0\": 60.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";

    OpenSearchAggregationResponseParser parser =
        new CompositeAggregationParser(new PercentilesParser("percentiles"));
    assertThat(
        parse(parser, response),
        contains(
            ImmutableMap.of("type", "cost", "region", "us"),
                ImmutableMap.of("percentiles", List.of(21.0, 27.0, 30.0, 35.0, 55.0, 58.0, 60.0)),
            ImmutableMap.of("type", "sale", "region", "uk"),
                ImmutableMap.of("percentiles", List.of(21.0, 27.0, 30.0, 35.0, 55.0, 58.0, 60.0))));
  }

  /**
   * Dedup pushdown (LITERAL_AGG) with a renamed field: {@code rename value as val | dedup
   * category}. The top_hits response returns {@code _source: {category, value}} but the output
   * schema expects {@code val}. The {@code fieldNameMapping} must translate {@code value -> val}.
   *
   * <p>Regression test for https://github.com/opensearch-project/sql/issues/5150
   */
  @Test
  void top_hits_field_name_mapping_single_rename_should_pass() {
    String response =
        "{\n"
            + "  \"composite#composite_buckets\": {\n"
            + "    \"buckets\": [\n"
            + "      {\n"
            + "        \"key\": { \"category\": \"A\" },\n"
            + "        \"doc_count\": 2,\n"
            + "        \"top_hits#dedup\": {\n"
            + "          \"hits\": {\n"
            + "            \"total\": { \"value\": 2, \"relation\": \"eq\" },\n"
            + "            \"hits\": [\n"
            + "              {\n"
            + "                \"_index\": \"idx\",\n"
            + "                \"_id\": \"1\",\n"
            + "                \"fields\": { \"value\": [10.5] }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";

    Map<String, List<String>> mapping = Map.of("value", List.of("val"));
    OpenSearchAggregationResponseParser parser =
        new CompositeAggregationParser(new TopHitsParser("dedup", false, false, mapping));
    assertThat(
        parse(parser, response),
        contains(ImmutableMap.of("category", "A"), ImmutableMap.of("val", 10.5)));
  }

  /**
   * Dedup pushdown (LITERAL_AGG) where two output names ({@code pay} from rename, {@code pay2} from
   * eval column-ref) both resolve to the same original field {@code salary}. The old {@code
   * Map<String,String>} approach silently dropped one mapping on collision; with {@code
   * Map<String,List<String>>} both aliases must appear in the result.
   *
   * <p>Regression test for https://github.com/opensearch-project/sql/issues/5197
   */
  @Test
  void top_hits_field_name_mapping_collision_should_duplicate_value() {
    String response =
        "{\n"
            + "  \"composite#composite_buckets\": {\n"
            + "    \"buckets\": [\n"
            + "      {\n"
            + "        \"key\": { \"dept_id\": \"eng\" },\n"
            + "        \"doc_count\": 3,\n"
            + "        \"top_hits#dedup\": {\n"
            + "          \"hits\": {\n"
            + "            \"total\": { \"value\": 3, \"relation\": \"eq\" },\n"
            + "            \"hits\": [\n"
            + "              {\n"
            + "                \"_index\": \"idx\",\n"
            + "                \"_id\": \"1\",\n"
            + "                \"fields\": { \"salary\": [50000] }\n"
            + "              }\n"
            + "            ]\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";

    // salary -> [pay, pay2]: both rename and eval-column-ref resolve to the same source field
    Map<String, List<String>> mapping = Map.of("salary", List.of("pay", "pay2"));
    OpenSearchAggregationResponseParser parser =
        new CompositeAggregationParser(new TopHitsParser("dedup", false, false, mapping));

    List<Map<String, Object>> result = parse(parser, response);
    // Bucket key row
    assertThat(result.get(0), org.hamcrest.Matchers.hasEntry("dept_id", "eng"));
    // Hit row must contain both aliases with the same value; original key must be absent
    Map<String, Object> hitRow = result.get(1);
    assertEquals(50000, hitRow.get("pay"));
    assertEquals(50000, hitRow.get("pay2"));
    assertNull(hitRow.get("salary"), "original field name must be removed after mapping");
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
