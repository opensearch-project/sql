/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.aggregation.dsl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.common.utils.StringUtils.format;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.expression.aggregation.StdDevAggregator.stddevPopulation;
import static org.opensearch.sql.expression.aggregation.StdDevAggregator.stddevSample;
import static org.opensearch.sql.expression.aggregation.VarianceAggregator.variancePopulation;
import static org.opensearch.sql.expression.aggregation.VarianceAggregator.varianceSample;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.aggregation.AvgAggregator;
import org.opensearch.sql.expression.aggregation.CountAggregator;
import org.opensearch.sql.expression.aggregation.MaxAggregator;
import org.opensearch.sql.expression.aggregation.MinAggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.aggregation.SumAggregator;
import org.opensearch.sql.expression.aggregation.TakeAggregator;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.storage.serialization.ExpressionSerializer;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class MetricAggregationBuilderTest {

  @Mock
  private ExpressionSerializer serializer;

  @Mock
  private NamedAggregator aggregator;

  private MetricAggregationBuilder aggregationBuilder;

  @BeforeEach
  void set_up() {
    aggregationBuilder = new MetricAggregationBuilder(serializer);
  }

  @Test
  void should_build_avg_aggregation() {
    assertEquals(format(
        "{%n"
            + "  \"avg(age)\" : {%n"
            + "    \"avg\" : {%n"
            + "      \"field\" : \"age\"%n"
            + "    }%n"
            + "  }%n"
            + "}"),
        buildQuery(
            Arrays.asList(
                named("avg(age)",
                    new AvgAggregator(Arrays.asList(ref("age", INTEGER)), INTEGER)))));
  }

  @Test
  void should_build_sum_aggregation() {
    assertEquals(format(
        "{%n"
            + "  \"sum(age)\" : {%n"
            + "    \"sum\" : {%n"
            + "      \"field\" : \"age\"%n"
            + "    }%n"
            + "  }%n"
            + "}"),
        buildQuery(
            Arrays.asList(
                named("sum(age)",
                    new SumAggregator(Arrays.asList(ref("age", INTEGER)), INTEGER)))));
  }

  @Test
  void should_build_count_aggregation() {
    assertEquals(format(
        "{%n"
            + "  \"count(age)\" : {%n"
            + "    \"value_count\" : {%n"
            + "      \"field\" : \"age\"%n"
            + "    }%n"
            + "  }%n"
            + "}"),
        buildQuery(
            Arrays.asList(
                named("count(age)",
                    new CountAggregator(Arrays.asList(ref("age", INTEGER)), INTEGER)))));
  }

  @Test
  void should_build_count_star_aggregation() {
    assertEquals(format(
        "{%n"
            + "  \"count(*)\" : {%n"
            + "    \"value_count\" : {%n"
            + "      \"field\" : \"_index\"%n"
            + "    }%n"
            + "  }%n"
            + "}"),
        buildQuery(
            Arrays.asList(
                named("count(*)",
                    new CountAggregator(Arrays.asList(literal("*")), INTEGER)))));
  }

  @Test
  void should_build_count_other_literal_aggregation() {
    assertEquals(format(
        "{%n"
            + "  \"count(1)\" : {%n"
            + "    \"value_count\" : {%n"
            + "      \"field\" : \"_index\"%n"
            + "    }%n"
            + "  }%n"
            + "}"),
        buildQuery(
            Arrays.asList(
                named("count(1)",
                    new CountAggregator(Arrays.asList(literal(1)), INTEGER)))));
  }

  @Test
  void should_build_min_aggregation() {
    assertEquals(format(
        "{%n"
            + "  \"min(age)\" : {%n"
            + "    \"min\" : {%n"
            + "      \"field\" : \"age\"%n"
            + "    }%n"
            + "  }%n"
            + "}"),
        buildQuery(
            Arrays.asList(
                named("min(age)",
                    new MinAggregator(Arrays.asList(ref("age", INTEGER)), INTEGER)))));
  }

  @Test
  void should_build_max_aggregation() {
    assertEquals(format(
        "{%n"
            + "  \"max(age)\" : {%n"
            + "    \"max\" : {%n"
            + "      \"field\" : \"age\"%n"
            + "    }%n"
            + "  }%n"
            + "}"),
        buildQuery(
            Arrays.asList(
                named("max(age)",
                    new MaxAggregator(Arrays.asList(ref("age", INTEGER)), INTEGER)))));
  }

  @Test
  void should_build_varPop_aggregation() {
    assertEquals(format(
        "{%n"
            + "  \"var_pop(age)\" : {%n"
            + "    \"extended_stats\" : {%n"
            + "      \"field\" : \"age\",%n"
            + "      \"sigma\" : 2.0%n"
            + "    }%n"
            + "  }%n"
            + "}"),
        buildQuery(
            Arrays.asList(
                named("var_pop(age)",
                    variancePopulation(Arrays.asList(ref("age", INTEGER)), INTEGER)))));
  }

  @Test
  void should_build_varSamp_aggregation() {
    assertEquals(format(
        "{%n"
            + "  \"var_samp(age)\" : {%n"
            + "    \"extended_stats\" : {%n"
            + "      \"field\" : \"age\",%n"
            + "      \"sigma\" : 2.0%n"
            + "    }%n"
            + "  }%n"
            + "}"),
        buildQuery(
            Arrays.asList(
                named("var_samp(age)",
                    varianceSample(Arrays.asList(ref("age", INTEGER)), INTEGER)))));
  }

  @Test
  void should_build_stddevPop_aggregation() {
    assertEquals(format(
        "{%n"
            + "  \"stddev_pop(age)\" : {%n"
            + "    \"extended_stats\" : {%n"
            + "      \"field\" : \"age\",%n"
            + "      \"sigma\" : 2.0%n"
            + "    }%n"
            + "  }%n"
            + "}"),
        buildQuery(
            Arrays.asList(
                named("stddev_pop(age)",
                    stddevPopulation(Arrays.asList(ref("age", INTEGER)), INTEGER)))));
  }

  @Test
  void should_build_stddevSamp_aggregation() {
    assertEquals(format(
        "{%n"
            + "  \"stddev_samp(age)\" : {%n"
            + "    \"extended_stats\" : {%n"
            + "      \"field\" : \"age\",%n"
            + "      \"sigma\" : 2.0%n"
            + "    }%n"
            + "  }%n"
            + "}"),
        buildQuery(
            Arrays.asList(
                named("stddev_samp(age)",
                    stddevSample(Arrays.asList(ref("age", INTEGER)), INTEGER)))));
  }

  @Test
  void should_build_cardinality_aggregation() {
    assertEquals(format(
        "{%n"
            + "  \"count(distinct name)\" : {%n"
            + "    \"cardinality\" : {%n"
            + "      \"field\" : \"name\"%n"
            + "    }%n"
            + "  }%n"
            + "}"),
        buildQuery(
            Collections.singletonList(named("count(distinct name)", new CountAggregator(
                Collections.singletonList(ref("name", STRING)), INTEGER).distinct(true)))));
  }

  @Test
  void should_build_filtered_cardinality_aggregation() {
    assertEquals(format(
        "{%n"
            + "  \"count(distinct name) filter(where age > 30)\" : {%n"
            + "    \"filter\" : {%n"
            + "      \"range\" : {%n"
            + "        \"age\" : {%n"
            + "          \"from\" : 30,%n"
            + "          \"to\" : null,%n"
            + "          \"include_lower\" : false,%n"
            + "          \"include_upper\" : true,%n"
            + "          \"boost\" : 1.0%n"
            + "        }%n"
            + "      }%n"
            + "    },%n"
            + "    \"aggregations\" : {%n"
            + "      \"count(distinct name) filter(where age > 30)\" : {%n"
            + "        \"cardinality\" : {%n"
            + "          \"field\" : \"name\"%n"
            + "        }%n"
            + "      }%n"
            + "    }%n"
            + "  }%n"
            + "}"),
        buildQuery(Collections.singletonList(named(
            "count(distinct name) filter(where age > 30)",
            new CountAggregator(Collections.singletonList(ref("name", STRING)), INTEGER)
                .condition(DSL.greater(ref("age", INTEGER), literal(30)))
                .distinct(true)))));
  }

  @Test
  void should_build_top_hits_aggregation() {
    assertEquals(format(
        "{%n"
            + "  \"take(name, 10)\" : {%n"
            + "    \"top_hits\" : {%n"
            + "      \"from\" : 0,%n"
            + "      \"size\" : 10,%n"
            + "      \"version\" : false,%n"
            + "      \"seq_no_primary_term\" : false,%n"
            + "      \"explain\" : false,%n"
            + "      \"_source\" : {%n"
            + "        \"includes\" : [ \"name\" ],%n"
            + "        \"excludes\" : [ ]%n"
            + "      }%n"
            + "    }%n"
            + "  }%n"
            + "}"),
        buildQuery(
            Collections.singletonList(named("take(name, 10)", new TakeAggregator(
                ImmutableList.of(ref("name", STRING), literal(10)), ARRAY)))));
  }

  @Test
  void should_build_filtered_top_hits_aggregation() {
    assertEquals(format(
        "{%n"
            + "  \"take(name, 10) filter(where age > 30)\" : {%n"
            + "    \"filter\" : {%n"
            + "      \"range\" : {%n"
            + "        \"age\" : {%n"
            + "          \"from\" : 30,%n"
            + "          \"to\" : null,%n"
            + "          \"include_lower\" : false,%n"
            + "          \"include_upper\" : true,%n"
            + "          \"boost\" : 1.0%n"
            + "        }%n"
            + "      }%n"
            + "    },%n"
            + "    \"aggregations\" : {%n"
            + "      \"take(name, 10) filter(where age > 30)\" : {%n"
            + "        \"top_hits\" : {%n"
            + "          \"from\" : 0,%n"
            + "          \"size\" : 10,%n"
            + "          \"version\" : false,%n"
            + "          \"seq_no_primary_term\" : false,%n"
            + "          \"explain\" : false,%n"
            + "          \"_source\" : {%n"
            + "            \"includes\" : [ \"name\" ],%n"
            + "            \"excludes\" : [ ]%n"
            + "          }%n"
            + "        }%n"
            + "      }%n"
            + "    }%n"
            + "  }%n"
            + "}"),
        buildQuery(Collections.singletonList(named(
            "take(name, 10) filter(where age > 30)",
            new TakeAggregator(
                ImmutableList.of(ref("name", STRING), literal(10)), ARRAY)
                .condition(DSL.greater(ref("age", INTEGER), literal(30)))))));
  }

  @Test
  void should_throw_exception_for_unsupported_distinct_aggregator() {
    assertThrows(IllegalStateException.class,
        () -> buildQuery(Collections.singletonList(named("avg(distinct age)", new AvgAggregator(
            Collections.singletonList(ref("name", STRING)), STRING).distinct(true)))),
        "unsupported distinct aggregator avg");
  }

  @Test
  void should_throw_exception_for_unsupported_aggregator() {
    when(aggregator.getFunctionName()).thenReturn(new FunctionName("unsupported_agg"));
    when(aggregator.getArguments()).thenReturn(Arrays.asList(ref("age", INTEGER)));

    IllegalStateException exception =
        assertThrows(IllegalStateException.class,
            () -> buildQuery(Arrays.asList(named("unsupported_agg(age)", aggregator))));
    assertEquals("unsupported aggregator unsupported_agg", exception.getMessage());
  }

  @Test
  void should_throw_exception_for_unsupported_exception() {
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> buildQuery(Arrays.asList(
            named("count(age)",
                new CountAggregator(Arrays.asList(named("age", ref("age", INTEGER))), INTEGER)))));
    assertEquals(
        "metric aggregation doesn't support expression age",
        exception.getMessage());
  }

  @SneakyThrows
  private String buildQuery(List<NamedAggregator> namedAggregatorList) {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readTree(
            aggregationBuilder.build(namedAggregatorList).getLeft().toString())
        .toPrettyString();
  }
}
