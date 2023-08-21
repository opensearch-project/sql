/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.aggregation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.opensearch.sql.common.utils.StringUtils.format;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.expression.DSL.span;
import static org.opensearch.sql.opensearch.utils.Utils.agg;
import static org.opensearch.sql.opensearch.utils.Utils.avg;
import static org.opensearch.sql.opensearch.utils.Utils.group;
import static org.opensearch.sql.opensearch.utils.Utils.sort;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.AvgAggregator;
import org.opensearch.sql.expression.aggregation.CountAggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.storage.serialization.ExpressionSerializer;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class AggregationQueryBuilderTest {
  @Mock private ExpressionSerializer serializer;

  private AggregationQueryBuilder queryBuilder;

  @BeforeEach
  void set_up() {
    queryBuilder = new AggregationQueryBuilder(serializer);
  }

  @Test
  void should_build_composite_aggregation_for_field_reference() {
    assertEquals(
        format(
            "{%n"
                + "  \"composite_buckets\" : {%n"
                + "    \"composite\" : {%n"
                + "      \"size\" : 1000,%n"
                + "      \"sources\" : [ {%n"
                + "        \"name\" : {%n"
                + "          \"terms\" : {%n"
                + "            \"field\" : \"name\",%n"
                + "            \"missing_bucket\" : true,%n"
                + "            \"missing_order\" : \"first\",%n"
                + "            \"order\" : \"asc\"%n"
                + "          }%n"
                + "        }%n"
                + "      } ]%n"
                + "    },%n"
                + "    \"aggregations\" : {%n"
                + "      \"avg(age)\" : {%n"
                + "        \"avg\" : {%n"
                + "          \"field\" : \"age\"%n"
                + "        }%n"
                + "      }%n"
                + "    }%n"
                + "  }%n"
                + "}"),
        buildQuery(
            Arrays.asList(
                named("avg(age)", new AvgAggregator(Arrays.asList(ref("age", INTEGER)), INTEGER))),
            Arrays.asList(named("name", ref("name", STRING)))));
  }

  @Test
  void should_build_composite_aggregation_for_field_reference_with_order() {
    assertEquals(
        format(
            "{%n"
                + "  \"composite_buckets\" : {%n"
                + "    \"composite\" : {%n"
                + "      \"size\" : 1000,%n"
                + "      \"sources\" : [ {%n"
                + "        \"name\" : {%n"
                + "          \"terms\" : {%n"
                + "            \"field\" : \"name\",%n"
                + "            \"missing_bucket\" : true,%n"
                + "            \"missing_order\" : \"last\",%n"
                + "            \"order\" : \"desc\"%n"
                + "          }%n"
                + "        }%n"
                + "      } ]%n"
                + "    },%n"
                + "    \"aggregations\" : {%n"
                + "      \"avg(age)\" : {%n"
                + "        \"avg\" : {%n"
                + "          \"field\" : \"age\"%n"
                + "        }%n"
                + "      }%n"
                + "    }%n"
                + "  }%n"
                + "}"),
        buildQuery(
            Arrays.asList(
                named("avg(age)", new AvgAggregator(Arrays.asList(ref("age", INTEGER)), INTEGER))),
            Arrays.asList(named("name", ref("name", STRING))),
            sort(ref("name", STRING), Sort.SortOption.DEFAULT_DESC)));
  }

  @Test
  void should_build_type_mapping_for_field_reference() {
    assertThat(
        buildTypeMapping(
            Arrays.asList(
                named("avg(age)", new AvgAggregator(Arrays.asList(ref("age", INTEGER)), INTEGER))),
            Arrays.asList(named("name", ref("name", STRING)))),
        containsInAnyOrder(
            map("avg(age)", OpenSearchDataType.of(INTEGER)),
            map("name", OpenSearchDataType.of(STRING))));
  }

  @Test
  void should_build_type_mapping_for_timestamp_type() {
    assertThat(
        buildTypeMapping(
            Arrays.asList(
                named(
                    "avg(timestamp)",
                    new AvgAggregator(Arrays.asList(ref("timestamp", TIMESTAMP)), TIMESTAMP))),
            Arrays.asList(named("timestamp", ref("timestamp", TIMESTAMP)))),
        containsInAnyOrder(
            map("avg(timestamp)", OpenSearchDateType.of()),
            map("timestamp", OpenSearchDateType.of())));
  }

  @Test
  void should_build_type_mapping_for_date_type() {
    assertThat(
        buildTypeMapping(
            Arrays.asList(
                named("avg(date)", new AvgAggregator(Arrays.asList(ref("date", DATE)), DATE))),
            Arrays.asList(named("date", ref("date", DATE)))),
        containsInAnyOrder(
            map("avg(date)", OpenSearchDateType.of(DATE)),
            map("date", OpenSearchDateType.of(DATE))));
  }

  @Test
  void should_build_type_mapping_for_time_type() {
    assertThat(
        buildTypeMapping(
            Arrays.asList(
                named("avg(time)", new AvgAggregator(Arrays.asList(ref("time", TIME)), TIME))),
            Arrays.asList(named("time", ref("time", TIME)))),
        containsInAnyOrder(
            map("avg(time)", OpenSearchDateType.of(TIME)),
            map("time", OpenSearchDateType.of(TIME))));
  }

  @Test
  void should_build_composite_aggregation_for_field_reference_of_keyword() {
    assertEquals(
        format(
            "{%n"
                + "  \"composite_buckets\" : {%n"
                + "    \"composite\" : {%n"
                + "      \"size\" : 1000,%n"
                + "      \"sources\" : [ {%n"
                + "        \"name\" : {%n"
                + "          \"terms\" : {%n"
                + "            \"field\" : \"name.keyword\",%n"
                + "            \"missing_bucket\" : true,%n"
                + "            \"missing_order\" : \"first\",%n"
                + "            \"order\" : \"asc\"%n"
                + "          }%n"
                + "        }%n"
                + "      } ]%n"
                + "    },%n"
                + "    \"aggregations\" : {%n"
                + "      \"avg(age)\" : {%n"
                + "        \"avg\" : {%n"
                + "          \"field\" : \"age\"%n"
                + "        }%n"
                + "      }%n"
                + "    }%n"
                + "  }%n"
                + "}"),
        buildQuery(
            Arrays.asList(
                named("avg(age)", new AvgAggregator(Arrays.asList(ref("age", INTEGER)), INTEGER))),
            Arrays.asList(
                named(
                    "name",
                    ref(
                        "name",
                        OpenSearchTextType.of(
                            Map.of(
                                "words",
                                OpenSearchDataType.of(
                                    OpenSearchDataType.MappingType.Keyword))))))));
  }

  @Test
  void should_build_type_mapping_for_field_reference_of_keyword() {
    assertThat(
        buildTypeMapping(
            Arrays.asList(
                named("avg(age)", new AvgAggregator(Arrays.asList(ref("age", INTEGER)), INTEGER))),
            Arrays.asList(named("name", ref("name", STRING)))),
        containsInAnyOrder(
            map("avg(age)", OpenSearchDataType.of(INTEGER)),
            map("name", OpenSearchDataType.of(STRING))));
  }

  @Test
  void should_build_composite_aggregation_for_expression() {
    doAnswer(
            invocation -> {
              Expression expr = invocation.getArgument(0);
              return expr.toString();
            })
        .when(serializer)
        .serialize(any());
    assertEquals(
        format(
            "{%n"
                + "  \"composite_buckets\" : {%n"
                + "    \"composite\" : {%n"
                + "      \"size\" : 1000,%n"
                + "      \"sources\" : [ {%n"
                + "        \"age\" : {%n"
                + "          \"terms\" : {%n"
                + "            \"script\" : {%n"
                + "              \"source\" : \"asin(age)\",%n"
                + "              \"lang\" : \"opensearch_query_expression\"%n"
                + "            },%n"
                + "            \"missing_bucket\" : true,%n"
                + "            \"missing_order\" : \"first\",%n"
                + "            \"order\" : \"asc\"%n"
                + "          }%n"
                + "        }%n"
                + "      } ]%n"
                + "    },%n"
                + "    \"aggregations\" : {%n"
                + "      \"avg(balance)\" : {%n"
                + "        \"avg\" : {%n"
                + "          \"script\" : {%n"
                + "            \"source\" : \"abs(balance)\",%n"
                + "            \"lang\" : \"opensearch_query_expression\"%n"
                + "          }%n"
                + "        }%n"
                + "      }%n"
                + "    }%n"
                + "  }%n"
                + "}"),
        buildQuery(
            Arrays.asList(
                named(
                    "avg(balance)",
                    new AvgAggregator(Arrays.asList(DSL.abs(ref("balance", INTEGER))), INTEGER))),
            Arrays.asList(named("age", DSL.asin(ref("age", INTEGER))))));
  }

  @Test
  void should_build_composite_aggregation_follow_with_order_by_position() {
    assertEquals(
        format(
            "{%n"
                + "  \"composite_buckets\" : {%n"
                + "    \"composite\" : {%n"
                + "      \"size\" : 1000,%n"
                + "      \"sources\" : [ {%n"
                + "        \"name\" : {%n"
                + "          \"terms\" : {%n"
                + "            \"field\" : \"name\",%n"
                + "            \"missing_bucket\" : true,%n"
                + "            \"missing_order\" : \"last\",%n"
                + "            \"order\" : \"desc\"%n"
                + "          }%n"
                + "        }%n"
                + "      }, {%n"
                + "        \"age\" : {%n"
                + "          \"terms\" : {%n"
                + "            \"field\" : \"age\",%n"
                + "            \"missing_bucket\" : true,%n"
                + "            \"missing_order\" : \"first\",%n"
                + "            \"order\" : \"asc\"%n"
                + "          }%n"
                + "        }%n"
                + "      } ]%n"
                + "    },%n"
                + "    \"aggregations\" : {%n"
                + "      \"avg(balance)\" : {%n"
                + "        \"avg\" : {%n"
                + "          \"field\" : \"balance\"%n"
                + "        }%n"
                + "      }%n"
                + "    }%n"
                + "  }%n"
                + "}"),
        buildQuery(
            agg(named("avg(balance)", avg(ref("balance", INTEGER), INTEGER))),
            group(named("age", ref("age", INTEGER)), named("name", ref("name", STRING))),
            sort(
                ref("name", STRING),
                Sort.SortOption.DEFAULT_DESC,
                ref("age", INTEGER),
                Sort.SortOption.DEFAULT_ASC)));
  }

  @Test
  void should_build_type_mapping_for_expression() {
    assertThat(
        buildTypeMapping(
            Arrays.asList(
                named(
                    "avg(balance)",
                    new AvgAggregator(Arrays.asList(DSL.abs(ref("balance", INTEGER))), INTEGER))),
            Arrays.asList(named("age", DSL.asin(ref("age", INTEGER))))),
        containsInAnyOrder(
            map("avg(balance)", OpenSearchDataType.of(INTEGER)),
            map("age", OpenSearchDataType.of(DOUBLE))));
  }

  @Test
  void should_build_aggregation_without_bucket() {
    assertEquals(
        format(
            "{%n"
                + "  \"avg(balance)\" : {%n"
                + "    \"avg\" : {%n"
                + "      \"field\" : \"balance\"%n"
                + "    }%n"
                + "  }%n"
                + "}"),
        buildQuery(
            Arrays.asList(
                named(
                    "avg(balance)",
                    new AvgAggregator(Arrays.asList(ref("balance", INTEGER)), INTEGER))),
            Collections.emptyList()));
  }

  @Test
  void should_build_filter_aggregation() {
    assertEquals(
        format(
            "{%n"
                + "  \"avg(age) filter(where age > 34)\" : {%n"
                + "    \"filter\" : {%n"
                + "      \"range\" : {%n"
                + "        \"age\" : {%n"
                + "          \"from\" : 20,%n"
                + "          \"to\" : null,%n"
                + "          \"include_lower\" : false,%n"
                + "          \"include_upper\" : true,%n"
                + "          \"boost\" : 1.0%n"
                + "        }%n"
                + "      }%n"
                + "    },%n"
                + "    \"aggregations\" : {%n"
                + "      \"avg(age) filter(where age > 34)\" : {%n"
                + "        \"avg\" : {%n"
                + "          \"field\" : \"age\"%n"
                + "        }%n"
                + "      }%n"
                + "    }%n"
                + "  }%n"
                + "}"),
        buildQuery(
            Arrays.asList(
                named(
                    "avg(age) filter(where age > 34)",
                    new AvgAggregator(Arrays.asList(ref("age", INTEGER)), INTEGER)
                        .condition(DSL.greater(ref("age", INTEGER), literal(20))))),
            Collections.emptyList()));
  }

  @Test
  void should_build_filter_aggregation_group_by() {
    assertEquals(
        format(
            "{%n"
                + "  \"composite_buckets\" : {%n"
                + "    \"composite\" : {%n"
                + "      \"size\" : 1000,%n"
                + "      \"sources\" : [ {%n"
                + "        \"gender\" : {%n"
                + "          \"terms\" : {%n"
                + "            \"field\" : \"gender\",%n"
                + "            \"missing_bucket\" : true,%n"
                + "            \"missing_order\" : \"first\",%n"
                + "            \"order\" : \"asc\"%n"
                + "          }%n"
                + "        }%n"
                + "      } ]%n"
                + "    },%n"
                + "    \"aggregations\" : {%n"
                + "      \"avg(age) filter(where age > 34)\" : {%n"
                + "        \"filter\" : {%n"
                + "          \"range\" : {%n"
                + "            \"age\" : {%n"
                + "              \"from\" : 20,%n"
                + "              \"to\" : null,%n"
                + "              \"include_lower\" : false,%n"
                + "              \"include_upper\" : true,%n"
                + "              \"boost\" : 1.0%n"
                + "            }%n"
                + "          }%n"
                + "        },%n"
                + "        \"aggregations\" : {%n"
                + "          \"avg(age) filter(where age > 34)\" : {%n"
                + "            \"avg\" : {%n"
                + "              \"field\" : \"age\"%n"
                + "            }%n"
                + "          }%n"
                + "        }%n"
                + "      }%n"
                + "    }%n"
                + "  }%n"
                + "}"),
        buildQuery(
            Arrays.asList(
                named(
                    "avg(age) filter(where age > 34)",
                    new AvgAggregator(Arrays.asList(ref("age", INTEGER)), INTEGER)
                        .condition(DSL.greater(ref("age", INTEGER), literal(20))))),
            Arrays.asList(named(ref("gender", OpenSearchDataType.of(STRING))))));
  }

  @Test
  void should_build_type_mapping_without_bucket() {
    assertThat(
        buildTypeMapping(
            Arrays.asList(
                named(
                    "avg(balance)",
                    new AvgAggregator(Arrays.asList(ref("balance", INTEGER)), INTEGER))),
            Collections.emptyList()),
        containsInAnyOrder(map("avg(balance)", OpenSearchDataType.of(INTEGER))));
  }

  @Test
  void should_build_histogram() {
    assertEquals(
        format(
            "{%n"
                + "  \"composite_buckets\" : {%n"
                + "    \"composite\" : {%n"
                + "      \"size\" : 1000,%n"
                + "      \"sources\" : [ {%n"
                + "        \"SpanExpression(field=age, value=10, unit=NONE)\" : {%n"
                + "          \"histogram\" : {%n"
                + "            \"field\" : \"age\",%n"
                + "            \"missing_bucket\" : true,%n"
                + "            \"missing_order\" : \"first\",%n"
                + "            \"order\" : \"asc\",%n"
                + "            \"interval\" : 10.0%n"
                + "          }%n"
                + "        }%n"
                + "      } ]%n"
                + "    },%n"
                + "    \"aggregations\" : {%n"
                + "      \"count(a)\" : {%n"
                + "        \"value_count\" : {%n"
                + "          \"field\" : \"a\"%n"
                + "        }%n"
                + "      }%n"
                + "    }%n"
                + "  }%n"
                + "}"),
        buildQuery(
            Arrays.asList(
                named("count(a)", new CountAggregator(Arrays.asList(ref("a", INTEGER)), INTEGER))),
            Arrays.asList(named(span(ref("age", INTEGER), literal(10), "")))));
  }

  @Test
  void should_build_histogram_two_metrics() {
    assertEquals(
        format(
            "{%n"
                + "  \"composite_buckets\" : {%n"
                + "    \"composite\" : {%n"
                + "      \"size\" : 1000,%n"
                + "      \"sources\" : [ {%n"
                + "        \"SpanExpression(field=age, value=10, unit=NONE)\" : {%n"
                + "          \"histogram\" : {%n"
                + "            \"field\" : \"age\",%n"
                + "            \"missing_bucket\" : true,%n"
                + "            \"missing_order\" : \"first\",%n"
                + "            \"order\" : \"asc\",%n"
                + "            \"interval\" : 10.0%n"
                + "          }%n"
                + "        }%n"
                + "      } ]%n"
                + "    },%n"
                + "    \"aggregations\" : {%n"
                + "      \"count(a)\" : {%n"
                + "        \"value_count\" : {%n"
                + "          \"field\" : \"a\"%n"
                + "        }%n"
                + "      },%n"
                + "      \"avg(b)\" : {%n"
                + "        \"avg\" : {%n"
                + "          \"field\" : \"b\"%n"
                + "        }%n"
                + "      }%n"
                + "    }%n"
                + "  }%n"
                + "}"),
        buildQuery(
            Arrays.asList(
                named("count(a)", new CountAggregator(Arrays.asList(ref("a", INTEGER)), INTEGER)),
                named("avg(b)", new AvgAggregator(Arrays.asList(ref("b", INTEGER)), INTEGER))),
            Arrays.asList(named(span(ref("age", INTEGER), literal(10), "")))));
  }

  @Test
  void fixed_interval_time_span() {
    assertEquals(
        format(
            "{%n"
                + "  \"composite_buckets\" : {%n"
                + "    \"composite\" : {%n"
                + "      \"size\" : 1000,%n"
                + "      \"sources\" : [ {%n"
                + "        \"SpanExpression(field=timestamp, value=1, unit=H)\" : {%n"
                + "          \"date_histogram\" : {%n"
                + "            \"field\" : \"timestamp\",%n"
                + "            \"missing_bucket\" : true,%n"
                + "            \"missing_order\" : \"first\",%n"
                + "            \"order\" : \"asc\",%n"
                + "            \"fixed_interval\" : \"1h\"%n"
                + "          }%n"
                + "        }%n"
                + "      } ]%n"
                + "    },%n"
                + "    \"aggregations\" : {%n"
                + "      \"count(a)\" : {%n"
                + "        \"value_count\" : {%n"
                + "          \"field\" : \"a\"%n"
                + "        }%n"
                + "      }%n"
                + "    }%n"
                + "  }%n"
                + "}"),
        buildQuery(
            Arrays.asList(
                named("count(a)", new CountAggregator(Arrays.asList(ref("a", INTEGER)), INTEGER))),
            Arrays.asList(named(span(ref("timestamp", TIMESTAMP), literal(1), "h")))));
  }

  @Test
  void calendar_interval_time_span() {
    assertEquals(
        format(
            "{%n"
                + "  \"composite_buckets\" : {%n"
                + "    \"composite\" : {%n"
                + "      \"size\" : 1000,%n"
                + "      \"sources\" : [ {%n"
                + "        \"SpanExpression(field=date, value=1, unit=W)\" : {%n"
                + "          \"date_histogram\" : {%n"
                + "            \"field\" : \"date\",%n"
                + "            \"missing_bucket\" : true,%n"
                + "            \"missing_order\" : \"first\",%n"
                + "            \"order\" : \"asc\",%n"
                + "            \"calendar_interval\" : \"1w\"%n"
                + "          }%n"
                + "        }%n"
                + "      } ]%n"
                + "    },%n"
                + "    \"aggregations\" : {%n"
                + "      \"count(a)\" : {%n"
                + "        \"value_count\" : {%n"
                + "          \"field\" : \"a\"%n"
                + "        }%n"
                + "      }%n"
                + "    }%n"
                + "  }%n"
                + "}"),
        buildQuery(
            Arrays.asList(
                named("count(a)", new CountAggregator(Arrays.asList(ref("a", INTEGER)), INTEGER))),
            Arrays.asList(named(span(ref("date", DATE), literal(1), "w")))));
  }

  @Test
  void general_span() {
    assertEquals(
        format(
            "{%n"
                + "  \"composite_buckets\" : {%n"
                + "    \"composite\" : {%n"
                + "      \"size\" : 1000,%n"
                + "      \"sources\" : [ {%n"
                + "        \"SpanExpression(field=age, value=1, unit=NONE)\" : {%n"
                + "          \"histogram\" : {%n"
                + "            \"field\" : \"age\",%n"
                + "            \"missing_bucket\" : true,%n"
                + "            \"missing_order\" : \"first\",%n"
                + "            \"order\" : \"asc\",%n"
                + "            \"interval\" : 1.0%n"
                + "          }%n"
                + "        }%n"
                + "      } ]%n"
                + "    },%n"
                + "    \"aggregations\" : {%n"
                + "      \"count(a)\" : {%n"
                + "        \"value_count\" : {%n"
                + "          \"field\" : \"a\"%n"
                + "        }%n"
                + "      }%n"
                + "    }%n"
                + "  }%n"
                + "}"),
        buildQuery(
            Arrays.asList(
                named("count(a)", new CountAggregator(Arrays.asList(ref("a", INTEGER)), INTEGER))),
            Arrays.asList(named(span(ref("age", INTEGER), literal(1), "")))));
  }

  @Test
  void invalid_unit() {
    assertThrows(
        IllegalStateException.class,
        () ->
            buildQuery(
                Arrays.asList(
                    named(
                        "count(a)",
                        new CountAggregator(Arrays.asList(ref("a", INTEGER)), INTEGER))),
                Arrays.asList(named(span(ref("age", INTEGER), literal(1), "invalid_unit")))));
  }

  @SneakyThrows
  private String buildQuery(
      List<NamedAggregator> namedAggregatorList, List<NamedExpression> groupByList) {
    return buildQuery(namedAggregatorList, groupByList, null);
  }

  @SneakyThrows
  private String buildQuery(
      List<NamedAggregator> namedAggregatorList,
      List<NamedExpression> groupByList,
      List<Pair<Sort.SortOption, Expression>> sortList) {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper
        .readTree(
            queryBuilder
                .buildAggregationBuilder(namedAggregatorList, groupByList, sortList)
                .getLeft()
                .get(0)
                .toString())
        .toPrettyString();
  }

  private Set<Map.Entry<String, OpenSearchDataType>> buildTypeMapping(
      List<NamedAggregator> namedAggregatorList, List<NamedExpression> groupByList) {
    return queryBuilder.buildTypeMapping(namedAggregatorList, groupByList).entrySet();
  }

  private Map.Entry<String, ExprType> map(String name, ExprType type) {
    return new AbstractMap.SimpleEntry<String, ExprType>(name, type);
  }
}
