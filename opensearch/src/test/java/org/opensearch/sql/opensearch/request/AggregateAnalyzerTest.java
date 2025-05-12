/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.request.AggregateAnalyzer.ExpressionNotAnalyzableException;
import org.opensearch.sql.opensearch.response.agg.CompositeAggregationParser;
import org.opensearch.sql.opensearch.response.agg.MetricParserHelper;
import org.opensearch.sql.opensearch.response.agg.NoBucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.response.agg.SingleValueParser;
import org.opensearch.sql.opensearch.response.agg.StatsParser;

class AggregateAnalyzerTest {

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private final List<String> schema = List.of("a", "b", "c");
  final Map<String, OpenSearchDataType> typeMapping =
      Map.of(
          "a",
          OpenSearchDataType.of(MappingType.Integer),
          "b",
          OpenSearchDataType.of(
              MappingType.Text, Map.of("fields", Map.of("keyword", Map.of("type", "keyword")))),
          "c",
          OpenSearchDataType.of(MappingType.Text)); // Text without keyword cannot be push down

  @Test
  void analyze_aggCall_simple() throws ExpressionNotAnalyzableException {
    AggregateCall countCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "cnt");
    AggregateCall avgCall =
        AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "avg");
    AggregateCall sumCall =
        AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "sum");
    AggregateCall minCall =
        AggregateCall.create(
            SqlStdOperatorTable.MIN,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "min");
    AggregateCall maxCall =
        AggregateCall.create(
            SqlStdOperatorTable.MAX,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "max");

    List<String> outputFields = List.of("cnt", "avg", "sum", "min", "max");
    Aggregate aggregate =
        createMockAggregate(
            List.of(countCall, avgCall, sumCall, minCall, maxCall), ImmutableBitSet.of());
    Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> result =
        AggregateAnalyzer.analyze(aggregate, schema, typeMapping, outputFields);
    assertEquals(
        "[{\"cnt\":{\"value_count\":{\"field\":\"_index\"}}},"
            + " {\"avg\":{\"avg\":{\"field\":\"a\"}}},"
            + " {\"sum\":{\"sum\":{\"field\":\"a\"}}},"
            + " {\"min\":{\"min\":{\"field\":\"a\"}}},"
            + " {\"max\":{\"max\":{\"field\":\"a\"}}}]",
        result.getLeft().toString());
    assertInstanceOf(NoBucketAggregationParser.class, result.getRight());
    MetricParserHelper metricsParser =
        ((NoBucketAggregationParser) result.getRight()).getMetricsParser();
    assertEquals(5, metricsParser.getMetricParserMap().size());
    metricsParser
        .getMetricParserMap()
        .forEach(
            (k, v) -> {
              assertTrue(outputFields.contains(k));
              assertInstanceOf(SingleValueParser.class, v);
            });
  }

  @Test
  void analyze_aggCall_extended() throws ExpressionNotAnalyzableException {
    AggregateCall varSampCall =
        AggregateCall.create(
            SqlStdOperatorTable.VAR_SAMP,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "var_samp");
    AggregateCall varPopCall =
        AggregateCall.create(
            SqlStdOperatorTable.VAR_POP,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "var_pop");
    AggregateCall stddevSampCall =
        AggregateCall.create(
            SqlStdOperatorTable.STDDEV_SAMP,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "stddev_samp");
    AggregateCall stddevPopCall =
        AggregateCall.create(
            SqlStdOperatorTable.STDDEV_SAMP,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "stddev_pop");
    List<String> outputFields = List.of("var_samp", "var_pop", "stddev_samp", "stddev_pop");
    Aggregate aggregate =
        createMockAggregate(
            List.of(varSampCall, varPopCall, stddevSampCall, stddevPopCall), ImmutableBitSet.of());
    Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> result =
        AggregateAnalyzer.analyze(aggregate, schema, typeMapping, outputFields);
    assertEquals(
        "[{\"var_samp\":{\"extended_stats\":{\"field\":\"a\",\"sigma\":2.0}}},"
            + " {\"var_pop\":{\"extended_stats\":{\"field\":\"a\",\"sigma\":2.0}}},"
            + " {\"stddev_samp\":{\"extended_stats\":{\"field\":\"a\",\"sigma\":2.0}}},"
            + " {\"stddev_pop\":{\"extended_stats\":{\"field\":\"a\",\"sigma\":2.0}}}]",
        result.getLeft().toString());
    assertInstanceOf(NoBucketAggregationParser.class, result.getRight());
    MetricParserHelper metricsParser =
        ((NoBucketAggregationParser) result.getRight()).getMetricsParser();
    assertEquals(4, metricsParser.getMetricParserMap().size());
    metricsParser
        .getMetricParserMap()
        .forEach(
            (k, v) -> {
              assertTrue(outputFields.contains(k));
              assertInstanceOf(StatsParser.class, v);
            });
  }

  @Test
  void analyze_groupBy() throws ExpressionNotAnalyzableException {
    AggregateCall aggCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "cnt");
    List<String> outputFields = List.of("a", "b", "cnt");
    Aggregate aggregate = createMockAggregate(List.of(aggCall), ImmutableBitSet.of(0, 1));
    Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> result =
        AggregateAnalyzer.analyze(aggregate, schema, typeMapping, outputFields);

    assertEquals(
        "[{\"composite_buckets\":{\"composite\":{\"size\":1000,\"sources\":["
            + "{\"a\":{\"terms\":{\"field\":\"a\",\"missing_bucket\":true,\"missing_order\":\"first\",\"order\":\"asc\"}}},"
            + "{\"b\":{\"terms\":{\"field\":\"b.keyword\",\"missing_bucket\":true,\"missing_order\":\"first\",\"order\":\"asc\"}}}]},"
            + "\"aggregations\":{\"cnt\":{\"value_count\":{\"field\":\"_index\"}}}}}]",
        result.getLeft().toString());
    assertInstanceOf(CompositeAggregationParser.class, result.getRight());
    MetricParserHelper metricsParser =
        ((CompositeAggregationParser) result.getRight()).getMetricsParser();
    assertEquals(1, metricsParser.getMetricParserMap().size());
    metricsParser
        .getMetricParserMap()
        .forEach(
            (k, v) -> {
              assertTrue(outputFields.contains(k));
              assertInstanceOf(SingleValueParser.class, v);
            });
  }

  @Test
  void analyze_aggCall_TextWithoutKeyword() {
    AggregateCall aggCall =
        AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(2),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "sum");
    Aggregate aggregate = createMockAggregate(List.of(aggCall), ImmutableBitSet.of());
    ExpressionNotAnalyzableException exception =
        assertThrows(
            ExpressionNotAnalyzableException.class,
            () -> AggregateAnalyzer.analyze(aggregate, schema, typeMapping, List.of("sum")));
    assertEquals("[field] must not be null: [sum]", exception.getCause().getMessage());
  }

  @Test
  void analyze_groupBy_TextWithoutKeyword() {
    AggregateCall aggCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "cnt");
    List<String> outputFields = List.of("c", "cnt");
    Aggregate aggregate = createMockAggregate(List.of(aggCall), ImmutableBitSet.of(2));
    ExpressionNotAnalyzableException exception =
        assertThrows(
            ExpressionNotAnalyzableException.class,
            () -> AggregateAnalyzer.analyze(aggregate, schema, typeMapping, outputFields));
    assertEquals("[field] must not be null", exception.getCause().getMessage());
  }

  private Aggregate createMockAggregate(List<AggregateCall> calls, ImmutableBitSet groups) {
    Aggregate agg = mock(Aggregate.class);
    when(agg.getGroupSet()).thenReturn(groups);
    when(agg.getAggCallList()).thenReturn(calls);
    return agg;
  }
}
