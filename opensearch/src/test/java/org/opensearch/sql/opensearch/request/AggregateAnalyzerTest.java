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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.request.AggregateAnalyzer.ExpressionNotAnalyzableException;
import org.opensearch.sql.opensearch.response.agg.CompositeAggregationParser;
import org.opensearch.sql.opensearch.response.agg.FilterParser;
import org.opensearch.sql.opensearch.response.agg.MetricParserHelper;
import org.opensearch.sql.opensearch.response.agg.NoBucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.response.agg.SingleValueParser;
import org.opensearch.sql.opensearch.response.agg.StatsParser;

class AggregateAnalyzerTest {

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private final List<String> schema = List.of("a", "b", "c");
  private final RelDataType rowType =
      typeFactory.createStructType(
          ImmutableList.of(
              typeFactory.createSqlType(SqlTypeName.INTEGER),
              typeFactory.createSqlType(SqlTypeName.VARCHAR),
              typeFactory.createSqlType(SqlTypeName.VARCHAR)),
          schema);
  final Map<String, ExprType> fieldTypes =
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
    Project project = createMockProject(List.of(0));
    Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> result =
        AggregateAnalyzer.analyze(aggregate, project, rowType, fieldTypes, outputFields, null);
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
    Project project = createMockProject(List.of(0));
    Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> result =
        AggregateAnalyzer.analyze(aggregate, project, rowType, fieldTypes, outputFields, null);
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
    Project project = createMockProject(List.of(0, 1));
    Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> result =
        AggregateAnalyzer.analyze(aggregate, project, rowType, fieldTypes, outputFields, null);

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
            ImmutableList.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "sum");
    Aggregate aggregate = createMockAggregate(List.of(aggCall), ImmutableBitSet.of());
    Project project = createMockProject(List.of(2));
    ExpressionNotAnalyzableException exception =
        assertThrows(
            ExpressionNotAnalyzableException.class,
            () ->
                AggregateAnalyzer.analyze(
                    aggregate, project, rowType, fieldTypes, List.of("sum"), null));
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
    Aggregate aggregate = createMockAggregate(List.of(aggCall), ImmutableBitSet.of(0));
    Project project = createMockProject(List.of(2));
    ExpressionNotAnalyzableException exception =
        assertThrows(
            ExpressionNotAnalyzableException.class,
            () ->
                AggregateAnalyzer.analyze(
                    aggregate, project, rowType, fieldTypes, outputFields, null));
    assertEquals("[field] must not be null", exception.getCause().getMessage());
  }

  @Test
  void analyze_aggCall_simpleFilter() throws ExpressionNotAnalyzableException {
    buildAggregation("filter_cnt")
        .withAggCall(
            b ->
                b.aggregateCall(
                    SqlStdOperatorTable.COUNT,
                    false,
                    b.call(
                        SqlStdOperatorTable.IS_TRUE,
                        b.call(SqlStdOperatorTable.GREATER_THAN, b.field("a"), b.literal(0))),
                    "filter_cnt"))
        .expectDslQuery(
            "[{\"filter_cnt\":{\"filter\":{\"range\":{\"a\":{"
                + "\"from\":0,"
                + "\"to\":null,"
                + "\"include_lower\":false,"
                + "\"include_upper\":true,"
                + "\"boost\":1.0}}},"
                + "\"aggregations\":{\"filter_cnt\":{\"value_count\":{\"field\":\"_index\"}}}}}]")
        .expectResponseParser(
            new MetricParserHelper(
                List.of(
                    FilterParser.builder()
                        .name("filter_cnt")
                        .metricsParser(new SingleValueParser("filter_cnt"))
                        .build())))
        .verify();
  }

  @Test
  void analyze_aggCall_simpleFilter_multiple() throws ExpressionNotAnalyzableException {
    buildAggregation("filter_avg", "filter_sum", "filter_min", "filter_max")
        .withAggCall(
            b ->
                b.aggregateCall(
                    SqlStdOperatorTable.AVG,
                    false,
                    b.call(
                        SqlStdOperatorTable.IS_TRUE,
                        b.call(SqlStdOperatorTable.EQUALS, b.field("a"), b.literal(10))),
                    "filter_avg",
                    b.field("a")))
        .withAggCall(
            b ->
                b.aggregateCall(
                    SqlStdOperatorTable.SUM,
                    false,
                    b.call(
                        SqlStdOperatorTable.IS_TRUE,
                        b.call(SqlStdOperatorTable.EQUALS, b.field("a"), b.literal(20))),
                    "filter_sum",
                    b.field("a")))
        .withAggCall(
            b ->
                b.aggregateCall(
                    SqlStdOperatorTable.MIN,
                    false,
                    b.call(
                        SqlStdOperatorTable.IS_TRUE,
                        b.call(SqlStdOperatorTable.EQUALS, b.field("b"), b.literal("test1"))),
                    "filter_min",
                    b.field("a")))
        .withAggCall(
            b ->
                b.aggregateCall(
                    SqlStdOperatorTable.MAX,
                    false,
                    b.call(
                        SqlStdOperatorTable.IS_TRUE,
                        b.call(SqlStdOperatorTable.EQUALS, b.field("b"), b.literal("test2"))),
                    "filter_max",
                    b.field("a")))
        .expectDslQuery(
            "[{\"filter_avg\":{\"filter\":{\"term\":{\"a\":{\"value\":10,\"boost\":1.0}}},"
                + "\"aggregations\":{\"filter_avg\":{\"avg\":{\"field\":\"a\"}}}}},"
                + " {\"filter_sum\":{\"filter\":{\"term\":{\"a\":{\"value\":20,\"boost\":1.0}}},"
                + "\"aggregations\":{\"filter_sum\":{\"sum\":{\"field\":\"a\"}}}}},"
                + " {\"filter_min\":{\"filter\":{\"term\":{\"b.keyword\":{\"value\":\"test1\",\"boost\":1.0}}},"
                + "\"aggregations\":{\"filter_min\":{\"min\":{\"field\":\"a\"}}}}},"
                + " {\"filter_max\":{\"filter\":{\"term\":{\"b.keyword\":{\"value\":\"test2\",\"boost\":1.0}}},"
                + "\"aggregations\":{\"filter_max\":{\"max\":{\"field\":\"a\"}}}}}]")
        .expectResponseParser(
            new MetricParserHelper(
                List.of(
                    FilterParser.builder()
                        .name("filter_avg")
                        .metricsParser(new SingleValueParser("filter_avg"))
                        .build(),
                    FilterParser.builder()
                        .name("filter_sum")
                        .metricsParser(new SingleValueParser("filter_sum"))
                        .build(),
                    FilterParser.builder()
                        .name("filter_min")
                        .metricsParser(new SingleValueParser("filter_min"))
                        .build(),
                    FilterParser.builder()
                        .name("filter_max")
                        .metricsParser(new SingleValueParser("filter_max"))
                        .build())))
        .verify();
  }

  @Test
  void analyze_aggCall_complexFilter() throws ExpressionNotAnalyzableException {
    buildAggregation("filter_count_range")
        .withAggCall(
            b ->
                b.aggregateCall(
                    SqlStdOperatorTable.COUNT,
                    false,
                    b.call(
                        SqlStdOperatorTable.IS_TRUE,
                        b.call(
                            SqlStdOperatorTable.AND,
                            b.call(
                                SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                                b.field("a"),
                                b.literal(30)),
                            b.call(
                                SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                                b.field("a"),
                                b.literal(50)))),
                    "filter_count_range"))
        .expectDslQuery(
            "[{\"filter_count_range\":{\"filter\":{\"range\":{\"a\":{\"from\":30.0,\"to\":50.0,"
                + "\"include_lower\":true,\"include_upper\":true,\"boost\":1.0}}},"
                + "\"aggregations\":{\"filter_count_range\":{\"value_count\":{\"field\":\"_index\"}}}}}]")
        .expectResponseParser(
            new MetricParserHelper(
                List.of(
                    FilterParser.builder()
                        .name("filter_count_range")
                        .metricsParser(new SingleValueParser("filter_count_range"))
                        .build())))
        .verify();
  }

  private Aggregate createMockAggregate(List<AggregateCall> calls, ImmutableBitSet groups) {
    Aggregate agg = mock(Aggregate.class);
    when(agg.getGroupSet()).thenReturn(groups);
    when(agg.getAggCallList()).thenReturn(calls);
    return agg;
  }

  private Project createMockProject(List<Integer> refIndex) {
    Project project = mock(Project.class);
    List<RexNode> rexNodes = new ArrayList<>();
    for (Integer index : refIndex) {
      RexInputRef ref = mock(RexInputRef.class);
      when(ref.getIndex()).thenReturn(index);
      when(ref.getType()).thenReturn(typeFactory.createSqlType(SqlTypeName.INTEGER));
      rexNodes.add(ref);
    }
    when(project.getProjects()).thenReturn(rexNodes);
    when(project.getRowType()).thenReturn(rowType);
    return project;
  }

  private AggregationTestBuilder buildAggregation(String... outputFields) {
    return new AggregationTestBuilder(List.of(outputFields));
  }

  /** Fluent API builder for creating aggregate filter tests */
  private class AggregationTestBuilder {
    private final List<String> outputFields;
    private final List<RelBuilder.AggCall> aggCalls = new ArrayList<>();
    private final RelBuilder relBuilder;
    private String expectedDsl;
    private MetricParserHelper expectedParser;

    AggregationTestBuilder(List<String> outputFields) {
      this.outputFields = new ArrayList<>(outputFields);
      this.relBuilder = createRelBuilder();
    }

    private RelBuilder createRelBuilder() {
      String tableName = "test";
      SchemaPlus root = Frameworks.createRootSchema(true);
      root.add(
          tableName,
          new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
              return rowType;
            }
          });
      return RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(root).build())
          .scan(tableName);
    }

    AggregationTestBuilder withAggCall(Function<RelBuilder, RelBuilder.AggCall> aggCallBuilder) {
      aggCalls.add(aggCallBuilder.apply(relBuilder));
      return this;
    }

    AggregationTestBuilder expectDslQuery(String expectedDsl) {
      this.expectedDsl = expectedDsl;
      return this;
    }

    AggregationTestBuilder expectResponseParser(MetricParserHelper expectedParser) {
      this.expectedParser = expectedParser;
      return this;
    }

    void verify() throws ExpressionNotAnalyzableException {
      // Create test RelNode plan
      RelNode rel =
          relBuilder
              .aggregate(relBuilder.groupKey(), aggCalls.toArray(new RelBuilder.AggCall[0]))
              .build();

      // Run analyzer
      Aggregate agg = (Aggregate) rel;
      Project project = (Project) agg.getInput(0);
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> result =
          AggregateAnalyzer.analyze(
              agg, project, rowType, fieldTypes, outputFields, agg.getCluster());

      if (expectedDsl != null) {
        assertEquals(expectedDsl, result.getLeft().toString());
      }

      if (expectedParser != null) {
        assertInstanceOf(NoBucketAggregationParser.class, result.getRight());
        assertEquals(
            expectedParser, ((NoBucketAggregationParser) result.getRight()).getMetricsParser());
      }
    }
  }
}
