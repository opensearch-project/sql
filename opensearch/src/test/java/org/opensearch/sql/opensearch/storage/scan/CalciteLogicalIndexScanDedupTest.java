/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.response.agg.BucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.response.agg.TopHitsParser;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/**
 * A dedup pushed into the scan types its output columns by what they read, and gives the parser the
 * script columns of the top_hits.
 */
class CalciteLogicalIndexScanDedupTest {

  private final OpenSearchRequestBuilder requestBuilder = mock(OpenSearchRequestBuilder.class);

  /** The column types the scan pushes, which replace the index types of the same names. */
  private Map<String, OpenSearchDataType> columnTypes() {
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, OpenSearchDataType>> types = ArgumentCaptor.forClass(Map.class);
    verify(requestBuilder).pushColumnTypes(types.capture());
    return types.getValue();
  }

  /**
   * A scan over g and k (keyword), n (integer), and ts, d2 (epoch_second) and d3 (yyyy-MM-dd)
   * (date), as the index's mapping gives.
   */
  private CalciteLogicalIndexScan scan() {
    RelDataType rowType =
        TYPE_FACTORY
            .builder()
            .add("g", TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
            .add("k", TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
            .add("n", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER))
            .add("ts", TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP))
            .add("d2", TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP))
            .add("d3", TYPE_FACTORY.createSqlType(SqlTypeName.DATE))
            .build();
    Map<String, OpenSearchDataType> mapping =
        OpenSearchDataType.parseMapping(
            Map.of(
                "g", Map.of("type", "keyword"),
                "k", Map.of("type", "keyword"),
                "n", Map.of("type", "integer"),
                "ts", Map.of("type", "date"),
                "d2", Map.of("type", "date", "format", "epoch_second"),
                "d3", Map.of("type", "date", "format", "yyyy-MM-dd")));
    Map<String, ExprType> fieldTypes =
        OpenSearchDataType.traverseAndFlatten(mapping).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getExprType()));
    OpenSearchIndex osIndex = mock(OpenSearchIndex.class);
    when(osIndex.getFieldTypes()).thenReturn(fieldTypes);
    when(osIndex.getAllFieldTypes()).thenReturn(fieldTypes);
    when(osIndex.getFieldOpenSearchTypes()).thenReturn(mapping);
    when(osIndex.getQueryBucketSize()).thenReturn(1000);
    when(osIndex.createRequestBuilder()).thenReturn(requestBuilder);
    RelOptTable table = mock(RelOptTable.class);
    when(table.getRowType()).thenReturn(rowType);
    RelOptCluster cluster =
        RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(TYPE_FACTORY));
    return new CalciteLogicalIndexScan(cluster, table, osIndex);
  }

  /**
   * {@code rename k as j | eval k = 1 | dedup g}: j reads k, and k is a script column named like
   * the field the top_hits fetches for j.
   */
  @Test
  void dedupTypesItsScriptColumnsAndNamesItsScripts() {
    // A script reads the time of the query.
    try (Hook.Closeable ignored =
        Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L))) {
      CalciteLogicalIndexScan scan = scan();
      RelBuilder builder =
          RelBuilder.proto(RelBuilder.Config.DEFAULT.withPruneInputOfAggregate(false))
              .create(scan.getCluster(), null);
      builder.push(scan);
      RexNode one = builder.getRexBuilder().makeExactLiteral(BigDecimal.ONE);
      builder.project(List.of(builder.field("g"), builder.field("k"), one), List.of("g", "j", "k"));
      Project project = (Project) builder.peek();
      builder.aggregate(builder.groupKey(builder.field("g")), builder.literalAgg(1));
      Aggregate aggregate = (Aggregate) builder.build();

      CalciteLogicalIndexScan pushed =
          (CalciteLogicalIndexScan) scan.pushDownAggregate(aggregate, project);
      pushed.getPushDownContext().createRequestBuilder();

      assertEquals(ExprCoreType.INTEGER, columnTypes().get("k").getExprCoreType());

      @SuppressWarnings("unchecked")
      ArgumentCaptor<Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser>> agg =
          ArgumentCaptor.forClass(Pair.class);
      verify(requestBuilder).pushDownAggregation(agg.capture());
      String dsl = agg.getValue().getLeft().toString();
      assertTrue(dsl.contains("\"script_fields\":{\"k#script\""), dsl);
      OpenSearchAggregationResponseParser parser = agg.getValue().getRight();
      assertInstanceOf(BucketAggregationParser.class, parser);
      TopHitsParser topHits =
          (TopHitsParser)
              ((BucketAggregationParser) parser)
                  .getMetricsParser()
                  .getMetricParserMap()
                  .values()
                  .iterator()
                  .next();
      assertEquals(
          new TopHitsParser(topHits.getName(), false, false, Map.of("k", List.of("j")))
              .withScriptFields(Map.of("k#script", "k")),
          topHits);
    }
  }

  /**
   * {@code rename d3 as dd | eval n = k, k = n, t2 = ts, d2 = ts | dedup dd}: each column that
   * reads another field than the index field of its name takes that field's type, date formats
   * included, except the group key dd, whose value is the bucket key.
   */
  @Test
  void dedupTypesEachColumnByTheFieldItReads() {
    CalciteLogicalIndexScan scan = scan();
    RelBuilder builder =
        RelBuilder.proto(RelBuilder.Config.DEFAULT.withPruneInputOfAggregate(false))
            .create(scan.getCluster(), null);
    builder.push(scan);
    builder.project(
        List.of(
            builder.field("d3"),
            builder.field("g"),
            builder.field("k"),
            builder.field("n"),
            builder.field("ts"),
            builder.field("ts")),
        List.of("dd", "g", "n", "k", "t2", "d2"));
    Project project = (Project) builder.peek();
    builder.aggregate(builder.groupKey(builder.field("dd")), builder.literalAgg(1));
    Aggregate aggregate = (Aggregate) builder.build();

    CalciteLogicalIndexScan pushed =
        (CalciteLogicalIndexScan) scan.pushDownAggregate(aggregate, project);
    pushed.getPushDownContext().createRequestBuilder();

    Map<String, OpenSearchDataType> types = columnTypes();
    assertEquals(Set.of("n", "k", "t2", "d2"), types.keySet());
    assertEquals(OpenSearchDataType.MappingType.Keyword, types.get("n").getMappingType());
    assertEquals(OpenSearchDataType.MappingType.Integer, types.get("k").getMappingType());
    // The date type of ts, without the epoch_second format of the index field d2.
    assertTrue(
        ((OpenSearchDateType) scan.osIndex.getFieldOpenSearchTypes().get("d2")).hasFormats());
    for (String column : List.of("t2", "d2")) {
      assertEquals(OpenSearchDataType.MappingType.Date, types.get(column).getMappingType(), column);
      assertFalse(((OpenSearchDateType) types.get(column)).hasFormats(), column);
    }
  }
}
