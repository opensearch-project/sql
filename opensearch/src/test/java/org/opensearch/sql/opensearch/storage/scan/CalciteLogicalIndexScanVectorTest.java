/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchKnnVectorType;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.response.agg.MetricParserHelper;
import org.opensearch.sql.opensearch.response.agg.NoBucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.response.agg.TopHitsParser;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/**
 * The {@code _source} includes of a pushed-down projection name each knn_vector inside a projected
 * object or nested field: on an index with k-NN derived source, the k-NN plugin injects a vector
 * only if an include matches the vector's own path. An aggregation over an object column, pushed
 * after a projection to it, still sees the vectors inside the object.
 */
class CalciteLogicalIndexScanVectorTest {

  private static final Set<String> KNN_VECTOR_FIELDS =
      Set.of("v", "obj.vec", "obj.inner.vec2", "obj2.vec", "nest.vec");

  /** The vectors inside a projected object, but not those of a sibling object named obj2. */
  @Test
  void projectedObjectsNameTheirVectors() {
    assertEquals(
        List.of("id", "obj", "nest", "obj.inner.vec2", "obj.vec", "nest.vec"),
        CalciteLogicalIndexScan.withVectorPaths(List.of("id", "obj", "nest"), KNN_VECTOR_FIELDS));
  }

  /** Other projections, including a vector itself, are as before. */
  @Test
  void otherProjectionsAreUnchanged() {
    assertEquals(
        List.of("id", "v", "obj.vec", "name"),
        CalciteLogicalIndexScan.withVectorPaths(
            List.of("id", "v", "obj.vec", "name"), KNN_VECTOR_FIELDS));
  }

  /** Field obj.vec, which a float index and a byte index map. */
  private static OpenSearchKnnVectorType mixed() {
    return OpenSearchKnnVectorType.of(3, VectorDataType.FLOAT, null, null, null)
        .inIndex("i0")
        .mergedWith(
            OpenSearchKnnVectorType.of(3, VectorDataType.BYTE, null, null, null).inIndex("i1"));
  }

  private final OpenSearchRequestBuilder requestBuilder = mock(OpenSearchRequestBuilder.class);

  /** A scan over id, obj (an object holding vector vec) and v, as the index's mapping gives. */
  private CalciteLogicalIndexScan scan() {
    RelDataType rowType =
        TYPE_FACTORY
            .builder()
            .add("id", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER))
            .add(
                "obj",
                TYPE_FACTORY.createMapType(
                    TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
                    TYPE_FACTORY.createSqlType(SqlTypeName.ANY),
                    true))
            .add("obj.vec", TYPE_FACTORY.createVectorType(mixed(), true))
            .add("v", TYPE_FACTORY.createVectorType(3, VectorDataType.FLOAT, null, true))
            .build();
    Map<String, ExprType> fieldTypes = new HashMap<>();
    fieldTypes.put("id", OpenSearchDataType.of(OpenSearchDataType.MappingType.Integer));
    fieldTypes.put("obj", OpenSearchDataType.of(OpenSearchDataType.MappingType.Object));
    fieldTypes.put("obj.vec", mixed());
    fieldTypes.put("v", OpenSearchKnnVectorType.of(3, VectorDataType.FLOAT, null, null, null));
    OpenSearchIndex osIndex = mock(OpenSearchIndex.class);
    when(osIndex.getFieldTypes()).thenReturn(fieldTypes);
    when(osIndex.getAllFieldTypes()).thenReturn(fieldTypes);
    when(osIndex.getKnnVectorFields()).thenReturn(Set.of("obj.vec", "obj2.vec", "v"));
    when(osIndex.getQueryBucketSize()).thenReturn(1000);
    when(osIndex.createRequestBuilder()).thenReturn(requestBuilder);
    RelOptTable table = mock(RelOptTable.class);
    when(table.getRowType()).thenReturn(rowType);
    RelOptCluster cluster =
        RelOptCluster.create(
            new VolcanoPlanner(), new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY));
    return new CalciteLogicalIndexScan(cluster, table, osIndex);
  }

  /** The pushed-down projection of obj also includes obj.vec. */
  @Test
  void projectionOfAnObjectIncludesItsVectors() {
    CalciteLogicalIndexScan projected = scan().pushDownProject(List.of(0, 1));
    projected.getPushDownContext().createRequestBuilder();
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Stream<String>> includes = ArgumentCaptor.forClass(Stream.class);
    verify(requestBuilder).pushDownProjectStream(includes.capture());
    assertEquals(List.of("id", "obj", "obj.vec"), includes.getValue().toList());
  }

  /**
   * first(obj) after a projection to [id, obj]: the scan's schema has no obj.vec, but the parser
   * still records the hit's index, as obj holds a vector a float index and a byte index map.
   */
  @Test
  void aggregationAfterAProjectionSeesTheVectorsOfAnObject() {
    CalciteLogicalIndexScan projected = scan().pushDownProject(List.of(0, 1));
    AggregateCall first =
        AggregateCall.create(
            PPLBuiltinOperators.FIRST,
            false,
            false,
            false,
            List.of(),
            List.of(1),
            -1,
            null,
            RelCollations.EMPTY,
            projected.getRowType().getFieldList().get(1).getType(),
            "first_obj");
    LogicalAggregate aggregate =
        LogicalAggregate.create(projected, List.of(), ImmutableBitSet.of(), null, List.of(first));
    CalciteLogicalIndexScan pushed =
        (CalciteLogicalIndexScan) projected.pushDownAggregate(aggregate, null);
    pushed.getPushDownContext().createRequestBuilder();
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser>> agg =
        ArgumentCaptor.forClass(Pair.class);
    verify(requestBuilder).pushDownAggregation(agg.capture());
    OpenSearchAggregationResponseParser parser = agg.getValue().getRight();
    assertInstanceOf(NoBucketAggregationParser.class, parser);
    assertEquals(
        new MetricParserHelper(
            List.of(new TopHitsParser("first_obj", true, false, null, true, null))),
        ((NoBucketAggregationParser) parser).getMetricsParser());
    verify(requestBuilder).pushTypeMapping(any());
  }
}
