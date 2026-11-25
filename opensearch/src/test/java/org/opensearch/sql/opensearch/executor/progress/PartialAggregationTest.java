/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.job.QueryResult;

class PartialAggregationTest {

  private static final TotalHits HITS = new TotalHits(10, TotalHits.Relation.EQUAL_TO);

  @Test
  void noMergeOrNoConverterHasNoRows() {
    PartialAggregation aggregation = maxOf();
    assertTrue(aggregation.snapshot().isEmpty());

    aggregation.onReduce(HITS, max(7), PipelineTree.EMPTY, false);
    assertTrue(aggregation.snapshot().isEmpty());
  }

  @Test
  void buildsTheMergeWithTheRootRowType() {
    PartialAggregation aggregation = maxOf();
    aggregation.rootRows((hits, aggs) -> List.<Object[]>of(new Object[] {maxValue(aggs)}));

    aggregation.onReduce(HITS, max(7), PipelineTree.EMPTY, false);
    QueryResult.Rows rows = aggregation.snapshot().orElseThrow();
    assertEquals(ExprCoreType.DOUBLE, rows.schema().getColumns().get(0).getExprType());
    assertEquals(List.of(ExprValueUtils.tupleValue(Map.of("m", 7D))), rows.rows());

    aggregation.onReduce(HITS, max(9), PipelineTree.EMPTY, false);
    assertEquals(
        List.of(ExprValueUtils.tupleValue(Map.of("m", 9D))),
        aggregation.snapshot().orElseThrow().rows());
  }

  @Test
  void buildsAMergeOnlyOnceAReaderHasAsked() {
    AtomicInteger builds = new AtomicInteger();
    PartialAggregation aggregation = maxOf();
    aggregation.rootRows(
        (hits, aggs) -> {
          builds.incrementAndGet();
          return List.<Object[]>of(new Object[] {maxValue(aggs)});
        });

    aggregation.onReduce(HITS, max(7), PipelineTree.EMPTY, false);
    aggregation.onReduce(HITS, max(8), PipelineTree.EMPTY, false);
    assertEquals(1, builds.get());
    // A read returns the merge built so far without building, and asks for the next one.
    assertEquals(7D, cell(aggregation));
    assertEquals(1, builds.get());

    aggregation.onReduce(HITS, max(9), PipelineTree.EMPTY, false);
    assertEquals(2, builds.get());
    assertEquals(9D, cell(aggregation));
  }

  @Test
  void finishesAPartialMergeButUsesTheLastAsItIs() {
    PartialAggregation aggregation = termsOf(100);

    // A partial merge keeps every bucket; the final reduce trims them to the requested size.
    aggregation.onReduce(HITS, terms(2, "a", "b", "c"), PipelineTree.EMPTY, false);
    assertEquals(2, aggregation.snapshot().orElseThrow().rows().size());

    aggregation.onReduce(HITS, terms(2, "a", "b", "c"), PipelineTree.EMPTY, true);
    assertEquals(3, aggregation.snapshot().orElseThrow().rows().size());
  }

  @Test
  void aMergeWithMoreBucketsThanASearchMayReturnHasNoRows() {
    PartialAggregation aggregation = termsOf(2);
    aggregation.onReduce(HITS, terms(5, "a", "b", "c"), PipelineTree.EMPTY, false);
    assertTrue(aggregation.snapshot().isEmpty());
  }

  @Test
  void aConversionFailureOrNoCellsHasNoRows() {
    PartialAggregation failing = maxOf();
    failing.rootRows(
        (hits, aggs) -> {
          throw new IllegalStateException("boom");
        });
    failing.onReduce(HITS, max(7), PipelineTree.EMPTY, false);
    assertTrue(failing.snapshot().isEmpty());

    PartialAggregation empty = maxOf();
    empty.rootRows((hits, aggs) -> List.of());
    empty.onReduce(HITS, max(7), PipelineTree.EMPTY, false);
    assertTrue(empty.snapshot().isEmpty());
  }

  private static PartialAggregation maxOf() {
    return new PartialAggregation(5, 100, rowType("m", SqlTypeName.DOUBLE));
  }

  private static PartialAggregation termsOf(int maxBuckets) {
    PartialAggregation aggregation =
        new PartialAggregation(5, maxBuckets, rowType("k", SqlTypeName.VARCHAR));
    aggregation.rootRows(
        (hits, aggs) ->
            ((StringTerms) aggs.asList().get(0))
                .getBuckets().stream().map(b -> new Object[] {b.getKeyAsString()}).toList());
    return aggregation;
  }

  private static double cell(PartialAggregation aggregation) {
    return aggregation.snapshot().orElseThrow().rows().get(0).tupleValue().get("m").doubleValue();
  }

  private static double maxValue(InternalAggregations aggs) {
    return ((InternalMax) aggs.asList().get(0)).getValue();
  }

  private static RelDataType rowType(String name, SqlTypeName type) {
    RelDataTypeFactory factory = new JavaTypeFactoryImpl();
    return factory.builder().add(name, factory.createSqlType(type)).build();
  }

  private static InternalAggregations max(double value) {
    return InternalAggregations.from(
        List.of(new InternalMax("m", value, DocValueFormat.RAW, null)));
  }

  private static InternalAggregations terms(int size, String... keys) {
    List<StringTerms.Bucket> buckets =
        java.util.Arrays.stream(keys)
            .map(
                key ->
                    new StringTerms.Bucket(
                        new BytesRef(key),
                        1,
                        InternalAggregations.EMPTY,
                        false,
                        0,
                        DocValueFormat.RAW))
            .toList();
    StringTerms terms =
        new StringTerms(
            "k",
            BucketOrder.key(true),
            BucketOrder.key(true),
            null,
            DocValueFormat.RAW,
            size,
            false,
            0,
            buckets,
            0,
            new BucketCountThresholds(1, 0, size, size));
    return InternalAggregations.from(List.of(terms));
  }
}
