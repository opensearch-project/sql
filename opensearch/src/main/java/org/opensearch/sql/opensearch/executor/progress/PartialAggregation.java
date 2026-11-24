/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.IntConsumer;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.util.BigArrays;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.job.QueryResult;
import org.opensearch.sql.opensearch.executor.ResultRows;

/**
 * The running result of an aggregation that is the query's root: the latest merge of the shard
 * results that have arrived, converted the same way as the final response.
 *
 * <p>A merge is built on the search thread that made it, before the coordinator merges it again:
 * reducing it elsewhere would race with that merge, which can change its inputs (a t-digest
 * compresses the digests it adds). It is built only once a reader has asked since the last one.
 */
public final class PartialAggregation {

  private static final Logger LOG = LogManager.getLogger(PartialAggregation.class);

  private final int batchedReduceSize;
  private final int maxBuckets;
  private final String[] columnNames;
  private final List<RelDataType> fieldTypes;
  private volatile BiFunction<TotalHits, InternalAggregations, List<Object[]>> rootRows;
  private volatile boolean wanted = true;
  private volatile Optional<QueryResult.Rows> latest = Optional.empty();

  public PartialAggregation(int batchedReduceSize, int maxBuckets, RelDataType rowType) {
    this.batchedReduceSize = batchedReduceSize;
    this.maxBuckets = maxBuckets;
    this.columnNames = rowType.getFieldNames().toArray(String[]::new);
    this.fieldTypes = rowType.getFieldList().stream().map(RelDataTypeField::getType).toList();
  }

  public int batchedReduceSize() {
    return batchedReduceSize;
  }

  /** Converts a merged result into the cells the root returns; empty if the request has none. */
  public void rootRows(BiFunction<TotalHits, InternalAggregations, List<Object[]>> rootRows) {
    this.rootRows = rootRows;
  }

  /** The latest merge built; asks for the next one. Does not block. */
  public Optional<QueryResult.Rows> snapshot() {
    wanted = true;
    return latest;
  }

  /** Called on the search thread that made the merge. */
  void onReduce(
      TotalHits totalHits,
      InternalAggregations aggregations,
      PipelineTree pipelines,
      boolean last) {
    BiFunction<TotalHits, InternalAggregations, List<Object[]>> convert = rootRows;
    if (!wanted || convert == null) {
      return;
    }
    wanted = false;
    try {
      latest = build(totalHits, aggregations, pipelines, last, convert);
    } catch (Exception e) {
      LOG.debug("Failed to build the running result of an aggregation", e);
    }
  }

  private Optional<QueryResult.Rows> build(
      TotalHits totalHits,
      InternalAggregations merged,
      PipelineTree pipelines,
      boolean last,
      BiFunction<TotalHits, InternalAggregations, List<Object[]>> convert)
      throws Exception {
    // A partial merge skips the final steps, such as trimming terms buckets to size.
    InternalAggregations aggregations =
        last
            ? merged
            : InternalAggregations.topLevelReduce(
                List.of(merged),
                ReduceContext.forFinalReduction(
                    BigArrays.NON_RECYCLING_INSTANCE, null, bucketLimit(), pipelines));
    List<Object[]> cells = convert.apply(totalHits, aggregations);
    if (cells.isEmpty()) {
      return Optional.empty();
    }
    List<ExprValue> tuples = new ArrayList<>(cells.size());
    for (Object[] row : cells) {
      tuples.add(ResultRows.tuple(columnNames, fieldTypes, row));
    }
    Schema schema = new Schema(ResultRows.columns(columnNames, fieldTypes, tuples));
    return Optional.of(new QueryResult.Rows(schema, tuples, Cursor.None, List.of(), 0));
  }

  /** Gives up on a merge with more buckets than a search may return. */
  private IntConsumer bucketLimit() {
    int[] buckets = {0};
    return count -> {
      buckets[0] += count;
      if (buckets[0] > maxBuckets) {
        throw new IllegalStateException("more than " + maxBuckets + " buckets");
      }
    };
  }
}
