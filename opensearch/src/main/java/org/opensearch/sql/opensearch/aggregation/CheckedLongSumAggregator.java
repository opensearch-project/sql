/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.aggregation;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.LongArray;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

/** Collects signed integral doc values into exact, checked long sums. */
class CheckedLongSumAggregator extends NumericMetricsAggregator.SingleValue {

  private final ValuesSource.Numeric valuesSource;
  private LongArray sums;
  private LongArray seen;

  CheckedLongSumAggregator(
      String name,
      ValuesSourceConfig valuesSourceConfig,
      SearchContext context,
      Aggregator parent,
      Map<String, Object> metadata)
      throws IOException {
    super(name, context, parent, metadata);
    valuesSource =
        valuesSourceConfig.hasValues()
            ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource()
            : null;
    if (valuesSource != null && (valuesSource.isFloatingPoint() || valuesSource.isBigInteger())) {
      throw new IllegalArgumentException(
          "checked_long_sum requires a signed integral numeric field");
    }
    if (valuesSource != null) {
      sums = context.bigArrays().newLongArray(1, true);
      seen = context.bigArrays().newLongArray(1, true);
    }
  }

  @Override
  public ScoreMode scoreMode() {
    return valuesSource != null && valuesSource.needsScores()
        ? ScoreMode.COMPLETE
        : ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  public LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector sub)
      throws IOException {
    if (valuesSource == null) {
      return LeafBucketCollector.NO_OP_COLLECTOR;
    }

    BigArrays bigArrays = this.context.bigArrays();
    SortedNumericDocValues values = valuesSource.longValues(context);
    return new LeafBucketCollectorBase(sub, values) {
      @Override
      public void collect(int doc, long bucket) throws IOException {
        if (!values.advanceExact(doc)) {
          return;
        }
        sums = bigArrays.grow(sums, bucket + 1);
        seen = bigArrays.grow(seen, bucket + 1);
        long sum = sums.get(bucket);
        for (int i = 0; i < values.docValueCount(); i++) {
          sum = CheckedLongSum.add(sum, values.nextValue());
        }
        sums.set(bucket, sum);
        seen.set(bucket, 1);
      }
    };
  }

  @Override
  public double metric(long owningBucketOrd) {
    return hasValue(owningBucketOrd) ? sums.get(owningBucketOrd) : Double.NaN;
  }

  @Override
  public InternalAggregation buildAggregation(long bucket) {
    if (!hasValue(bucket)) {
      return buildEmptyAggregation();
    }
    return new InternalCheckedLongSum(name, sums.get(bucket), true, metadata());
  }

  @Override
  public InternalAggregation buildEmptyAggregation() {
    return new InternalCheckedLongSum(name, 0L, false, metadata());
  }

  @Override
  public void doReset() {
    if (sums != null) {
      sums.fill(0, sums.size(), 0L);
      seen.fill(0, seen.size(), 0L);
    }
  }

  @Override
  protected void doClose() {
    Releasables.close(sums, seen);
  }

  private boolean hasValue(long bucket) {
    return valuesSource != null && bucket < seen.size() && seen.get(bucket) != 0;
  }
}
