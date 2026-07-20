/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.aggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.InternalNumericMetricsAggregation;

/** Exact transport and reduction result for {@code checked_long_sum}. */
public class InternalCheckedLongSum extends InternalNumericMetricsAggregation.SingleValue {

  private final long sum;
  private final boolean hasValue;

  public InternalCheckedLongSum(
      String name, long sum, boolean hasValue, Map<String, Object> metadata) {
    super(name, metadata);
    this.sum = sum;
    this.hasValue = hasValue;
  }

  public InternalCheckedLongSum(StreamInput in) throws IOException {
    super(in);
    hasValue = in.readBoolean();
    sum = in.readLong();
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeBoolean(hasValue);
    out.writeLong(sum);
  }

  @Override
  public String getWriteableName() {
    return CheckedLongSumAggregationBuilder.NAME;
  }

  public Long longValue() {
    return hasValue ? sum : null;
  }

  @Override
  public double value() {
    return hasValue ? sum : Double.NaN;
  }

  @Override
  public InternalCheckedLongSum reduce(
      List<InternalAggregation> aggregations, ReduceContext reduceContext) {
    long reduced = 0L;
    boolean anyValue = false;
    for (InternalAggregation aggregation : aggregations) {
      InternalCheckedLongSum checkedSum = (InternalCheckedLongSum) aggregation;
      if (checkedSum.hasValue) {
        reduced = CheckedLongSum.add(reduced, checkedSum.sum);
        anyValue = true;
      }
    }
    return new InternalCheckedLongSum(name, reduced, anyValue, getMetadata());
  }

  @Override
  public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
    if (hasValue) {
      builder.field(CommonFields.VALUE.getPreferredName(), sum);
    } else {
      builder.nullField(CommonFields.VALUE.getPreferredName());
    }
    return builder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), sum, hasValue);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass() || !super.equals(obj)) {
      return false;
    }
    InternalCheckedLongSum that = (InternalCheckedLongSum) obj;
    return sum == that.sum && hasValue == that.hasValue;
  }
}
