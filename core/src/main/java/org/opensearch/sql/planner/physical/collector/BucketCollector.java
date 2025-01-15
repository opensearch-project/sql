/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.collector;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

/** Collect Bucket from {@link BindingTuple}. */
@RequiredArgsConstructor
public class BucketCollector implements Collector {

  /** Bucket Expression. */
  private final NamedExpression bucketExpr;

  /** Collector Constructor. */
  private final Supplier<Collector> supplier;

  /**
   * Map from bucketKey to nested collector sorted by key to make sure final result is in order
   * after traversal.
   */
  private final Map<ExprValue, Collector> collectorMap = new TreeMap<>();

  /** Bucket Index. */
  private int bucketIndex = 0;

  /**
   * Collect Bucket from {@link BindingTuple}. If bucket not exist, create new bucket and {@link
   * Collector}. If bucket exist, let {@link Collector} in the bucket collect from {@link
   * BindingTuple}.
   *
   * @param input {@link BindingTuple}.
   */
  @Override
  public void collect(BindingTuple input) {
    ExprValue bucketKey = bucketKey(input);
    collectorMap.putIfAbsent(bucketKey, supplier.get());
    collectorMap.get(bucketKey).collect(input);
  }

  /**
   * Bucket Key.
   *
   * @param tuple {@link BindingTuple}.
   * @return Bucket Key.
   */
  protected ExprValue bucketKey(BindingTuple tuple) {
    return bucketExpr.valueOf(tuple);
  }

  /**
   * Get result from all the buckets.
   *
   * @return list of {@link ExprValue}.
   */
  @Override
  public List<ExprValue> results() {
    ExprValue[] buckets = allocateBuckets();
    for (Map.Entry<ExprValue, Collector> entry : collectorMap.entrySet()) {
      ImmutableList.Builder<ExprValue> builder = new ImmutableList.Builder<>();
      for (ExprValue tuple : entry.getValue().results()) {
        LinkedHashMap<String, ExprValue> tmp = new LinkedHashMap<>();
        tmp.put(bucketExpr.getName(), entry.getKey());
        tmp.putAll(tuple.tupleValue());
        builder.add(ExprTupleValue.fromExprValueMap(tmp));
      }
      buckets[locateBucket(entry.getKey())] = new ExprCollectionValue(builder.build());
    }
    return Arrays.stream(buckets)
        .filter(Objects::nonNull)
        .flatMap(v -> v.collectionValue().stream())
        .collect(Collectors.toList());
  }

  /**
   * Allocates Buckets for building results.
   *
   * @return buckets.
   */
  protected ExprValue[] allocateBuckets() {
    return new ExprValue[collectorMap.size()];
  }

  /**
   * Current Bucket index in allocated buckets.
   *
   * @param value bucket key.
   * @return index.
   */
  protected int locateBucket(ExprValue value) {
    return bucketIndex++;
  }
}
