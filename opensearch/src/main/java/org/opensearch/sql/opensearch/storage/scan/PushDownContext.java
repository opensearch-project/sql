/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import com.google.common.collect.Iterators;
import java.util.AbstractCollection;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import lombok.Getter;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

@Getter
public class PushDownContext extends AbstractCollection<PushDownOperation> {
  private final OpenSearchIndex osIndex;
  private final OpenSearchRequestBuilder requestBuilder;
  private ArrayDeque<PushDownOperation> operationsForRequestBuilder;

  private boolean isAggregatePushed = false;
  private AggPushDownAction aggPushDownAction;
  private ArrayDeque<PushDownOperation> operationsForAgg;

  private boolean isLimitPushed = false;
  private boolean isProjectPushed = false;

  public PushDownContext(OpenSearchIndex osIndex) {
    this.osIndex = osIndex;
    this.requestBuilder = osIndex.createRequestBuilder();
  }

  @Override
  public PushDownContext clone() {
    PushDownContext newContext = new PushDownContext(osIndex);
    newContext.addAll(this);
    return newContext;
  }

  /**
   * Create a new {@link PushDownContext} without the collation action.
   *
   * @return A new push-down context without the collation action.
   */
  public PushDownContext cloneWithoutSort() {
    PushDownContext newContext = new PushDownContext(osIndex);
    for (PushDownOperation action : this) {
      if (action.type() != PushDownType.SORT) {
        newContext.add(action);
      }
    }
    return newContext;
  }

  @NotNull
  @Override
  public Iterator<PushDownOperation> iterator() {
    if (operationsForRequestBuilder == null) {
      return Collections.emptyIterator();
    } else if (operationsForAgg == null) {
      return operationsForRequestBuilder.iterator();
    } else {
      return Iterators.concat(operationsForRequestBuilder.iterator(), operationsForAgg.iterator());
    }
  }

  @Override
  public int size() {
    return (operationsForRequestBuilder == null ? 0 : operationsForRequestBuilder.size())
        + (operationsForAgg == null ? 0 : operationsForAgg.size());
  }

  ArrayDeque<PushDownOperation> getOperationsForRequestBuilder() {
    if (operationsForRequestBuilder == null) {
      this.operationsForRequestBuilder = new ArrayDeque<>();
    }
    return operationsForRequestBuilder;
  }

  ArrayDeque<PushDownOperation> getOperationsForAgg() {
    if (operationsForAgg == null) {
      this.operationsForAgg = new ArrayDeque<>();
    }
    return operationsForAgg;
  }

  @Override
  public boolean add(PushDownOperation operation) {
    if (operation.type() == PushDownType.AGGREGATION) {
      isAggregatePushed = true;
      this.aggPushDownAction = (AggPushDownAction) operation.action();
    }
    if (operation.type() == PushDownType.LIMIT) {
      isLimitPushed = true;
    }
    if (operation.type() == PushDownType.PROJECT) {
      isProjectPushed = true;
    }
    operation.action().transform(this, operation);
    return true;
  }

  void add(PushDownType type, Object digest, AbstractAction<?> action) {
    add(new PushDownOperation(type, digest, action));
  }

  public boolean containsDigest(Object digest) {
    return this.stream().anyMatch(action -> action.digest().equals(digest));
  }

  public OpenSearchRequestBuilder createRequestBuilder() {
    OpenSearchRequestBuilder newRequestBuilder = osIndex.createRequestBuilder();
    if (operationsForRequestBuilder != null) {
      operationsForRequestBuilder.forEach(
          operation -> ((OSRequestBuilderAction) operation.action()).apply(newRequestBuilder));
    }
    return newRequestBuilder;
  }
}

enum PushDownType {
  FILTER,
  PROJECT,
  AGGREGATION,
  SORT,
  LIMIT,
  SCRIPT,
  COLLAPSE
  // HIGHLIGHT,
  // NESTED
}

/**
 * Represents a push down operation that can be applied to an OpenSearchRequestBuilder.
 *
 * @param type PushDownType enum
 * @param digest the digest of the pushed down operator
 * @param action the lambda action to apply on the OpenSearchRequestBuilder
 */
record PushDownOperation(PushDownType type, Object digest, AbstractAction<?> action) {
  public String toString() {
    return type + "->" + digest;
  }
}

interface AbstractAction<T> {
  void apply(T target);

  void transform(PushDownContext context, PushDownOperation operation);
}

interface OSRequestBuilderAction extends AbstractAction<OpenSearchRequestBuilder> {
  default void transform(PushDownContext context, PushDownOperation operation) {
    apply(context.getRequestBuilder());
    context.getOperationsForRequestBuilder().add(operation);
  }
}

interface AggregationBuilderAction extends AbstractAction<AggPushDownAction> {
  default void transform(PushDownContext context, PushDownOperation operation) {
    apply(context.getAggPushDownAction());
    context.getOperationsForAgg().add(operation);
  }
}

record FilterDigest(int scriptCount, RexNode condition) {
  @Override
  public String toString() {
    return condition.toString();
  }
}

record LimitDigest(int limit, int offset) {
  @Override
  public String toString() {
    return offset == 0 ? String.valueOf(limit) : "[" + limit + " from " + offset + "]";
  }
}

// TODO: shall we do deep copy for this action since it's mutable?
class AggPushDownAction implements OSRequestBuilderAction {

  private Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder;
  private final Map<String, OpenSearchDataType> extendedTypeMapping;
  @Getter private final long scriptCount;
  // Record the output field names of all buckets as the sequence of buckets
  private List<String> bucketNames;

  public AggPushDownAction(
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder,
      Map<String, OpenSearchDataType> extendedTypeMapping,
      List<String> bucketNames) {
    this.aggregationBuilder = aggregationBuilder;
    this.extendedTypeMapping = extendedTypeMapping;
    this.scriptCount =
        aggregationBuilder.getLeft().stream().filter(this::isScriptAggBuilder).count();
    this.bucketNames = bucketNames;
  }

  private boolean isScriptAggBuilder(AggregationBuilder aggBuilder) {
    return aggBuilder instanceof ValuesSourceAggregationBuilder<?> valueSourceAgg
        && valueSourceAgg.script() != null;
  }

  @Override
  public void apply(OpenSearchRequestBuilder requestBuilder) {
    requestBuilder.pushDownAggregation(aggregationBuilder);
    requestBuilder.pushTypeMapping(extendedTypeMapping);
  }

  public void pushDownSortIntoAggBucket(
      List<RelFieldCollation> collations, List<String> fieldNames) {
    // aggregationBuilder.getLeft() could be empty when count agg optimization works
    if (aggregationBuilder.getLeft().isEmpty()) return;
    AggregationBuilder builder = aggregationBuilder.getLeft().getFirst();
    List<String> selected = new ArrayList<>(collations.size());
    if (builder instanceof CompositeAggregationBuilder compositeAggBuilder) {
      // It will always use a single CompositeAggregationBuilder for the aggregation with GroupBy
      // See {@link AggregateAnalyzer}
      List<CompositeValuesSourceBuilder<?>> buckets = compositeAggBuilder.sources();
      List<CompositeValuesSourceBuilder<?>> newBuckets = new ArrayList<>(buckets.size());
      List<String> newBucketNames = new ArrayList<>(buckets.size());
      // Have to put the collation required buckets first, then the rest of buckets.
      collations.forEach(
          collation -> {
            /*
             Must find the bucket by field name because:
               1. The sequence of buckets may have changed after sort push-down.
               2. The schema of scan operator may be inconsistent with the sequence of buckets
               after project push-down.
            */
            String bucketName = fieldNames.get(collation.getFieldIndex());
            CompositeValuesSourceBuilder<?> bucket = buckets.get(bucketNames.indexOf(bucketName));
            Direction direction = collation.getDirection();
            NullDirection nullDirection = collation.nullDirection;
            SortOrder order =
                Direction.DESCENDING.equals(direction) ? SortOrder.DESC : SortOrder.ASC;
            if (bucket.missingBucket()) {
              MissingOrder missingOrder =
                  switch (nullDirection) {
                    case FIRST -> MissingOrder.FIRST;
                    case LAST -> MissingOrder.LAST;
                    default -> MissingOrder.DEFAULT;
                  };
              bucket.missingOrder(missingOrder);
            }
            newBuckets.add(bucket.order(order));
            newBucketNames.add(bucketName);
            selected.add(bucketName);
          });
      IntStream.range(0, buckets.size())
          .mapToObj(fieldNames::get)
          .filter(name -> !selected.contains(name))
          .forEach(
              name -> {
                newBuckets.add(buckets.get(bucketNames.indexOf(name)));
                newBucketNames.add(name);
              });
      Builder newAggBuilder = new Builder();
      compositeAggBuilder.getSubAggregations().forEach(newAggBuilder::addAggregator);
      aggregationBuilder =
          Pair.of(
              Collections.singletonList(
                  AggregationBuilders.composite("composite_buckets", newBuckets)
                      .subAggregations(newAggBuilder)
                      .size(compositeAggBuilder.size())),
              aggregationBuilder.getRight());
      bucketNames = newBucketNames;
    }
    if (builder instanceof TermsAggregationBuilder termsAggBuilder) {
      termsAggBuilder.order(BucketOrder.key(!collations.getFirst().getDirection().isDescending()));
    }
    // TODO for MultiTermsAggregationBuilder
  }

  /**
   * Check if the limit can be pushed down into aggregation bucket when the limit size is less than
   * bucket number.
   */
  public boolean pushDownLimitIntoBucketSize(Integer size) {
    // aggregationBuilder.getLeft() could be empty when count agg optimization works
    if (aggregationBuilder.getLeft().isEmpty()) return false;
    AggregationBuilder builder = aggregationBuilder.getLeft().getFirst();
    if (builder instanceof CompositeAggregationBuilder compositeAggBuilder) {
      if (size < compositeAggBuilder.size()) {
        compositeAggBuilder.size(size);
        return true;
      } else {
        return false;
      }
    }
    if (builder instanceof TermsAggregationBuilder termsAggBuilder) {
      if (size < termsAggBuilder.size()) {
        termsAggBuilder.size(size);
        return true;
      } else {
        return false;
      }
    }
    if (builder instanceof MultiTermsAggregationBuilder multiTermsAggBuilder) {
      if (size < multiTermsAggBuilder.size()) {
        multiTermsAggBuilder.size(size);
        return true;
      } else {
        return false;
      }
    }
    // now we only have Composite, Terms and MultiTerms bucket aggregations,
    // add code here when we could support more in the future.
    if (builder instanceof ValuesSourceAggregationBuilder.LeafOnly<?, ?>) {
      // Note: all metric aggregations will be treated as pushed since it generates only one row.
      return true;
    }
    throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
        "Unknown aggregation builder " + builder.getClass().getSimpleName());
  }
}
