/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import com.google.common.collect.Iterators;
import java.util.AbstractCollection;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder.PushDownUnSupportedException;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/** Push down context is used to store all the push down operations that are applied to the query */
@Getter
public class PushDownContext extends AbstractCollection<PushDownOperation> {
  private final OpenSearchIndex osIndex;
  private ArrayDeque<PushDownOperation> operationsForRequestBuilder;

  private boolean isAggregatePushed = false;
  private AggPushDownAction aggPushDownAction;
  private ArrayDeque<PushDownOperation> operationsForAgg;

  // Records the start pos of the query, which is updated by new added limit operations.
  private int startFrom = 0;

  private boolean isLimitPushed = false;
  private boolean isProjectPushed = false;
  private boolean isMeasureOrderPushed = false;
  private boolean isSortPushed = false;
  private boolean isTopKPushed = false;
  private boolean isRareTopPushed = false;

  public PushDownContext(OpenSearchIndex osIndex) {
    this.osIndex = osIndex;
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
      startFrom += ((LimitDigest) operation.digest()).offset();
      if (startFrom >= osIndex.getMaxResultWindow()) {
        throw new PushDownUnSupportedException(
            String.format(
                "Requested offset %d should be less than the max result window %d",
                startFrom, osIndex.getMaxResultWindow()));
      }
      isLimitPushed = true;
      if (isSortPushed || isMeasureOrderPushed) {
        isTopKPushed = true;
      }
    }
    if (operation.type() == PushDownType.PROJECT) {
      isProjectPushed = true;
    }
    if (operation.type() == PushDownType.SORT) {
      isSortPushed = true;
    }
    if (operation.type() == PushDownType.SORT_AGG_METRICS) {
      isMeasureOrderPushed = true;
    }
    if (operation.type() == PushDownType.RARE_TOP) {
      isRareTopPushed = true;
    }
    operation.action().pushOperation(this, operation);
    return true;
  }

  public void add(PushDownType type, Object digest, AbstractAction<?> action) {
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
