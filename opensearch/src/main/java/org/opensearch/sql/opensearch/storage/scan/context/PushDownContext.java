/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import java.util.AbstractCollection;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.jetbrains.annotations.NotNull;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder.PushDownUnSupportedException;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/** Push down context is used to store all the push down operations that are applied to the query */
@Getter
public class PushDownContext extends AbstractCollection<PushDownOperation> {
  private final OpenSearchIndex osIndex;
  private ArrayDeque<PushDownOperation> queue = new ArrayDeque<>();
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
  private boolean isSortExprPushed = false;
  private boolean isTopKPushed = false;
  private boolean isRareTopPushed = false;
  private boolean isScriptPushed = false;

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
      if (action.type() != PushDownType.SORT && action.type() != PushDownType.SORT_EXPR) {
        newContext.add(action);
      }
    }
    return newContext;
  }

  /**
   * Aggregation will eliminate all collations, thus we need to remove sort. Will also remove
   * project as sources is useless for aggregation, and remove filter if it's derived from the
   * aggregate.
   */
  public PushDownContext cloneForAggregate(Aggregate aggregate, @Nullable Project project) {
    PushDownContext newContext = new PushDownContext(osIndex);
    ArrayDeque<PushDownOperation> tempQueue = new ArrayDeque<>(this.queue);
    while (!tempQueue.isEmpty()) {
      PushDownOperation operation = tempQueue.pollFirst();
      if (operation.type() == PushDownType.SORT
          || operation.type() == PushDownType.SORT_EXPR
          || operation.type() == PushDownType.PROJECT) {
        continue;
      }
      if (operation.type() == PushDownType.FILTER) {
        List<Integer> currentColumns = null;
        // If project push down happens between this aggregate push down and previous filter push
        // down,
        // there is a single project left in the queue. That project will affect the mapping between
        // them.
        while (!tempQueue.isEmpty() && tempQueue.peekFirst().type() == PushDownType.PROJECT) {
          ProjectDigest projectDigest = (ProjectDigest) tempQueue.pollFirst().digest();
          List<Integer> selectedColumns = projectDigest.selectedColumns();
          currentColumns =
              currentColumns == null
                  ? selectedColumns
                  : selectedColumns.stream().map(currentColumns::get).toList();
        }
        // Check if filter is derived from aggregate. Ensure there is no other push down operations
        // left between them.
        if (tempQueue.isEmpty()
            && PlanUtils.isNotNullDerivedFromAgg(
                ((FilterDigest) operation.digest()).condition(),
                aggregate,
                project,
                currentColumns)) {
          continue;
        }
      }
      newContext.add(operation);
    }
    return newContext;
  }

  @NotNull
  @Override
  public Iterator<PushDownOperation> iterator() {
    return queue.iterator();
  }

  @Override
  public int size() {
    return queue.size();
  }

  void addOperationForRequestBuilder(PushDownOperation operation) {
    if (operationsForRequestBuilder == null) {
      this.operationsForRequestBuilder = new ArrayDeque<>();
    }
    operationsForRequestBuilder.add(operation);
    queue.add(operation);
  }

  void addOperationForAgg(PushDownOperation operation) {
    if (operationsForAgg == null) {
      this.operationsForAgg = new ArrayDeque<>();
    }
    operationsForAgg.add(operation);
    queue.add(operation);
  }

  @Override
  public boolean add(PushDownOperation operation) {
    operation.action().pushOperation(this, operation);
    if (operation.type() == PushDownType.AGGREGATION) {
      isAggregatePushed = true;
      this.aggPushDownAction = (AggPushDownAction) operation.action();
    }
    if (operation.type() == PushDownType.LIMIT) {
      startFrom += ((LimitDigest) operation.digest()).offset();
      if (startFrom >= osIndex.getMaxResultWindow()) {
        throw new PushDownUnSupportedException(
            String.format(
                "[INTERNAL] Requested offset %d should be less than the max result window %d",
                startFrom, osIndex.getMaxResultWindow()));
      }
      isLimitPushed = true;
      if (isSortPushed || isMeasureOrderPushed || isSortExprPushed) {
        isTopKPushed = true;
      }
    }
    if (operation.type() == PushDownType.PROJECT) {
      isProjectPushed = true;
    }
    if (operation.type() == PushDownType.SORT) {
      isSortPushed = true;
    }
    if (operation.type() == PushDownType.SORT_EXPR) {
      isSortExprPushed = true;
    }
    if (operation.type() == PushDownType.SORT_AGG_METRICS) {
      isMeasureOrderPushed = true;
    }
    if (operation.type() == PushDownType.RARE_TOP) {
      isRareTopPushed = true;
    }
    if (operation.type() == PushDownType.SCRIPT) {
      isScriptPushed = true;
    }
    return true;
  }

  public void add(PushDownType type, Object digest, AbstractAction<?> action) {
    add(new PushDownOperation(type, digest, action));
  }

  public boolean containsDigest(Object digest) {
    return this.stream().anyMatch(action -> action.digest().equals(digest));
  }

  // TODO check on adding
  public boolean containsDigestOnTop(Object digest) {
    return this.queue.peekLast() != null && this.queue.peekLast().digest().equals(digest);
  }

  /**
   * Get the digest of the first operation of a specific type.
   *
   * @param type The PushDownType to get the digest for
   * @return The digest object, or null if no operation of the specified type exists
   */
  public Object getDigestByType(PushDownType type) {
    return this.stream()
        .filter(operation -> operation.type() == type)
        .map(PushDownOperation::digest)
        .findFirst()
        .orElse(null);
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
