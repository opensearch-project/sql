/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.List;
import java.util.Map;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchProgressListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchShard;
import org.opensearch.action.search.SearchTask;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.InternalComposite;

/**
 * Best-effort progress of one index scan, built from signals the searches already return.
 *
 * <ul>
 *   <li>A single-request source (plain search or non-composite aggregation) reports the fraction of
 *       shards that finished the query phase of its in-flight request.
 *   <li>A PIT search reports hits read over the response's total hits, capped by the row limit.
 *       When the total is only a lower bound, the estimate grows with the hits read, so no exact
 *       hit count is required.
 *   <li>A composite aggregation reports bucket document counts over an exact total hit count.
 *       Aggregations usually report only a lower bound, and then it reports buckets read over the
 *       row limit, which bounds the buckets the scan returns.
 *   <li>Before a paged source's first response, its finished shards count toward the first page,
 *       which is at least {@code page size / row limit} of the rows the scan returns.
 * </ul>
 *
 * <p>Thread-safe: shard callbacks arrive on transport threads while the scan reads responses.
 */
public final class SourceProgress {

  private boolean complete;
  private boolean paged;
  private boolean composite;
  private long rowLimit = Long.MAX_VALUE;
  private int responses;
  private double firstPageShare;
  private long unitsRead;
  private long lastPageUnits;
  private long bucketsRead;
  private long lastPageBuckets;
  private long totalUnits = -1L;
  private boolean totalExact;
  private int totalShards;
  private int finishedShards;

  SourceProgress() {}

  /**
   * Starts one pass over the scan with the largest number of rows it returns. A scan iterated again
   * (e.g. the inner side of a nested-loop join) starts a fresh estimate; a finished source stays
   * finished.
   */
  public synchronized void start(long rowLimit) {
    this.rowLimit = Math.max(0L, rowLimit);
    if (!complete) {
      responses = 0;
      firstPageShare = 0D;
      unitsRead = 0L;
      lastPageUnits = 0L;
      totalUnits = -1L;
      totalExact = false;
      composite = false;
      bucketsRead = 0L;
      lastPageBuckets = 0L;
    }
  }

  /** Returns a copy of {@code request} whose search task reports shard progress to this source. */
  public SearchRequest track(SearchRequest request) {
    beginRequest(isPaged(request), pageSize(request));
    ShardListener listener = new ShardListener();
    SearchRequest tracked =
        new SearchRequest(request) {
          @Override
          public SearchTask createTask(
              long id,
              String type,
              String action,
              TaskId parentTaskId,
              Map<String, String> headers) {
            SearchTask task = super.createTask(id, type, action, parentTaskId, headers);
            task.setProgressListener(listener);
            return task;
          }
        };
    // The copy constructor does not carry the search pipeline.
    return tracked.pipeline(request.pipeline());
  }

  /** Records the units and total hits of one completed search response. */
  public synchronized void onResponse(SearchResponse response) {
    List<InternalComposite> composites =
        response.getAggregations() == null
            ? List.of()
            : response.getAggregations().asList().stream()
                .filter(InternalComposite.class::isInstance)
                .map(InternalComposite.class::cast)
                .toList();
    long units;
    if (!composites.isEmpty()) {
      composite = true;
      lastPageBuckets = composites.stream().mapToLong(agg -> agg.getBuckets().size()).sum();
      bucketsRead += lastPageBuckets;
      units =
          composites.stream()
              .flatMap(agg -> agg.getBuckets().stream())
              .mapToLong(InternalComposite.InternalBucket::getDocCount)
              .sum();
    } else {
      units = response.getHits() == null ? 0L : response.getHits().getHits().length;
    }
    TotalHits total = response.getHits() == null ? null : response.getHits().getTotalHits();
    if (total != null) {
      totalUnits = Math.max(totalUnits, total.value());
      totalExact = total.relation() == TotalHits.Relation.EQUAL_TO;
    }
    responses++;
    unitsRead += units;
    lastPageUnits = units;
    totalShards = 0;
    finishedShards = 0;
  }

  /** Marks the scan as finished; it reports {@code 1.0} from now on. */
  public synchronized void complete() {
    complete = true;
  }

  /** Returns this source's progress in {@code [0.0, 1.0]}. */
  public synchronized double fractionDone() {
    if (complete) {
      return 1D;
    }
    double inFlight = totalShards > 0 ? (double) finishedShards / totalShards : 0D;
    if (!paged) {
      // A single-request source sends no further search after its response.
      return responses > 0 ? 1D : inFlight;
    }
    if (responses == 0) {
      return inFlight * firstPageShare;
    }
    if (composite && !totalExact) {
      return ratio(bucketsRead + inFlight * lastPageBuckets, rowLimit);
    }
    return ratio(unitsRead + inFlight * lastPageUnits, estimate());
  }

  private static double ratio(double done, long estimate) {
    if (estimate <= 0L) {
      return 0D;
    }
    return Math.min(1D, done / Math.max(estimate, done));
  }

  private long estimate() {
    long bound =
        totalUnits < 0L || !totalExact
            ? Math.max(totalUnits, unitsRead + lastPageUnits)
            : totalUnits;
    // The row limit counts PIT hits, not the document counts measured for composite.
    return composite ? bound : Math.min(bound, rowLimit);
  }

  private synchronized void beginRequest(boolean paged, int pageSize) {
    this.paged |= paged;
    if (responses == 0 && pageSize > 0 && rowLimit > 0L) {
      firstPageShare = Math.min(1D, (double) pageSize / rowLimit);
    }
    totalShards = 0;
    finishedShards = 0;
  }

  private synchronized void onShards(int total, int skipped) {
    totalShards = total;
    finishedShards = Math.min(total, skipped);
  }

  private synchronized void onShardDone() {
    finishedShards = Math.min(totalShards, finishedShards + 1);
  }

  private static boolean isPaged(SearchRequest request) {
    return request.source() != null
        && (request.source().pointInTimeBuilder() != null || composite(request) != null);
  }

  /** Rows one page can return: buckets for composite, hits for PIT; {@code -1} if unknown. */
  private static int pageSize(SearchRequest request) {
    if (request.source() == null) {
      return -1;
    }
    CompositeAggregationBuilder composite = composite(request);
    return composite != null ? composite.size() : request.source().size();
  }

  private static CompositeAggregationBuilder composite(SearchRequest request) {
    if (request.source().aggregations() == null) {
      return null;
    }
    return request.source().aggregations().getAggregatorFactories().stream()
        .filter(CompositeAggregationBuilder.class::isInstance)
        .map(CompositeAggregationBuilder.class::cast)
        .findFirst()
        .orElse(null);
  }

  /** Counts shards that finished the query phase; skipped shards count as finished. */
  final class ShardListener extends SearchProgressListener {

    @Override
    protected void onListShards(
        List<SearchShard> shards,
        List<SearchShard> skippedShards,
        SearchResponse.Clusters clusters,
        boolean fetchPhase) {
      onShards(shards.size() + skippedShards.size(), skippedShards.size());
    }

    @Override
    protected void onQueryResult(int shardIndex) {
      onShardDone();
    }

    @Override
    protected void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
      onShardDone();
    }
  }
}
