/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import static org.opensearch.search.internal.SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;

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
 *   <li>A PIT search reports hits read over the total hits, capped by the row limit.
 *   <li>A composite aggregation reports bucket document counts over the total hits.
 *   <li>The first page of a paged source asks for the total hit count it can afford: exact for
 *       composite, whose pages visit every matching document anyway, and for a PIT scan with no row
 *       limit, which reads every match anyway. A PIT scan with a row limit keeps the default
 *       threshold, so it costs no more than a synchronous scan. PIT pages read one snapshot, so
 *       later pages need no count. Without the total, PIT falls back to an estimate that grows with
 *       the hits read, and composite to buckets read over the row limit.
 *   <li>Before a paged source's first response, its finished shards count toward the first page,
 *       which is at least {@code page size / row limit} of the rows the scan returns.
 * </ul>
 *
 * <p>Each pass over the scan (a scan may be iterated again, e.g. the inner side of a nested-loop
 * join) has its own state, so a late response from an earlier pass cannot change the current one.
 * Thread-safe: shard callbacks arrive on transport threads while the scan reads responses.
 */
public final class SourceProgress {

  private boolean complete;
  private Pass pass = new Pass(Long.MAX_VALUE);

  SourceProgress() {}

  /** Starts a pass over the scan with the largest number of rows it returns. */
  public synchronized void start(long rowLimit) {
    pass = new Pass(Math.max(0L, rowLimit));
  }

  /** Returns a copy of {@code request} whose search reports its progress to the current pass. */
  public synchronized Tracked track(SearchRequest request) {
    Pass current = pass;
    boolean firstPage = current.beginRequest(isPaged(request), pageSize(request));
    ShardListener listener = new ShardListener(current);
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
    if (firstPage) {
      // Copy the source: the scan reuses its own builder for later pages.
      tracked.source(
          request.source().shallowCopy().trackTotalHitsUpTo(current.totalHitsToTrack(request)));
    }
    // The copy constructor does not carry the search pipeline.
    return new Tracked(tracked.pipeline(request.pipeline()), current);
  }

  /** Marks the scan as finished; it reports {@code 1.0} from now on. */
  public synchronized void complete() {
    complete = true;
  }

  /** Returns this source's progress in {@code [0.0, 1.0]}. */
  public synchronized double fractionDone() {
    return complete ? 1D : pass.fractionDone();
  }

  /** A search request bound to the pass it was sent in. */
  public final class Tracked {
    private final SearchRequest request;
    private final Pass pass;

    private Tracked(SearchRequest request, Pass pass) {
      this.request = request;
      this.pass = pass;
    }

    /** The request to send in place of the original. */
    public SearchRequest request() {
      return request;
    }

    /** Records the units and total hits of the response to {@link #request()}. */
    public void onResponse(SearchResponse response) {
      synchronized (SourceProgress.this) {
        pass.onResponse(response);
      }
    }
  }

  /** State of one pass over the scan; guarded by the enclosing {@link SourceProgress}. */
  private static final class Pass {
    private final long rowLimit;
    private boolean paged;
    private boolean composite;
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

    private Pass(long rowLimit) {
      this.rowLimit = rowLimit;
    }

    /** Returns whether this request is the first page of a paged pass. */
    private boolean beginRequest(boolean paged, int pageSize) {
      this.paged |= paged;
      if (responses == 0 && pageSize > 0 && rowLimit > 0L) {
        firstPageShare = Math.min(1D, (double) pageSize / rowLimit);
      }
      totalShards = 0;
      finishedShards = 0;
      return paged && responses == 0;
    }

    private int totalHitsToTrack(SearchRequest request) {
      return composite(request) != null || rowLimit >= Integer.MAX_VALUE
          ? Integer.MAX_VALUE
          : DEFAULT_TRACK_TOTAL_HITS_UP_TO;
    }

    private void onResponse(SearchResponse response) {
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
      // Only the first page asks for a count; later pages report the default lower bound.
      if (total != null && !totalExact) {
        totalUnits = Math.max(totalUnits, total.value());
        totalExact =
            total.relation() == TotalHits.Relation.EQUAL_TO
                || (!composite && total.value() >= rowLimit);
      }
      responses++;
      unitsRead += units;
      lastPageUnits = units;
      totalShards = 0;
      finishedShards = 0;
    }

    private double fractionDone() {
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

    private long estimate() {
      long bound = totalExact ? totalUnits : Math.max(totalUnits, unitsRead + lastPageUnits);
      // The row limit counts PIT hits, not the document counts measured for composite.
      return composite ? bound : Math.min(bound, rowLimit);
    }

    private void onShards(int total, int skipped) {
      totalShards = total;
      finishedShards = Math.min(total, skipped);
    }

    private void onShardDone() {
      finishedShards = Math.min(totalShards, finishedShards + 1);
    }
  }

  private static double ratio(double done, long estimate) {
    if (estimate <= 0L) {
      return 0D;
    }
    return Math.min(1D, done / Math.max(estimate, done));
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
    if (request.source() == null || request.source().aggregations() == null) {
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
    private final Pass pass;

    private ShardListener(Pass pass) {
      this.pass = pass;
    }

    @Override
    protected void onListShards(
        List<SearchShard> shards,
        List<SearchShard> skippedShards,
        SearchResponse.Clusters clusters,
        boolean fetchPhase) {
      synchronized (SourceProgress.this) {
        pass.onShards(shards.size() + skippedShards.size(), skippedShards.size());
      }
    }

    @Override
    protected void onQueryResult(int shardIndex) {
      synchronized (SourceProgress.this) {
        pass.onShardDone();
      }
    }

    @Override
    protected void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
      synchronized (SourceProgress.this) {
        pass.onShardDone();
      }
    }
  }
}
