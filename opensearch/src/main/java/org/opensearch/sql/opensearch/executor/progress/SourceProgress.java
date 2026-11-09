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
 * Progress of one index scan. A single-request search reports finished shards; a PIT or composite
 * scan reports hits or bucket doc counts read over the total hits from its first page.
 */
public final class SourceProgress {

  private boolean complete;
  private Pass pass = new Pass(Long.MAX_VALUE);

  SourceProgress() {}

  /** Starts a new pass, e.g. when the inner side of a nested-loop join is scanned again. */
  public synchronized void start(long rowLimit) {
    pass = new Pass(Math.max(0L, rowLimit));
  }

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
      // Copy: the scan reuses its builder for later pages.
      tracked.source(
          request.source().shallowCopy().trackTotalHitsUpTo(current.totalHitsToTrack(request)));
    }
    // The copy constructor drops the pipeline.
    return new Tracked(tracked.pipeline(request.pipeline()), current);
  }

  public synchronized void complete() {
    complete = true;
  }

  public synchronized double fractionDone() {
    return complete ? 1D : pass.fractionDone();
  }

  /**
   * A search request bound to the pass it was sent in, so a late response cannot reach a newer one.
   */
  public final class Tracked {
    private final SearchRequest request;
    private final Pass pass;

    private Tracked(SearchRequest request, Pass pass) {
      this.request = request;
      this.pass = pass;
    }

    public SearchRequest request() {
      return request;
    }

    public void onResponse(SearchResponse response) {
      synchronized (SourceProgress.this) {
        pass.onResponse(response);
      }
    }
  }

  private static final class Pass {
    private final long rowLimit;
    private boolean paged;
    private boolean composite;
    private int responses;
    private double firstPageShare;
    private long unitsRead;
    private long lastPageUnits;
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
        // The scan returns at most rowLimit rows, so the first page is at least this share.
        firstPageShare = Math.min(1D, (double) pageSize / rowLimit);
      }
      totalShards = 0;
      finishedShards = 0;
      return paged && responses == 0;
    }

    /**
     * Count exactly only when it costs nothing extra: composite visits every match, and a PIT scan
     * without a row limit reads every match.
     */
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
        units =
            composites.stream()
                .flatMap(agg -> agg.getBuckets().stream())
                .mapToLong(InternalComposite.InternalBucket::getDocCount)
                .sum();
      } else {
        units = response.getHits() == null ? 0L : response.getHits().getHits().length;
      }
      TotalHits total = response.getHits() == null ? null : response.getHits().getTotalHits();
      // Later pages report only the default lower bound; keep the first page's total.
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
        return responses > 0 ? 1D : inFlight;
      }
      if (responses == 0) {
        return inFlight * firstPageShare;
      }
      long estimate = estimate();
      if (estimate <= 0L) {
        return 0D;
      }
      double done = unitsRead + inFlight * lastPageUnits;
      return Math.min(1D, done / Math.max(estimate, done));
    }

    private long estimate() {
      long bound = totalExact ? totalUnits : Math.max(totalUnits, unitsRead + lastPageUnits);
      // The row limit counts hits, not composite doc counts.
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

  private static boolean isPaged(SearchRequest request) {
    return request.source() != null
        && (request.source().pointInTimeBuilder() != null || composite(request) != null);
  }

  /** Buckets for composite, hits for PIT; {@code -1} if unknown. */
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
