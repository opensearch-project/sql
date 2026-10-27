/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.Test;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchShard;
import org.opensearch.action.search.SearchTask;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.composite.InternalComposite;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

class SourceProgressTest {

  private static final double DELTA = 1e-9;

  @Test
  void singleRequest_reportsFinishedShardsThenOne() {
    SourceProgress progress = new SourceProgress();
    SourceProgress.ShardListener listener = listenerOf(progress, plainSearch());

    listener.onListShards(shards(3), shards(1), null, false);
    assertEquals(0.25D, progress.fractionDone(), DELTA);
    listener.onQueryResult(0);
    listener.onQueryFailure(1, null, new RuntimeException());
    assertEquals(0.75D, progress.fractionDone(), DELTA);

    progress.onResponse(hitsResponse(4, 4, TotalHits.Relation.EQUAL_TO));
    assertEquals(1D, progress.fractionDone(), DELTA);
  }

  @Test
  void finishedShardsNeverExceedTotal() {
    SourceProgress progress = new SourceProgress();
    SourceProgress.ShardListener listener = listenerOf(progress, plainSearch());
    listener.onListShards(shards(1), List.of(), null, false);
    listener.onQueryResult(0);
    listener.onQueryResult(0);
    assertEquals(1D, progress.fractionDone(), DELTA);
  }

  @Test
  void pitPaging_readsUnitsOverExactTotal() {
    SourceProgress progress = new SourceProgress();
    progress.start(100);
    SourceProgress.ShardListener first = listenerOf(progress, pitSearch());
    first.onListShards(shards(2), List.of(), null, false);
    first.onQueryResult(0);
    // The first page is 10 of at most 100 rows, half of its shards have finished.
    assertEquals(0.05D, progress.fractionDone(), DELTA);

    progress.onResponse(hitsResponse(10, 40, TotalHits.Relation.EQUAL_TO));
    assertEquals(0.25D, progress.fractionDone(), DELTA);

    SourceProgress.ShardListener second = listenerOf(progress, pitSearch());
    second.onListShards(shards(2), List.of(), null, false);
    second.onQueryResult(0);
    assertEquals(0.375D, progress.fractionDone(), DELTA);

    progress.onResponse(hitsResponse(10, 40, TotalHits.Relation.EQUAL_TO));
    assertEquals(0.5D, progress.fractionDone(), DELTA);
  }

  @Test
  void pitPaging_estimateIsCappedByRowLimit() {
    SourceProgress progress = new SourceProgress();
    progress.start(20);
    listenerOf(progress, pitSearch());
    progress.onResponse(hitsResponse(10, 1_000, TotalHits.Relation.EQUAL_TO));
    assertEquals(0.5D, progress.fractionDone(), DELTA);
  }

  @Test
  void pitPaging_lowerBoundTotalGrowsWithUnitsRead() {
    SourceProgress progress = new SourceProgress();
    progress.start(1_000_000);
    listenerOf(progress, pitSearch());
    progress.onResponse(hitsResponse(5, 10, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
    assertEquals(0.5D, progress.fractionDone(), DELTA);
    listenerOf(progress, pitSearch());
    progress.onResponse(hitsResponse(5, 10, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
    // The lower bound is reached, so the estimate is one page beyond what was read.
    assertEquals(10D / 15D, progress.fractionDone(), DELTA);
  }

  @Test
  void compositePaging_readsBucketDocCountsAndIgnoresRowLimit() {
    SourceProgress progress = new SourceProgress();
    progress.start(1);
    listenerOf(progress, compositeSearch());
    progress.onResponse(compositeResponse(100, 30L, 10L));
    assertEquals(0.4D, progress.fractionDone(), DELTA);
  }

  @Test
  void compositeFirstPage_reportsShardsScaledByPageShare() {
    SourceProgress progress = new SourceProgress();
    progress.start(4);
    SourceProgress.ShardListener listener = listenerOf(progress, compositeSearch());
    listener.onListShards(shards(2), List.of(), null, false);
    listener.onQueryResult(0);
    // Composite pages default to 10 buckets, which covers the 4-row limit.
    assertEquals(0.5D, progress.fractionDone(), DELTA);
  }

  @Test
  void firstPageOfUnknownSize_reportsZero() {
    SourceProgress progress = new SourceProgress();
    progress.start(100);
    SourceProgress.ShardListener listener =
        listenerOf(
            progress,
            new SearchRequest()
                .source(
                    new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder("pit"))));
    listener.onListShards(shards(1), List.of(), null, false);
    listener.onQueryResult(0);
    assertEquals(0D, progress.fractionDone(), DELTA);
  }

  @Test
  void start_beginsFreshPassUnlessComplete() {
    SourceProgress progress = new SourceProgress();
    progress.start(100);
    listenerOf(progress, pitSearch());
    progress.onResponse(hitsResponse(10, 40, TotalHits.Relation.EQUAL_TO));
    assertEquals(0.25D, progress.fractionDone(), DELTA);

    progress.start(100);
    assertEquals(0D, progress.fractionDone(), DELTA);

    progress.complete();
    progress.start(100);
    assertEquals(1D, progress.fractionDone(), DELTA);
  }

  @Test
  void track_countsPitTotalUpToRowLimitOnFirstPageOnly() {
    SourceProgress progress = new SourceProgress();
    progress.start(100);
    SearchRequest first = pitSearch();
    SearchRequest trackedFirst = progress.track(first);
    assertEquals(100, trackedFirst.source().trackTotalHitsUpTo().intValue());
    assertNull(first.source().trackTotalHitsUpTo(), "the scan's own builder is not changed");

    progress.onResponse(hitsResponse(10, 40, TotalHits.Relation.EQUAL_TO));
    SearchRequest second = pitSearch();
    assertSame(second.source(), progress.track(second).source());
    assertNull(new SourceProgress().track(plainSearch()).source().trackTotalHitsUpTo());
  }

  @Test
  void track_countsCompositeTotalExactly() {
    SourceProgress progress = new SourceProgress();
    progress.start(100);
    assertEquals(
        Integer.MAX_VALUE,
        progress.track(compositeSearch()).source().trackTotalHitsUpTo().intValue());
  }

  @Test
  void exactTotal_isKeptWhenLaterPagesReportLowerBound() {
    SourceProgress progress = new SourceProgress();
    progress.start(1_000_000);
    listenerOf(progress, pitSearch());
    progress.onResponse(hitsResponse(10, 40, TotalHits.Relation.EQUAL_TO));
    listenerOf(progress, pitSearch());
    progress.onResponse(hitsResponse(10, 10, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
    assertEquals(0.5D, progress.fractionDone(), DELTA);
  }

  @Test
  void pitTotalAtRowLimit_isExactForProgress() {
    SourceProgress progress = new SourceProgress();
    progress.start(20);
    listenerOf(progress, pitSearch());
    progress.onResponse(hitsResponse(10, 20, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
    assertEquals(0.5D, progress.fractionDone(), DELTA);
  }

  @Test
  void track_keepsSearchPipeline() {
    SearchRequest request = plainSearch().pipeline("my_pipeline");
    assertEquals("my_pipeline", new SourceProgress().track(request).pipeline());
  }

  @Test
  void compositePaging_withLowerBoundTotal_readsBucketsOverRowLimit() {
    SourceProgress progress = new SourceProgress();
    progress.start(20);
    listenerOf(progress, compositeSearch());
    progress.onResponse(
        compositeResponse(10_000, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, 30L, 10L));
    // The total is a lower bound, so progress is buckets read over the row limit: 2 of 20.
    assertEquals(0.1D, progress.fractionDone(), DELTA);
  }

  @Test
  void complete_reportsOne() {
    SourceProgress progress = new SourceProgress();
    listenerOf(progress, pitSearch());
    progress.complete();
    assertEquals(1D, progress.fractionDone(), DELTA);
  }

  @Test
  void track_returnsCopyAndLeavesOtherTasksUntouched() {
    SourceProgress progress = new SourceProgress();
    SearchRequest original = plainSearch();
    SearchRequest tracked = progress.track(original);
    assertNotSame(original, tracked);
    assertSame(original.source(), tracked.source());
    SearchTask untracked =
        original.createTask(1L, "transport", "search", TaskId.EMPTY_TASK_ID, null);
    assertNotSame(SourceProgress.ShardListener.class, untracked.getProgressListener().getClass());
  }

  private static SourceProgress.ShardListener listenerOf(
      SourceProgress progress, SearchRequest request) {
    SearchTask task =
        progress
            .track(request)
            .createTask(1L, "transport", "search", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    return (SourceProgress.ShardListener) task.getProgressListener();
  }

  private static List<SearchShard> shards(int count) {
    return Collections.nCopies(count, mock(SearchShard.class));
  }

  private static SearchRequest plainSearch() {
    return new SearchRequest("test").source(new SearchSourceBuilder());
  }

  private static SearchRequest pitSearch() {
    return new SearchRequest()
        .source(
            new SearchSourceBuilder().size(10).pointInTimeBuilder(new PointInTimeBuilder("pit")));
  }

  private static SearchRequest compositeSearch() {
    return new SearchRequest("test")
        .source(
            new SearchSourceBuilder()
                .size(0)
                .aggregation(
                    AggregationBuilders.composite(
                        "composite", List.of(new TermsValuesSourceBuilder("k").field("k")))));
  }

  private static SearchResponse hitsResponse(
      int pageSize, long total, TotalHits.Relation relation) {
    SearchHit[] hits = new SearchHit[pageSize];
    for (int i = 0; i < pageSize; i++) {
      hits[i] = new SearchHit(i);
    }
    SearchResponse response = mock(SearchResponse.class);
    when(response.getHits()).thenReturn(new SearchHits(hits, new TotalHits(total, relation), 1F));
    return response;
  }

  private static SearchResponse compositeResponse(long total, long... docCounts) {
    return compositeResponse(total, TotalHits.Relation.EQUAL_TO, docCounts);
  }

  private static SearchResponse compositeResponse(
      long total, TotalHits.Relation relation, long... docCounts) {
    List<InternalComposite.InternalBucket> buckets =
        java.util.Arrays.stream(docCounts)
            .mapToObj(
                count -> {
                  InternalComposite.InternalBucket bucket =
                      mock(InternalComposite.InternalBucket.class);
                  when(bucket.getDocCount()).thenReturn(count);
                  return bucket;
                })
            .toList();
    InternalComposite composite = mock(InternalComposite.class);
    when(composite.getBuckets()).thenReturn(buckets);
    SearchResponse response = mock(SearchResponse.class);
    when(response.getAggregations()).thenReturn(new Aggregations(List.of(composite)));
    when(response.getHits())
        .thenReturn(new SearchHits(new SearchHit[0], new TotalHits(total, relation), 0F));
    return response;
  }
}
