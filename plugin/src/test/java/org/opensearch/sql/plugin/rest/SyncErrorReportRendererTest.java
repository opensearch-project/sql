/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.exception.NonFallbackCalciteException;

/**
 * Verifies {@link SyncErrorReportRenderer#statusCodeFor(Throwable)} takes the status of an {@link
 * org.opensearch.OpenSearchException} under the wrappers a failed search arrives in, so a shard's
 * 400 is not reported as a 500, and that any other wrapper hides it.
 */
public class SyncErrorReportRendererTest {

  @Test
  public void shardFailureInsideExecuteQueryReturns400() {
    SearchPhaseExecutionException shardFailure =
        new SearchPhaseExecutionException(
            "query",
            "all shards failed",
            new ShardSearchFailure[] {
              new ShardSearchFailure(
                  new QueryShardException(
                      new Index("logs", "_na_"),
                      "No mapping found for [f] in order to sort on",
                      null))
            });
    ErrorReport report = ErrorReport.wrap(wrappedInsideExecuteQuery(shardFailure)).build();

    assertEquals(400, SyncErrorReportRenderer.statusCodeFor(report));
  }

  @Test
  public void coordinatorFailureWithoutShardFailuresReturns500() {
    // A search exception with no shard failures takes its status from its cause.
    SearchPhaseExecutionException coordinatorFailure =
        new SearchPhaseExecutionException(
            "fetch", "", new ClassCastException("BytesRef to Long"), new ShardSearchFailure[0]);

    assertEquals(
        500, SyncErrorReportRenderer.statusCodeFor(wrappedInsideExecuteQuery(coordinatorFailure)));
  }

  @Test
  public void shardServerErrorReturns500OverFirstShard400() {
    // SearchPhaseExecutionException#status takes the first shard's status unless a later one is
    // 5xx.
    SearchPhaseExecutionException mixedFailure =
        new SearchPhaseExecutionException(
            "query",
            "all shards failed",
            new ShardSearchFailure[] {
              new ShardSearchFailure(
                  new QueryShardException(
                      new Index("logs", "_na_"),
                      "No mapping found for [f] in order to sort on",
                      null)),
              new ShardSearchFailure(new RuntimeException("shard fault"))
            });

    assertEquals(
        500, SyncErrorReportRenderer.statusCodeFor(wrappedInsideExecuteQuery(mixedFailure)));
  }

  @Test
  public void shardFailureInsideResultSetNextReturns503OverClientError400() {
    // NonFallbackCalciteException, a client-error type, is on top of this cause chain.
    SearchPhaseExecutionException noShardAvailableFailure =
        new SearchPhaseExecutionException(
            "query",
            "all shards failed",
            new ShardSearchFailure[] {
              new ShardSearchFailure(
                  new NoShardAvailableActionException(new ShardId(new Index("logs", "_na_"), 0)))
            });

    assertEquals(
        503,
        SyncErrorReportRenderer.statusCodeFor(wrappedInsideResultSetNext(noShardAvailableFailure)));
  }

  @Test
  public void otherWrapperReturns500OverMetadataBlock403() {
    // OpenSearchNodeClient.getIndexMappings wraps a failed mapping fetch this way.
    ClusterBlockException metadataBlock =
        new ClusterBlockException(Map.of("logs", Set.of(IndexMetadata.INDEX_METADATA_BLOCK)));

    assertEquals(
        500,
        SyncErrorReportRenderer.statusCodeFor(
            new IllegalStateException("Failed to read mapping", metadataBlock)));
  }

  @Test
  public void clientErrorWrapperReturns400OverShard500() {
    // A client-error type outside SEARCH_FAILURE_WRAPPERS keeps its 400 over anything it wraps.
    SearchPhaseExecutionException shardServerError =
        new SearchPhaseExecutionException(
            "query",
            "all shards failed",
            new ShardSearchFailure[] {new ShardSearchFailure(new RuntimeException("shard fault"))});

    assertEquals(
        400,
        SyncErrorReportRenderer.statusCodeFor(
            new IllegalArgumentException("Bad argument", shardServerError)));
  }

  @Test
  public void clientErrorWithoutCauseReturns400() {
    // A client-error type with no cause keeps its 400.
    assertEquals(
        400,
        SyncErrorReportRenderer.statusCodeFor(
            new NonFallbackCalciteException("Insufficient resources to start query")));
  }

  @Test
  public void clientErrorOverNonOpenSearchCauseReturns400() {
    // QueryService throws this when checked arithmetic overflows.
    assertEquals(
        400,
        SyncErrorReportRenderer.statusCodeFor(
            new NonFallbackCalciteException(
                "Arithmetic overflow: long overflow", new ArithmeticException("long overflow"))));
  }

  @Test
  public void cyclicCauseChainReturns500() {
    RuntimeException first = new RuntimeException("first");
    first.initCause(new RuntimeException("second", first));

    assertEquals(500, SyncErrorReportRenderer.statusCodeFor(first));
  }

  /**
   * The wrappers around a search that failed in BackgroundSearchScanner while the fetch ran inside
   * ResultSet.next(), which Avatica does not wrap.
   */
  private static NonFallbackCalciteException wrappedInsideResultSetNext(
      SearchPhaseExecutionException searchFailure) {
    return new NonFallbackCalciteException(
        "Failed to fetch data from the index", new ExecutionException(searchFailure));
  }

  /**
   * The wrappers around a search that failed in BackgroundSearchScanner while the fetch ran inside
   * executeQuery, which are the ResultSet.next() wrappers plus the SQLException from Avatica and
   * the RuntimeException from OpenSearchExecutionEngine.execute.
   */
  private static RuntimeException wrappedInsideExecuteQuery(
      SearchPhaseExecutionException searchFailure) {
    return new RuntimeException(
        new SQLException(
            "exception while executing query", wrappedInsideResultSetNext(searchFailure)));
  }
}
