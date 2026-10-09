/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.index.Index;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.exception.NonFallbackCalciteException;

/**
 * Verifies {@link SyncErrorReportRenderer#statusCodeFor(Throwable)} takes the status of an {@link
 * org.opensearch.OpenSearchException} buried in the cause chain, so a shard's 400 is not reported
 * as a 500 once the Calcite path has wrapped it.
 */
public class SyncErrorReportRendererTest {

  @Test
  public void shardFailureUnderCalciteWrappersReturns400() {
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
    ErrorReport report = ErrorReport.wrap(calciteWrapped(shardFailure)).build();

    assertEquals(400, SyncErrorReportRenderer.statusCodeFor(report));
  }

  @Test
  public void coordinatorFailureWithoutShardFailuresReturns500() {
    // A search exception with no shard failures takes its status from its cause.
    SearchPhaseExecutionException coordinatorFailure =
        new SearchPhaseExecutionException(
            "fetch", "", new ClassCastException("BytesRef to Long"), new ShardSearchFailure[0]);

    assertEquals(500, SyncErrorReportRenderer.statusCodeFor(calciteWrapped(coordinatorFailure)));
  }

  @Test
  public void clientErrorAtTopReturns400OverShardServerError() {
    // NonFallbackCalciteException is a QueryEngineException, and the client-error check runs first.
    SearchPhaseExecutionException shardServerError =
        new SearchPhaseExecutionException(
            "query",
            "all shards failed",
            new ShardSearchFailure[] {new ShardSearchFailure(new RuntimeException("shard fault"))});

    assertEquals(
        400,
        SyncErrorReportRenderer.statusCodeFor(
            new NonFallbackCalciteException("Failed to fetch data", shardServerError)));
  }

  /**
   * The wrappers the Calcite execution path puts around a search that failed in
   * BackgroundSearchScanner.
   */
  private static RuntimeException calciteWrapped(SearchPhaseExecutionException searchFailure) {
    return new RuntimeException(
        new SQLException(
            "exception while executing query",
            new NonFallbackCalciteException(
                "Failed to fetch data from the index", new ExecutionException(searchFailure))));
  }
}
