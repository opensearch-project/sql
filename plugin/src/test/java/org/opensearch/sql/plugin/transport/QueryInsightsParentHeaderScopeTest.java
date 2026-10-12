/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.opensearch.action.support.ContextPreservingActionListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;

/**
 * Pins the thread-context contract {@code TransportPPLQueryAction} relies on when it stamps the
 * Query Insights parent header: the header must reach the searches scheduled during execution, but
 * must not outlive the call on the calling thread or reappear in the caller's callback. Without
 * that, a caller issuing several PPL queries through {@code NodeClient} would tag later queries (or
 * a plain DSL search) with the first query's marker.
 */
public class QueryInsightsParentHeaderScopeTest {

  private static final String MARKER = "PPL:node-1:42";

  @Test
  public void headerReachesScheduledWorkButDoesNotLeak() {
    ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    AtomicReference<String> seenByScheduledWork = new AtomicReference<>("unset");
    AtomicReference<String> seenByCallback = new AtomicReference<>("unset");

    ActionListener<String> caller =
        ActionListener.wrap(
            response ->
                seenByCallback.set(threadContext.getHeader(QueryInsightsMarker.PARENT_HEADER)),
            e -> {});
    // Captured before stamping, mirroring doExecute.
    ActionListener<String> callerListener =
        ContextPreservingActionListener.wrapPreservingContext(caller, threadContext);

    Runnable scheduledWork;
    try (ThreadContext.StoredContext ignored = threadContext.newStoredContext(true)) {
      threadContext.putHeader(QueryInsightsMarker.PARENT_HEADER, MARKER);
      // Executors capture the thread context when work is scheduled; this is what carries the
      // header to the sql-worker and background-scan threads.
      scheduledWork =
          threadContext.preserveContext(
              () ->
                  seenByScheduledWork.set(
                      threadContext.getHeader(QueryInsightsMarker.PARENT_HEADER)));
    }

    assertNull(
        "header must not outlive the call on the calling thread",
        threadContext.getHeader(QueryInsightsMarker.PARENT_HEADER));

    scheduledWork.run();
    assertEquals(
        "searches scheduled during execution must still see the header",
        MARKER,
        seenByScheduledWork.get());

    callerListener.onResponse("ok");
    assertNull("the caller's callback must not inherit the header", seenByCallback.get());
  }

  @Test
  public void existingCallerHeaderIsPreservedAndRestored() {
    ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    threadContext.putHeader(QueryInsightsMarker.PARENT_HEADER, "caller-supplied");

    try (ThreadContext.StoredContext ignored = threadContext.newStoredContext(true)) {
      // doExecute only stamps when absent, so a caller that already carries a marker keeps it.
      if (threadContext.getHeader(QueryInsightsMarker.PARENT_HEADER) == null) {
        threadContext.putHeader(QueryInsightsMarker.PARENT_HEADER, MARKER);
      }
      assertEquals("caller-supplied", threadContext.getHeader(QueryInsightsMarker.PARENT_HEADER));
    }

    assertEquals(
        "the caller's own header must survive the scoped stamp",
        "caller-supplied",
        threadContext.getHeader(QueryInsightsMarker.PARENT_HEADER));
  }
}
