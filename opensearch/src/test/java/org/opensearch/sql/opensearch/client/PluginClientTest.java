/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.Subject;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

class PluginClientTest {

  private static final String CALLER_HEADER = "caller";
  private static final String SUBJECT_HEADER = "subject";

  private ThreadPool threadPool;
  private Client delegate;
  private PluginClient pluginClient;

  @BeforeEach
  void setUp() {
    threadPool = new TestThreadPool(PluginClientTest.class.getName());
    delegate = mock(Client.class);
    when(delegate.settings()).thenReturn(Settings.EMPTY);
    when(delegate.threadPool()).thenReturn(threadPool);
    pluginClient = new PluginClient(delegate);
  }

  @AfterEach
  void tearDown() {
    ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
  }

  @Test
  void throwsWhenNoSubjectHasBeenAssigned() {
    assertThrows(
        IllegalStateException.class,
        () ->
            pluginClient.execute(
                SearchAction.INSTANCE, new SearchRequest(), ActionListener.wrap(r -> {}, e -> {})));
  }

  @Test
  void runsTheActionAsTheAssignedSubject() {
    pluginClient.setSubject(subject("sql"));
    AtomicReference<String> seenBySubject = new AtomicReference<>();
    AtomicReference<String> seenByCaller = new AtomicReference<>();
    captureListener(
        () -> {
          seenBySubject.set(threadPool.getThreadContext().getHeader(SUBJECT_HEADER));
          seenByCaller.set(threadPool.getThreadContext().getHeader(CALLER_HEADER));
        });

    threadPool.getThreadContext().putHeader(CALLER_HEADER, "alice");
    pluginClient.execute(
        SearchAction.INSTANCE, new SearchRequest(), ActionListener.wrap(r -> {}, e -> {}));

    assertEquals("sql", seenBySubject.get());
    // runAs swaps the caller out, so the action does not run with the caller's headers.
    assertNull(seenByCaller.get());
    // and the calling thread has them back once doExecute returns.
    assertEquals("alice", threadPool.getThreadContext().getHeader(CALLER_HEADER));
  }

  @Test
  void restoresTheCallersContextBeforeTheListenerRuns() throws Exception {
    pluginClient.setSubject(subject("sql"));
    AtomicReference<ActionListener<SearchResponse>> transportListener = new AtomicReference<>();
    captureListener(transportListener);

    AtomicReference<String> seenByListener = new AtomicReference<>();
    threadPool.getThreadContext().putHeader(CALLER_HEADER, "alice");
    pluginClient.execute(
        SearchAction.INSTANCE,
        new SearchRequest(),
        ActionListener.wrap(
            response -> seenByListener.set(threadPool.getThreadContext().getHeader(CALLER_HEADER)),
            e -> {}));
    assertNotNull(transportListener.get());

    // A transport response arrives on a pooled thread that never carried the caller's context, so
    // the listener only sees it if doExecute wired the restore to the listener.
    Thread responseThread = new Thread(() -> transportListener.get().onResponse(null));
    responseThread.start();
    responseThread.join();

    assertEquals("alice", seenByListener.get());
  }

  @Test
  void reportsASynchronousFailureThroughTheListener() {
    RuntimeException failure = new RuntimeException("no subject privileges");
    pluginClient.setSubject(
        new Subject() {
          @Override
          public Principal getPrincipal() {
            return new NamedPrincipal("sql");
          }

          @Override
          public <E extends Exception> void runAs(CheckedRunnable<E> runnable) {
            throw failure;
          }
        });

    AtomicReference<Exception> reported = new AtomicReference<>();
    threadPool.getThreadContext().putHeader(CALLER_HEADER, "alice");
    pluginClient.execute(
        SearchAction.INSTANCE,
        new SearchRequest(),
        ActionListener.wrap(response -> {}, reported::set));

    assertEquals(failure, reported.get());
    assertEquals("alice", threadPool.getThreadContext().getHeader(CALLER_HEADER));
  }

  /** Captures the listener the plugin client hands to the delegate, without completing it. */
  @SuppressWarnings("unchecked")
  private void captureListener(AtomicReference<ActionListener<SearchResponse>> captured) {
    doAnswer(
            invocation -> {
              captured.set(invocation.getArgument(2));
              return null;
            })
        .when(delegate)
        .execute(any(), any(), any());
  }

  /** Runs the given probe on the thread and context the delegate is called with. */
  private void captureListener(Runnable probe) {
    doAnswer(
            invocation -> {
              probe.run();
              return null;
            })
        .when(delegate)
        .execute(any(), any(), any());
  }

  /**
   * Mirrors what {@code NoopPluginSubject} and the security plugin's {@code SecurePluginSubject}
   * do: stash the caller's context, run as the plugin, and restore on the way out. A pass-through
   * fake would pass against variants that are broken in production.
   */
  private Subject subject(String name) {
    return new Subject() {
      @Override
      public Principal getPrincipal() {
        return new NamedPrincipal(name);
      }

      @Override
      public <E extends Exception> void runAs(CheckedRunnable<E> runnable) throws E {
        ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
          threadContext.putHeader(SUBJECT_HEADER, name);
          runnable.run();
        }
      }
    };
  }
}
