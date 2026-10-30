/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.identity.PluginSubject;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/** Tests that the calls the SQL stores make on PluginClient run as its subject. */
class PluginClientTest {

  private static final String SUBJECT_TRANSIENT = "subject.marker";

  private ThreadContext threadContext;
  private PluginClient pluginClient;
  private String subjectSeenByDelegate;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    threadContext = new ThreadContext(Settings.EMPTY);

    ThreadPool threadPool = mock(ThreadPool.class);
    when(threadPool.getThreadContext()).thenReturn(threadContext);

    Client delegate = mock(Client.class);
    when(delegate.settings()).thenReturn(Settings.EMPTY);
    when(delegate.threadPool()).thenReturn(threadPool);
    doAnswer(
            invocation -> {
              subjectSeenByDelegate = threadContext.getTransient(SUBJECT_TRANSIENT);
              return null;
            })
        .when(delegate)
        .execute(any(ActionType.class), any(ActionRequest.class), any(ActionListener.class));

    pluginClient = new PluginClient(delegate);
    pluginClient.setSubject(new RecordingSubject(threadContext));
  }

  @Test
  void runsTheConvenienceMethodsTheStoresUseInsideTheSubject() {
    // StateStore and the datasource storage call these rather than execute, and admin() builds
    // its own client around this one, so each has to end up in doExecute.
    pluginClient.index(new IndexRequest("index"));
    assertEquals("set-by-subject", subjectSeenByDelegate, "index() must run inside runAs");

    subjectSeenByDelegate = null;
    pluginClient.admin().indices().create(new CreateIndexRequest("index"));
    assertEquals("set-by-subject", subjectSeenByDelegate, "admin() must run inside runAs");
  }

  /** Stands in for the plugin subject, stashing around its body and marking the context. */
  private static class RecordingSubject implements PluginSubject {

    private final ThreadContext threadContext;

    RecordingSubject(ThreadContext threadContext) {
      this.threadContext = threadContext;
    }

    @Override
    public Principal getPrincipal() {
      return () -> "plugin:test";
    }

    @Override
    public <E extends Exception> void runAs(CheckedRunnable<E> runnable) throws E {
      try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
        threadContext.putTransient(SUBJECT_TRANSIENT, "set-by-subject");
        runnable.run();
      }
    }
  }
}
