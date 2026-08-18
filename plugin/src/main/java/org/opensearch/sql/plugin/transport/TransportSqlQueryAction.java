/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Establishes a coordinator {@link SqlQueryTask} for the duration of a SQL query. The transport
 * framework registers the task (via {@link TransportSqlQueryRequest#createTask}) before {@link
 * #doExecute} runs and unregisters it when the response listener completes. We bind the task to the
 * executing thread's {@code OpenSearchQueryManager} ThreadLocal so the existing {@code
 * applyParentTask} logic in {@code OpenSearchNodeClient} stamps it as the parent on every DSL
 * search the SQL engine issues. The listener is completed once the wrapped REST channel emits its
 * response, keeping the task alive across the (possibly asynchronous) execution.
 */
public class TransportSqlQueryAction
    extends HandledTransportAction<TransportSqlQueryRequest, TransportSqlQueryResponse> {

  @Inject
  public TransportSqlQueryAction(TransportService transportService, ActionFilters actionFilters) {
    super(SqlQueryAction.NAME, transportService, actionFilters, TransportSqlQueryRequest::new);
  }

  @Override
  protected void doExecute(
      Task task,
      TransportSqlQueryRequest request,
      ActionListener<TransportSqlQueryResponse> listener) {
    if (task instanceof CancellableTask cancellableTask) {
      OpenSearchQueryManager.setCancellableTask(cancellableTask);
    }

    AtomicBoolean completed = new AtomicBoolean(false);
    RestChannel channel = new CompletionSignalingChannel(request.getChannel(), listener, completed);
    try {
      request.getWork().accept(channel);
    } catch (Exception e) {
      // Only surface here if the channel hasn't already reported completion (success or error).
      if (completed.compareAndSet(false, true)) {
        listener.onFailure(e);
      }
    }
  }

  /**
   * Delegates all channel behavior to the real channel, and completes the transport listener the
   * first time a response is sent — which is what triggers unregistration of the coordinator task.
   */
  private static final class CompletionSignalingChannel implements RestChannel {
    private final RestChannel delegate;
    private final ActionListener<TransportSqlQueryResponse> listener;
    private final AtomicBoolean completed;

    CompletionSignalingChannel(
        RestChannel delegate,
        ActionListener<TransportSqlQueryResponse> listener,
        AtomicBoolean completed) {
      this.delegate = delegate;
      this.listener = listener;
      this.completed = completed;
    }

    @Override
    public void sendResponse(RestResponse response) {
      try {
        delegate.sendResponse(response);
      } finally {
        if (completed.compareAndSet(false, true)) {
          listener.onResponse(new TransportSqlQueryResponse());
        }
      }
    }

    @Override
    public XContentBuilder newBuilder() throws IOException {
      return delegate.newBuilder();
    }

    @Override
    public XContentBuilder newErrorBuilder() throws IOException {
      return delegate.newErrorBuilder();
    }

    @Override
    public XContentBuilder newBuilder(MediaType mediaType, boolean useFiltering)
        throws IOException {
      return delegate.newBuilder(mediaType, useFiltering);
    }

    @Override
    public XContentBuilder newBuilder(
        MediaType mediaType, MediaType responseContentType, boolean useFiltering)
        throws IOException {
      return delegate.newBuilder(mediaType, responseContentType, useFiltering);
    }

    @Override
    public BytesStreamOutput bytesOutput() {
      return delegate.bytesOutput();
    }

    @Override
    public RestRequest request() {
      return delegate.request();
    }

    @Override
    public boolean detailedErrorsEnabled() {
      return delegate.detailedErrorsEnabled();
    }

    @Override
    public boolean detailedErrorStackTraceEnabled() {
      return delegate.detailedErrorStackTraceEnabled();
    }
  }
}
