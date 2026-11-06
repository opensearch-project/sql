/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteAction;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateAction;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.identity.PluginSubject;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.opensearch.client.PluginClient;
import org.opensearch.sql.spark.execution.session.SessionModel;
import org.opensearch.sql.spark.execution.session.SessionState;
import org.opensearch.sql.spark.execution.xcontent.SessionModelXContentSerializer;
import org.opensearch.sql.spark.execution.xcontent.XContentSerializerUtil;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.support.AbstractClient;

/** Tests that StateStore sends each request through its plugin client and maps the reply. */
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class StateStoreTest {

  private static final String INDEX = ".query_execution_request_mys3";
  private static final String SESSION_ID = "session-1";
  private static final String SUBJECT_TRANSIENT = "test.plugin.subject";
  private static final String SUBJECT = "plugin:sql";
  private static final String CALLER_TRANSIENT = "test.caller";
  private static final ShardId SHARD = new ShardId(INDEX, "uuid", 0);

  private ThreadContext threadContext;
  private RecordingClient delegate;
  private ClusterService clusterService;
  private StateStore stateStore;

  @BeforeEach
  void setUp() {
    threadContext = new ThreadContext(Settings.EMPTY);
    ThreadPool threadPool = mock(ThreadPool.class);
    when(threadPool.getThreadContext()).thenReturn(threadContext);
    delegate = new RecordingClient(threadPool);

    PluginClient pluginClient = new PluginClient(delegate);
    pluginClient.setSubject(new MarkingSubject(threadContext));

    clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
    indexExists(true);
    stateStore = new StateStore(pluginClient, clusterService);
  }

  @AfterEach
  void everyActionRanAsThePluginSubjectWithAValidRequest() {
    // Checked here rather than inside the client, so no catch in StateStore can hide a failure.
    for (RecordingClient.Run run : delegate.runs) {
      assertEquals(SUBJECT, run.subject(), run.action().name() + " must run as the plugin subject");
      assertNull(run.caller(), run.action().name() + " must not see the caller's context");
      assertNull(run.validation(), run.action().name() + " request must be valid");
    }
  }

  @Test
  void createIndexesTheDocumentAsThePluginSubject() {
    delegate.reply(IndexAction.INSTANCE, new IndexResponse(SHARD, SESSION_ID, 5, 2, 1, true));

    SessionModel created = stateStore.create(SESSION_ID, session(), SessionModel::of, INDEX);

    IndexRequest request = delegate.onlyRequest(IndexAction.INSTANCE);
    assertEquals(INDEX, request.index());
    assertEquals(SESSION_ID, request.id());
    assertEquals(DocWriteRequest.OpType.CREATE, request.opType());
    assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, request.ifSeqNo());
    assertEquals(SequenceNumbers.UNASSIGNED_PRIMARY_TERM, request.ifPrimaryTerm());
    assertEquals(WriteRequest.RefreshPolicy.WAIT_UNTIL, request.getRefreshPolicy());
    assertEquals(SESSION_ID, request.sourceAsMap().get(SessionModelXContentSerializer.SESSION_ID));
    assertEquals(XContentSerializerUtil.buildMetadata(5, 2), created.getMetadata());
    assertEquals(SESSION_ID, created.getSessionId());
  }

  @Test
  void createLeavesTheCallersContextInPlace() {
    delegate.reply(IndexAction.INSTANCE, new IndexResponse(SHARD, SESSION_ID, 5, 2, 1, true));
    threadContext.putTransient(CALLER_TRANSIENT, "user");

    stateStore.create(SESSION_ID, session(), SessionModel::of, INDEX);

    assertEquals("user", threadContext.getTransient(CALLER_TRANSIENT));
    assertNull(threadContext.getTransient(SUBJECT_TRANSIENT));
  }

  @Test
  void createCreatesAMissingIndexAsThePluginSubjectFirst() {
    indexExists(false);
    delegate.reply(CreateIndexAction.INSTANCE, new CreateIndexResponse(true, true, INDEX));
    delegate.reply(IndexAction.INSTANCE, new IndexResponse(SHARD, SESSION_ID, 0, 1, 1, true));

    stateStore.create(SESSION_ID, session(), SessionModel::of, INDEX);

    assertEquals(
        List.of(CreateIndexAction.INSTANCE, IndexAction.INSTANCE), delegate.actionsRun());
    CreateIndexRequest request = delegate.onlyRequest(CreateIndexAction.INSTANCE);
    assertEquals(INDEX, request.index());
    assertEquals("1", request.settings().get("index.number_of_shards"));
    assertEquals("0-2", request.settings().get("index.auto_expand_replicas"));
    assertTrue(request.mappings().contains("sessionId"), request.mappings());
  }

  @Test
  void createFailsWhenTheIndexIsNotAcknowledged() {
    indexExists(false);
    delegate.reply(CreateIndexAction.INSTANCE, new CreateIndexResponse(false, false, INDEX));

    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () -> stateStore.create(SESSION_ID, session(), SessionModel::of, INDEX));

    assertTrue(e.getMessage().contains("Index creation is not acknowledged."), e.getMessage());
    assertEquals(List.of(CreateIndexAction.INSTANCE), delegate.actionsRun());
  }

  @Test
  void createReportsAFailedIndexCreation() {
    indexExists(false);
    delegate.fail(CreateIndexAction.INSTANCE, new IllegalStateException("no permissions"));

    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () -> stateStore.create(SESSION_ID, session(), SessionModel::of, INDEX));

    assertTrue(e.getMessage().contains("no permissions"), e.getMessage());
    assertEquals(List.of(CreateIndexAction.INSTANCE), delegate.actionsRun());
  }

  @Test
  void createFailsWhenTheDocumentWasNotCreated() {
    delegate.reply(IndexAction.INSTANCE, new IndexResponse(SHARD, SESSION_ID, 5, 2, 2, false));

    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () -> stateStore.create(SESSION_ID, session(), SessionModel::of, INDEX));

    assertEquals("Failed create doc. id: session-1, error: updated", e.getMessage());
  }

  @Test
  void createRethrowsAVersionConflictUnwrapped() {
    // OpenSearchSessionStorageService catches this type to report a session that already exists.
    VersionConflictEngineException conflict =
        new VersionConflictEngineException(SHARD, SESSION_ID, "document already exists");
    delegate.fail(IndexAction.INSTANCE, conflict);

    assertSame(
        conflict,
        assertThrows(
            VersionConflictEngineException.class,
            () -> stateStore.create(SESSION_ID, session(), SessionModel::of, INDEX)));
  }

  @Test
  void getReadsTheDocumentAsThePluginSubject() throws IOException {
    delegate.reply(GetAction.INSTANCE, getResponse(true, 7, 3));

    Optional<SessionModel> found =
        stateStore.get(SESSION_ID, new SessionModelXContentSerializer()::fromXContent, INDEX);

    GetRequest request = delegate.onlyRequest(GetAction.INSTANCE);
    assertEquals(INDEX, request.index());
    assertEquals(SESSION_ID, request.id());
    assertTrue(request.refresh());
    SessionModel model = found.orElseThrow();
    assertEquals(SESSION_ID, model.getSessionId());
    assertEquals(SessionState.NOT_STARTED, model.getSessionState());
    assertEquals(XContentSerializerUtil.buildMetadata(7, 3), model.getMetadata());
  }

  @Test
  void getReturnsEmptyWhenTheDocumentIsMissing() throws IOException {
    delegate.reply(GetAction.INSTANCE, getResponse(
                false, SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM));

    assertFalse(
        stateStore
            .get(SESSION_ID, new SessionModelXContentSerializer()::fromXContent, INDEX)
            .isPresent());
    assertEquals(List.of(GetAction.INSTANCE), delegate.actionsRun());
  }

  @Test
  void getCreatesAMissingIndexAndReturnsEmptyWithoutReading() {
    indexExists(false);
    delegate.reply(CreateIndexAction.INSTANCE, new CreateIndexResponse(true, true, INDEX));

    assertFalse(
        stateStore
            .get(SESSION_ID, new SessionModelXContentSerializer()::fromXContent, INDEX)
            .isPresent());
    assertEquals(List.of(CreateIndexAction.INSTANCE), delegate.actionsRun());
  }

  @Test
  void getRethrowsATransportFailure() {
    IllegalStateException denied = new IllegalStateException("no permissions");
    delegate.fail(GetAction.INSTANCE, denied);

    assertSame(
        denied,
        assertThrows(
            IllegalStateException.class,
            () ->
                stateStore.get(
                    SESSION_ID, new SessionModelXContentSerializer()::fromXContent, INDEX)));
  }

  @Test
  void updateStateWritesTheNewStateAsThePluginSubject() {
    delegate.reply(
        UpdateAction.INSTANCE,
        new UpdateResponse(SHARD, SESSION_ID, 9, 4, 2, DocWriteResponse.Result.UPDATED));
    SessionModel stored =
        SessionModel.of(session(), XContentSerializerUtil.buildMetadata(8, 4));

    SessionModel updated =
        stateStore.updateState(
            stored, SessionState.RUNNING, SessionModel::copyWithState, INDEX);

    UpdateRequest request = delegate.onlyRequest(UpdateAction.INSTANCE);
    assertEquals(INDEX, request.index());
    assertEquals(SESSION_ID, request.id());
    assertEquals(8, request.ifSeqNo());
    assertEquals(4, request.ifPrimaryTerm());
    assertEquals(WriteRequest.RefreshPolicy.WAIT_UNTIL, request.getRefreshPolicy());
    assertTrue(request.fetchSource().fetchSource());
    assertEquals("running", request.doc().sourceAsMap().get("state"));
    assertEquals(SessionState.RUNNING, updated.getSessionState());
    assertEquals(XContentSerializerUtil.buildMetadata(9, 4), updated.getMetadata());
  }

  @Test
  void updateStateRethrowsAVersionConflictUnwrapped() {
    VersionConflictEngineException conflict =
        new VersionConflictEngineException(SHARD, SESSION_ID, "seqNo changed");
    delegate.fail(UpdateAction.INSTANCE, conflict);

    assertSame(
        conflict,
        assertThrows(
            VersionConflictEngineException.class,
            () ->
                stateStore.updateState(
                    session(), SessionState.RUNNING, SessionModel::copyWithState, INDEX)));
  }

  @Test
  void deleteRemovesTheDocumentAsThePluginSubject() {
    delegate.reply(DeleteAction.INSTANCE, new DeleteResponse(SHARD, SESSION_ID, 3, 1, 2, true));

    assertTrue(stateStore.delete(SESSION_ID, INDEX));

    DeleteRequest request = delegate.onlyRequest(DeleteAction.INSTANCE);
    assertEquals(INDEX, request.index());
    assertEquals(SESSION_ID, request.id());
  }

  @Test
  void deleteReturnsFalseWhenTheDocumentWasNotFound() {
    delegate.reply(DeleteAction.INSTANCE, new DeleteResponse(SHARD, SESSION_ID, 3, 1, 2, false));

    assertFalse(stateStore.delete(SESSION_ID, INDEX));
    assertEquals(List.of(DeleteAction.INSTANCE), delegate.actionsRun());
  }

  @Test
  void deleteDoesNothingWhenTheIndexIsMissing() {
    indexExists(false);

    assertTrue(stateStore.delete(SESSION_ID, INDEX));
    assertEquals(List.of(), delegate.actionsRun());
  }

  @Test
  void deleteWrapsATransportFailure() {
    IllegalStateException denied = new IllegalStateException("no permissions");
    delegate.fail(DeleteAction.INSTANCE, denied);

    RuntimeException e =
        assertThrows(RuntimeException.class, () -> stateStore.delete(SESSION_ID, INDEX));

    assertSame(denied, e.getCause());
    assertEquals(
        "Failed to delete index state doc session-1 in index " + INDEX, e.getMessage());
  }

  @Test
  void countSearchesAsThePluginSubject() {
    delegate.reply(SearchAction.INSTANCE, searchResponse(RestStatus.OK, 4));
    QueryBuilder query = QueryBuilders.termQuery("state", "running");

    assertEquals(4L, stateStore.count(INDEX, query));

    SearchRequest request = delegate.onlyRequest(SearchAction.INSTANCE);
    assertEquals(List.of(INDEX), List.of(request.indices()));
    assertEquals("_primary_first", request.preference());
    assertEquals(0, request.source().size());
    assertSame(query, request.source().query());
  }

  @Test
  void countFailsOnAStatusOtherThanOk() {
    delegate.reply(SearchAction.INSTANCE, searchResponse(RestStatus.PARTIAL_CONTENT, 4));

    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () -> stateStore.count(INDEX, QueryBuilders.matchAllQuery()));

    assertEquals(
        "Fetching job metadata information failed with status : PARTIAL_CONTENT",
        e.getMessage());
  }

  @Test
  void countRethrowsATransportFailure() {
    IllegalStateException denied = new IllegalStateException("no permissions");
    delegate.fail(SearchAction.INSTANCE, denied);

    assertSame(
        denied,
        assertThrows(
            IllegalStateException.class,
            () -> stateStore.count(INDEX, QueryBuilders.matchAllQuery())));
  }

  private void indexExists(boolean exists) {
    when(clusterService.state().routingTable().hasIndex(INDEX)).thenReturn(exists);
  }

  private static SessionModel session() {
    return SessionModel.initInteractiveSession("account", "app", "job", SESSION_ID, "mys3");
  }

  private static GetResponse getResponse(boolean exists, long seqNo, long primaryTerm)
      throws IOException {
    BytesReference source =
        exists
            ? BytesReference.bytes(
                new SessionModelXContentSerializer().toXContent(session(), ToXContent.EMPTY_PARAMS))
            : null;
    return new GetResponse(
        new GetResult(
            INDEX, SESSION_ID, seqNo, primaryTerm, 1, exists, source, Map.of(), Map.of()));
  }

  private static SearchResponse searchResponse(RestStatus status, long totalHits) {
    SearchResponse response = mock(SearchResponse.class);
    when(response.status()).thenReturn(status);
    when(response.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[0], new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), 0f));
    return response;
  }

  /**
   * Stands in for the node client: it records each action with the context it ran in, and answers
   * it with the reply set for its action type. A call made on it directly rather than through the
   * plugin client is recorded with no subject.
   */
  private static final class RecordingClient extends AbstractClient {

    /** One action as the client received it. */
    record Run(
        ActionType<?> action,
        ActionRequest request,
        Object subject,
        Object caller,
        Exception validation) {}

    private final Map<ActionType<?>, Object> replies = new HashMap<>();
    private final List<Run> runs = new ArrayList<>();

    RecordingClient(ThreadPool threadPool) {
      super(Settings.EMPTY, threadPool);
    }

    <R extends ActionResponse> void reply(ActionType<R> action, R response) {
      replies.put(action, response);
    }

    void fail(ActionType<?> action, Exception failure) {
      replies.put(action, failure);
    }

    List<ActionType<?>> actionsRun() {
      return runs.stream().<ActionType<?>>map(Run::action).toList();
    }

    @SuppressWarnings("unchecked")
    <R extends ActionRequest> R onlyRequest(ActionType<?> action) {
      List<Run> matching = runs.stream().filter(run -> run.action().equals(action)).toList();
      assertEquals(1, matching.size(), "runs of " + action.name());
      return (R) matching.get(0).request();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action, Request request, ActionListener<Response> listener) {
      ThreadContext context = threadPool().getThreadContext();
      runs.add(
          new Run(
              action,
              request,
              context.getTransient(SUBJECT_TRANSIENT),
              context.getTransient(CALLER_TRANSIENT),
              request.validate()));
      Object reply = replies.get(action);
      if (reply instanceof Exception failure) {
        listener.onFailure(failure);
      } else {
        assertInstanceOf(ActionResponse.class, reply, "no reply set for " + action.name());
        listener.onResponse((Response) reply);
      }
    }

    @Override
    public void close() {}
  }

  /** Stands in for the plugin subject: it stashes the caller's context and marks its own. */
  private static final class MarkingSubject implements PluginSubject {

    private final ThreadContext threadContext;

    MarkingSubject(ThreadContext threadContext) {
      this.threadContext = threadContext;
    }

    @Override
    public Principal getPrincipal() {
      return () -> SUBJECT;
    }

    @Override
    public <E extends Exception> void runAs(CheckedRunnable<E> runnable) throws E {
      try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
        threadContext.putTransient(SUBJECT_TRANSIENT, SUBJECT);
        runnable.run();
      }
    }
  }
}
