/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponseNodeV2;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.job.Principal;
import org.opensearch.sql.job.QueryFailure;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.job.QueryJobService;
import org.opensearch.sql.job.QueryJobState;
import org.opensearch.sql.job.QueryJobStatus;
import org.opensearch.sql.job.QueryResult;
import org.opensearch.sql.job.SecurityAdapter;
import org.opensearch.sql.job.exceptions.QueryJobForbiddenException;
import org.opensearch.sql.job.exceptions.QueryJobNotFoundException;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigSupplier;
import org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher;

@ExtendWith(MockitoExtension.class)
class AsyncQueryExecutorServiceRoutingTest {

  private static final QueryJobId JOB_ID = new QueryJobId("owner-node", "query-context");
  private static final Principal ALICE = new Principal("alice", "tenant", List.of("reader"));

  @Mock private AsyncQueryJobMetadataStorageService metadataStorage;
  @Mock private SparkQueryDispatcher sparkDispatcher;
  @Mock private SparkExecutionEngineConfigSupplier sparkConfig;
  @Mock private QueryJobService jobService;
  @Mock private SecurityAdapter securityAdapter;
  @Mock private AsyncQueryRequestContext requestContext;

  private AsyncQueryExecutorServiceImpl service;

  @BeforeEach
  void setUp() {
    service =
        new AsyncQueryExecutorServiceImpl(
            metadataStorage, sparkDispatcher, sparkConfig, jobService, securityAdapter);
  }

  @Test
  void routesSuccessfulRowsToJobServiceWithCurrentPrincipal() {
    Schema schema = new Schema(List.of(new Schema.Column("count", null, INTEGER)));
    QueryResult.Rows rows =
        new QueryResult.Rows(schema, List.of(tupleValue(Map.of("count", 3))), null, null, 10);
    stubSnapshot(QueryJobState.SUCCEEDED, Optional.of(rows), Optional.empty());

    AsyncQueryExecutionResponse response = fetch();

    assertEquals("SUCCEEDED", response.getStatus());
    assertSame(schema, response.getSchema());
    assertEquals(rows.rows(), response.getResults());
    assertNull(response.getError());
    assertNull(response.getExplain());
    verify(jobService).get(JOB_ID, ALICE);
    verifyNoSparkCalls();
  }

  @Test
  void routesExplainResponseUnformatted() {
    ExplainResponse explain =
        new ExplainResponse(new ExplainResponseNodeV2("logical", "physical", null));
    stubSnapshot(
        QueryJobState.SUCCEEDED,
        Optional.of(new QueryResult.Explain(explain, 10)),
        Optional.empty());

    AsyncQueryExecutionResponse response = fetch();

    assertEquals("SUCCEEDED", response.getStatus());
    assertEmptyResults(response);
    assertNull(response.getError());
    assertSame(explain, response.getExplain());
    verifyNoSparkCalls();
  }

  @Test
  void failedJobReturnsStoredFailureReason() {
    stubSnapshot(
        QueryJobState.FAILED,
        Optional.empty(),
        Optional.of(new QueryFailure("IllegalArgumentException", "invalid query", Map.of())));

    AsyncQueryExecutionResponse response = fetch();

    assertEquals("FAILED", response.getStatus());
    assertEquals("invalid query", response.getError());
    assertNull(response.getErrorDetails());
    assertEmptyResults(response);
    verifyNoSparkCalls();
  }

  @Test
  void failedJobCarriesStructuredErrorDetails() {
    Map<String, Object> details =
        Map.of("code", "FIELD_NOT_FOUND", "reason", "Field [x] not found.");
    stubSnapshot(
        QueryJobState.FAILED,
        Optional.empty(),
        Optional.of(new QueryFailure("IllegalArgumentException", "Field [x] not found.", details)));

    AsyncQueryExecutionResponse response = fetch();

    assertEquals("FAILED", response.getStatus());
    assertEquals("Field [x] not found.", response.getError());
    assertEquals(details, response.getErrorDetails());
    verifyNoSparkCalls();
  }

  @Test
  void failedJobWithoutFailureDetailsReturnsGenericError() {
    stubSnapshot(QueryJobState.FAILED, Optional.empty(), Optional.empty());

    AsyncQueryExecutionResponse response = fetch();

    assertEquals("FAILED", response.getStatus());
    assertEquals("query execution failed", response.getError());
    assertEmptyResults(response);
    verifyNoSparkCalls();
  }

  @Test
  void pendingSnapshotReturnsStateAndEmptyRows() {
    assertSnapshotWithoutResult(QueryJobState.PENDING);
  }

  @Test
  void runningSnapshotReturnsStateAndEmptyRows() {
    assertSnapshotWithoutResult(QueryJobState.RUNNING);
  }

  @Test
  void cancelledSnapshotReturnsStateAndEmptyRows() {
    assertSnapshotWithoutResult(QueryJobState.CANCELLED);
  }

  @Test
  void succeededSnapshotWithoutResultReturnsEmptyRows() {
    assertSnapshotWithoutResult(QueryJobState.SUCCEEDED);
  }

  private void assertSnapshotWithoutResult(QueryJobState state) {
    stubSnapshot(state, Optional.empty(), Optional.empty());

    AsyncQueryExecutionResponse response = fetch();

    assertEquals(state.name(), response.getStatus());
    assertEmptyResults(response);
    assertNull(response.getError());
    assertNull(response.getExplain());
    verifyNoSparkCalls();
  }

  @Test
  void runningMarkerIsNotRenderedAsFinalRows() {
    stubSnapshot(
        QueryJobState.SUCCEEDED, Optional.of(new QueryResult.Running(JOB_ID)), Optional.empty());

    AsyncQueryExecutionResponse response = fetch();

    assertEmptyResults(response);
    assertNull(response.getExplain());
    verifyNoSparkCalls();
  }

  @Test
  void absentSecurityAdapterUsesUnsecuredPrincipal() {
    service =
        new AsyncQueryExecutorServiceImpl(
            metadataStorage, sparkDispatcher, sparkConfig, jobService, null);
    when(jobService.get(JOB_ID, Principal.UNSECURED))
        .thenReturn(snapshot(QueryJobState.RUNNING, Optional.empty(), Optional.empty()));

    assertEquals("RUNNING", fetch().getStatus());

    verify(jobService).get(JOB_ID, Principal.UNSECURED);
    verifyNoInteractions(securityAdapter);
    verifyNoSparkCalls();
  }

  @Test
  void sparkOnlyServiceKeepsValidJobIdsOnSparkPath() {
    service = new AsyncQueryExecutorServiceImpl(metadataStorage, sparkDispatcher, sparkConfig);
    when(metadataStorage.getJobMetadata(JOB_ID.encode())).thenReturn(Optional.empty());

    assertThrows(AsyncQueryNotFoundException.class, this::fetch);

    verify(metadataStorage).getJobMetadata(JOB_ID.encode());
    verifyNoInteractions(jobService, securityAdapter, sparkDispatcher, sparkConfig);
  }

  @Test
  void nullIdKeepsSparkLookupBehavior() {
    assertSparkLookup(null);
  }

  @Test
  void emptyIdKeepsSparkLookupBehavior() {
    assertSparkLookup("");
  }

  @Test
  void blankIdKeepsSparkLookupBehavior() {
    assertSparkLookup(" ");
  }

  @Test
  void malformedJobIdKeepsSparkLookupBehavior() {
    assertSparkLookup("spark-query-id");
  }

  private void assertSparkLookup(String id) {
    when(metadataStorage.getJobMetadata(id)).thenReturn(Optional.empty());

    assertThrows(
        AsyncQueryNotFoundException.class, () -> service.getAsyncQueryResults(id, requestContext));

    verify(metadataStorage).getJobMetadata(id);
    verifyNoInteractions(jobService, securityAdapter, sparkDispatcher, sparkConfig);
  }

  @Test
  void missingPplJobPropagatesWithoutSparkFallback() {
    when(securityAdapter.current()).thenReturn(ALICE);
    QueryJobNotFoundException missing = new QueryJobNotFoundException(JOB_ID);
    when(jobService.get(JOB_ID, ALICE)).thenThrow(missing);

    assertSame(missing, assertThrows(QueryJobNotFoundException.class, this::fetch));
    verifyNoSparkCalls();
  }

  @Test
  void ownershipDenialPropagatesWithoutSparkFallback() {
    when(securityAdapter.current()).thenReturn(ALICE);
    QueryJobForbiddenException forbidden = new QueryJobForbiddenException();
    when(jobService.get(JOB_ID, ALICE)).thenThrow(forbidden);

    assertSame(forbidden, assertThrows(QueryJobForbiddenException.class, this::fetch));
    verifyNoSparkCalls();
  }

  private AsyncQueryExecutionResponse fetch() {
    return service.getAsyncQueryResults(JOB_ID.encode(), requestContext);
  }

  private void stubSnapshot(
      QueryJobState state, Optional<QueryResult> result, Optional<QueryFailure> failure) {
    when(securityAdapter.current()).thenReturn(ALICE);
    when(jobService.get(JOB_ID, ALICE)).thenReturn(snapshot(state, result, failure));
  }

  private QueryJobStatus snapshot(
      QueryJobState state, Optional<QueryResult> result, Optional<QueryFailure> failure) {
    return new QueryJobStatus(
        JOB_ID,
        state,
        0,
        state == QueryJobState.PENDING ? OptionalLong.empty() : OptionalLong.of(1),
        state.isTerminal() ? OptionalLong.of(2) : OptionalLong.empty(),
        failure,
        result);
  }

  private void assertEmptyResults(AsyncQueryExecutionResponse response) {
    assertEquals(List.of(), response.getSchema().getColumns());
    assertEquals(List.of(), response.getResults());
  }

  private void verifyNoSparkCalls() {
    verifyNoInteractions(metadataStorage, sparkDispatcher, sparkConfig);
  }
}
