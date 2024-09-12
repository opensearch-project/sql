/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.scheduler.OpenSearchAsyncQueryScheduler.SCHEDULER_INDEX_NAME;

import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.sql.spark.scheduler.model.AsyncQuerySchedulerRequest;
import org.opensearch.sql.spark.scheduler.model.ScheduledAsyncQueryJobRequest;

public class OpenSearchAsyncQuerySchedulerTest {

  private static final String TEST_SCHEDULER_INDEX_NAME = "testQS";

  private static final String TEST_JOB_ID = "testJob";

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Client client;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ClusterService clusterService;

  @Mock private ActionFuture<IndexResponse> indexResponseActionFuture;

  @Mock private ActionFuture<UpdateResponse> updateResponseActionFuture;

  @Mock private ActionFuture<DeleteResponse> deleteResponseActionFuture;

  @Mock private ActionFuture<CreateIndexResponse> createIndexResponseActionFuture;

  @Mock private IndexResponse indexResponse;

  @Mock private UpdateResponse updateResponse;

  private OpenSearchAsyncQueryScheduler scheduler;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.openMocks(this);
    scheduler = new OpenSearchAsyncQueryScheduler(client, clusterService);
  }

  @Test
  public void testScheduleJob() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME))
        .thenReturn(Boolean.FALSE);
    when(client.admin().indices().create(any(CreateIndexRequest.class)))
        .thenReturn(createIndexResponseActionFuture);
    when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(true, true, TEST_SCHEDULER_INDEX_NAME));
    when(client.index(any(IndexRequest.class))).thenReturn(indexResponseActionFuture);

    // Test the if case
    when(indexResponseActionFuture.actionGet()).thenReturn(indexResponse);
    when(indexResponse.getResult()).thenReturn(DocWriteResponse.Result.CREATED);

    ScheduledAsyncQueryJobRequest request =
        ScheduledAsyncQueryJobRequest.scheduledAsyncQueryJobRequestBuilder()
            .jobId(TEST_JOB_ID)
            .lastUpdateTime(Instant.now())
            .build();

    scheduler.scheduleJob(request);

    // Verify index created
    verify(client.admin().indices(), times(1)).create(ArgumentMatchers.any());

    // Verify doc indexed
    ArgumentCaptor<IndexRequest> captor = ArgumentCaptor.forClass(IndexRequest.class);
    verify(client, times(1)).index(captor.capture());
    IndexRequest capturedRequest = captor.getValue();
    assertEquals(request.getName(), capturedRequest.id());
    assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, capturedRequest.getRefreshPolicy());
  }

  @Test
  public void testScheduleJobWithExistingJob() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME))
        .thenReturn(Boolean.TRUE);

    ScheduledAsyncQueryJobRequest request =
        ScheduledAsyncQueryJobRequest.scheduledAsyncQueryJobRequestBuilder()
            .jobId(TEST_JOB_ID)
            .lastUpdateTime(Instant.now())
            .build();

    when(client.index(any(IndexRequest.class))).thenThrow(VersionConflictEngineException.class);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              scheduler.scheduleJob(request);
            });

    verify(client, times(1)).index(ArgumentCaptor.forClass(IndexRequest.class).capture());
    assertEquals("A job already exists with name: testJob", exception.getMessage());
  }

  @Test
  public void testScheduleJobWithExceptions() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME))
        .thenReturn(Boolean.FALSE);
    when(client.admin().indices().create(any(CreateIndexRequest.class)))
        .thenReturn(createIndexResponseActionFuture);
    when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(true, true, TEST_SCHEDULER_INDEX_NAME));
    when(client.index(any(IndexRequest.class))).thenThrow(new RuntimeException("Test exception"));

    ScheduledAsyncQueryJobRequest request =
        ScheduledAsyncQueryJobRequest.scheduledAsyncQueryJobRequestBuilder()
            .jobId(TEST_JOB_ID)
            .lastUpdateTime(Instant.now())
            .build();

    assertThrows(RuntimeException.class, () -> scheduler.scheduleJob(request));

    when(client.index(any(IndexRequest.class))).thenReturn(indexResponseActionFuture);
    when(indexResponseActionFuture.actionGet()).thenReturn(indexResponse);
    when(indexResponse.getResult()).thenReturn(DocWriteResponse.Result.NOT_FOUND);

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> scheduler.scheduleJob(request));
    assertEquals("Schedule job failed with result : not_found", exception.getMessage());
  }

  @Test
  public void testUnscheduleJob() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(true);

    when(updateResponseActionFuture.actionGet()).thenReturn(updateResponse);
    when(updateResponse.getResult()).thenReturn(DocWriteResponse.Result.UPDATED);

    when(client.update(any(UpdateRequest.class))).thenReturn(updateResponseActionFuture);

    AsyncQuerySchedulerRequest request =
        AsyncQuerySchedulerRequest.builder().jobId(TEST_JOB_ID).build();
    scheduler.unscheduleJob(request);

    ArgumentCaptor<UpdateRequest> captor = ArgumentCaptor.forClass(UpdateRequest.class);
    verify(client).update(captor.capture());

    UpdateRequest capturedRequest = captor.getValue();
    assertEquals(TEST_JOB_ID, capturedRequest.id());
    assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, capturedRequest.getRefreshPolicy());

    // Reset the captor for the next verification
    captor = ArgumentCaptor.forClass(UpdateRequest.class);

    when(updateResponse.getResult()).thenReturn(DocWriteResponse.Result.NOOP);
    scheduler.unscheduleJob(request);

    verify(client, times(2)).update(captor.capture());
    capturedRequest = captor.getValue();
    assertEquals(TEST_JOB_ID, capturedRequest.id());
    assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, capturedRequest.getRefreshPolicy());
  }

  @Test
  public void testUnscheduleJobInvalidJobId() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(true);

    AsyncQuerySchedulerRequest request = AsyncQuerySchedulerRequest.builder().build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> scheduler.unscheduleJob(request));
    assertEquals("JobId cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testUnscheduleJobWithIndexNotFound() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(false);

    AsyncQuerySchedulerRequest request =
        AsyncQuerySchedulerRequest.builder().jobId(TEST_JOB_ID).build();
    scheduler.unscheduleJob(request);

    // Verify that no update operation was performed
    verify(client, never()).update(any(UpdateRequest.class));
  }

  @Test
  public void testUpdateJob() {
    ScheduledAsyncQueryJobRequest request =
        ScheduledAsyncQueryJobRequest.scheduledAsyncQueryJobRequestBuilder()
            .jobId(TEST_JOB_ID)
            .lastUpdateTime(Instant.now())
            .build();

    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(true);

    when(updateResponseActionFuture.actionGet()).thenReturn(updateResponse);
    when(updateResponse.getResult()).thenReturn(DocWriteResponse.Result.UPDATED);

    when(client.update(any(UpdateRequest.class))).thenReturn(updateResponseActionFuture);

    scheduler.updateJob(request);

    ArgumentCaptor<UpdateRequest> captor = ArgumentCaptor.forClass(UpdateRequest.class);
    verify(client).update(captor.capture());

    UpdateRequest capturedRequest = captor.getValue();
    assertEquals(request.getName(), capturedRequest.id());
    assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, capturedRequest.getRefreshPolicy());
  }

  @Test
  public void testUpdateJobWithIndexNotFound() {
    ScheduledAsyncQueryJobRequest request =
        ScheduledAsyncQueryJobRequest.scheduledAsyncQueryJobRequestBuilder()
            .jobId(TEST_JOB_ID)
            .lastUpdateTime(Instant.now())
            .build();

    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(false);

    assertThrows(IllegalStateException.class, () -> scheduler.updateJob(request));
  }

  @Test
  public void testUpdateJobWithExceptions() {
    ScheduledAsyncQueryJobRequest request =
        ScheduledAsyncQueryJobRequest.scheduledAsyncQueryJobRequestBuilder()
            .jobId(TEST_JOB_ID)
            .lastUpdateTime(Instant.now())
            .build();

    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(true);
    when(client.update(any(UpdateRequest.class)))
        .thenThrow(new DocumentMissingException(null, null));

    IllegalArgumentException exception1 =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              scheduler.updateJob(request);
            });

    assertEquals("Job: testJob doesn't exist", exception1.getMessage());

    when(client.update(any(UpdateRequest.class))).thenThrow(new RuntimeException("Test exception"));

    RuntimeException exception2 =
        assertThrows(
            RuntimeException.class,
            () -> {
              scheduler.updateJob(request);
            });

    assertEquals("java.lang.RuntimeException: Test exception", exception2.getMessage());

    when(client.update(any(UpdateRequest.class))).thenReturn(updateResponseActionFuture);
    when(updateResponseActionFuture.actionGet()).thenReturn(updateResponse);
    when(updateResponse.getResult()).thenReturn(DocWriteResponse.Result.NOT_FOUND);

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> scheduler.updateJob(request));
    assertEquals("Update job failed with result : not_found", exception.getMessage());
  }

  @Test
  public void testRemoveJob() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(true);

    DeleteResponse deleteResponse = mock(DeleteResponse.class);
    when(deleteResponseActionFuture.actionGet()).thenReturn(deleteResponse);
    when(deleteResponse.getResult()).thenReturn(DocWriteResponse.Result.DELETED);

    when(client.delete(any(DeleteRequest.class))).thenReturn(deleteResponseActionFuture);

    AsyncQuerySchedulerRequest request =
        AsyncQuerySchedulerRequest.builder().jobId(TEST_JOB_ID).build();
    scheduler.removeJob(request);

    ArgumentCaptor<DeleteRequest> captor = ArgumentCaptor.forClass(DeleteRequest.class);
    verify(client).delete(captor.capture());

    DeleteRequest capturedRequest = captor.getValue();
    assertEquals(TEST_JOB_ID, capturedRequest.id());
    assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, capturedRequest.getRefreshPolicy());
  }

  @Test
  public void testRemoveJobWithIndexNotFound() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(false);

    AsyncQuerySchedulerRequest request =
        AsyncQuerySchedulerRequest.builder().jobId(TEST_JOB_ID).build();
    assertThrows(IllegalStateException.class, () -> scheduler.removeJob(request));
  }

  @Test
  public void testRemoveJobInvalidJobId() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(true);

    AsyncQuerySchedulerRequest request = AsyncQuerySchedulerRequest.builder().jobId("").build();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> scheduler.removeJob(request));
    assertEquals("JobId cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testCreateAsyncQuerySchedulerIndex() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(false);

    CreateIndexResponse createIndexResponse = mock(CreateIndexResponse.class);
    when(createIndexResponseActionFuture.actionGet()).thenReturn(createIndexResponse);
    when(createIndexResponse.isAcknowledged()).thenReturn(true);

    when(client.admin().indices().create(any(CreateIndexRequest.class)))
        .thenReturn(createIndexResponseActionFuture);

    scheduler.createAsyncQuerySchedulerIndex();

    ArgumentCaptor<CreateIndexRequest> captor = ArgumentCaptor.forClass(CreateIndexRequest.class);
    verify(client.admin().indices()).create(captor.capture());

    CreateIndexRequest capturedRequest = captor.getValue();
    assertEquals(SCHEDULER_INDEX_NAME, capturedRequest.index());
  }

  @Test
  public void testCreateAsyncQuerySchedulerIndexFailure() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(false);

    when(client.admin().indices().create(any(CreateIndexRequest.class)))
        .thenThrow(new RuntimeException("Error creating index"));

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              scheduler.createAsyncQuerySchedulerIndex();
            });

    assertEquals(
        "Internal server error while creating .async-query-scheduler index: Error creating index",
        exception.getMessage());

    when(client.admin().indices().create(any(CreateIndexRequest.class)))
        .thenReturn(createIndexResponseActionFuture);
    Mockito.when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(false, false, SCHEDULER_INDEX_NAME));

    ScheduledAsyncQueryJobRequest request =
        ScheduledAsyncQueryJobRequest.scheduledAsyncQueryJobRequestBuilder()
            .jobId(TEST_JOB_ID)
            .lastUpdateTime(Instant.now())
            .build();

    RuntimeException runtimeException =
        Assertions.assertThrows(RuntimeException.class, () -> scheduler.scheduleJob(request));
    Assertions.assertEquals(
        "Internal server error while creating .async-query-scheduler index: Index creation is not"
            + " acknowledged.",
        runtimeException.getMessage());
  }

  @Test
  public void testUpdateJobNotFound() {
    ScheduledAsyncQueryJobRequest request =
        ScheduledAsyncQueryJobRequest.scheduledAsyncQueryJobRequestBuilder()
            .jobId(TEST_JOB_ID)
            .lastUpdateTime(Instant.now())
            .build();

    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(true);

    when(client.update(any(UpdateRequest.class)))
        .thenThrow(new DocumentMissingException(null, null));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              scheduler.updateJob(request);
            });

    assertEquals("Job: testJob doesn't exist", exception.getMessage());
  }

  @Test
  public void testRemoveJobNotFound() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(true);

    DeleteResponse deleteResponse = mock(DeleteResponse.class);
    when(deleteResponseActionFuture.actionGet()).thenReturn(deleteResponse);
    when(deleteResponse.getResult()).thenReturn(DocWriteResponse.Result.NOT_FOUND);

    when(client.delete(any(DeleteRequest.class))).thenReturn(deleteResponseActionFuture);

    AsyncQuerySchedulerRequest request =
        AsyncQuerySchedulerRequest.builder().jobId(TEST_JOB_ID).build();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              scheduler.removeJob(request);
            });

    assertEquals("Job : testJob doesn't exist", exception.getMessage());
  }

  @Test
  public void testRemoveJobWithExceptions() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(true);

    when(client.delete(any(DeleteRequest.class))).thenThrow(new RuntimeException("Test exception"));

    AsyncQuerySchedulerRequest request =
        AsyncQuerySchedulerRequest.builder().jobId(TEST_JOB_ID).build();
    assertThrows(RuntimeException.class, () -> scheduler.removeJob(request));

    DeleteResponse deleteResponse = mock(DeleteResponse.class);
    when(client.delete(any(DeleteRequest.class))).thenReturn(deleteResponseActionFuture);
    when(deleteResponseActionFuture.actionGet()).thenReturn(deleteResponse);
    when(deleteResponse.getResult()).thenReturn(DocWriteResponse.Result.NOOP);

    RuntimeException runtimeException =
        Assertions.assertThrows(RuntimeException.class, () -> scheduler.removeJob(request));
    Assertions.assertEquals("Remove job failed with result : noop", runtimeException.getMessage());
  }

  @Test
  public void testGetJobRunner() {
    ScheduledJobRunner jobRunner = OpenSearchAsyncQueryScheduler.getJobRunner();
    assertNotNull(jobRunner);
  }
}
