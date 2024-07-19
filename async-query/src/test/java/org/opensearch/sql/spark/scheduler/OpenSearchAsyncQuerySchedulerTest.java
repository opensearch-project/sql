/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.scheduler.OpenSearchAsyncQueryScheduler.SCHEDULER_INDEX_NAME;

import java.io.IOException;
import java.time.Instant;
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
import org.opensearch.sql.spark.scheduler.model.OpenSearchRefreshIndexJobRequest;
import org.opensearch.threadpool.ThreadPool;

public class OpenSearchAsyncQuerySchedulerTest {

  private static final String TEST_SCHEDULER_INDEX_NAME = "testQS";

  private static final String TEST_JOB_ID = "testJob";

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Client client;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ClusterService clusterService;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ThreadPool threadPool;

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
    scheduler = new OpenSearchAsyncQueryScheduler();
    scheduler.loadJobResource(client, clusterService, threadPool);
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
    when(indexResponseActionFuture.actionGet()).thenReturn(indexResponse);
    when(indexResponse.getResult()).thenReturn(DocWriteResponse.Result.CREATED);

    OpenSearchRefreshIndexJobRequest request =
        OpenSearchRefreshIndexJobRequest.builder()
            .jobName(TEST_JOB_ID)
            .lastUpdateTime(Instant.now())
            .build();

    scheduler.scheduleJob(request);

    // Verify index created
    verify(client.admin().indices(), Mockito.times(1)).create(ArgumentMatchers.any());

    // Verify doc indexed
    ArgumentCaptor<IndexRequest> captor = ArgumentCaptor.forClass(IndexRequest.class);
    verify(client, Mockito.times(1)).index(captor.capture());
    IndexRequest capturedRequest = captor.getValue();
    assertEquals(request.getName(), capturedRequest.id());
    assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, capturedRequest.getRefreshPolicy());
  }

  @Test
  public void testScheduleJobWithExistingJob() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME))
        .thenReturn(Boolean.TRUE);

    OpenSearchRefreshIndexJobRequest request =
        OpenSearchRefreshIndexJobRequest.builder()
            .jobName(TEST_JOB_ID)
            .lastUpdateTime(Instant.now())
            .build();

    when(client.index(any(IndexRequest.class))).thenThrow(VersionConflictEngineException.class);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              scheduler.scheduleJob(request);
            });

    verify(client, Mockito.times(1)).index(ArgumentCaptor.forClass(IndexRequest.class).capture());
    assertEquals("A job already exists with name: testJob", exception.getMessage());
  }

  @Test
  public void testScheduleJobWithException() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME))
        .thenReturn(Boolean.FALSE);
    when(client.admin().indices().create(any(CreateIndexRequest.class)))
        .thenReturn(createIndexResponseActionFuture);
    when(createIndexResponseActionFuture.actionGet())
        .thenReturn(new CreateIndexResponse(true, true, TEST_SCHEDULER_INDEX_NAME));
    when(client.index(any(IndexRequest.class))).thenThrow(new RuntimeException("Test exception"));

    OpenSearchRefreshIndexJobRequest request =
        OpenSearchRefreshIndexJobRequest.builder()
            .jobName(TEST_JOB_ID)
            .lastUpdateTime(Instant.now())
            .build();

    assertThrows(RuntimeException.class, () -> scheduler.scheduleJob(request));
  }

  @Test
  public void testUnscheduleJob() throws IOException {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(true);

    when(updateResponseActionFuture.actionGet()).thenReturn(updateResponse);
    when(updateResponse.getResult()).thenReturn(DocWriteResponse.Result.UPDATED);

    when(client.update(any(UpdateRequest.class))).thenReturn(updateResponseActionFuture);

    scheduler.unscheduleJob(TEST_JOB_ID);

    ArgumentCaptor<UpdateRequest> captor = ArgumentCaptor.forClass(UpdateRequest.class);
    verify(client).update(captor.capture());

    UpdateRequest capturedRequest = captor.getValue();
    assertEquals(TEST_JOB_ID, capturedRequest.id());
    assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, capturedRequest.getRefreshPolicy());
  }

  @Test
  public void testUnscheduleJobWithIndexNotFound() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(false);

    assertThrows(IllegalArgumentException.class, () -> scheduler.unscheduleJob(TEST_JOB_ID));
  }

  @Test
  public void testUpdateJob() throws IOException {
    OpenSearchRefreshIndexJobRequest request =
        OpenSearchRefreshIndexJobRequest.builder()
            .jobName(TEST_JOB_ID)
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
    OpenSearchRefreshIndexJobRequest request =
        OpenSearchRefreshIndexJobRequest.builder()
            .jobName(TEST_JOB_ID)
            .lastUpdateTime(Instant.now())
            .build();

    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(false);

    assertThrows(IllegalArgumentException.class, () -> scheduler.updateJob(request));
  }

  @Test
  public void testRemoveJob() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(true);

    DeleteResponse deleteResponse = mock(DeleteResponse.class);
    when(deleteResponseActionFuture.actionGet()).thenReturn(deleteResponse);
    when(deleteResponse.getResult()).thenReturn(DocWriteResponse.Result.DELETED);

    when(client.delete(any(DeleteRequest.class))).thenReturn(deleteResponseActionFuture);

    scheduler.removeJob(TEST_JOB_ID);

    ArgumentCaptor<DeleteRequest> captor = ArgumentCaptor.forClass(DeleteRequest.class);
    verify(client).delete(captor.capture());

    DeleteRequest capturedRequest = captor.getValue();
    assertEquals(TEST_JOB_ID, capturedRequest.id());
    assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, capturedRequest.getRefreshPolicy());
  }

  @Test
  public void testRemoveJobWithIndexNotFound() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(false);

    assertThrows(IllegalArgumentException.class, () -> scheduler.removeJob(TEST_JOB_ID));
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
  }

  @Test
  public void testUpdateJobNotFound() {
    OpenSearchRefreshIndexJobRequest request =
        OpenSearchRefreshIndexJobRequest.builder()
            .jobName(TEST_JOB_ID)
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

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              scheduler.removeJob(TEST_JOB_ID);
            });

    assertEquals("Job : testJob doesn't exist", exception.getMessage());
  }

  @Test
  public void testRemoveJobWithException() {
    when(clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)).thenReturn(true);

    when(client.delete(any(DeleteRequest.class))).thenThrow(new RuntimeException("Test exception"));

    assertThrows(RuntimeException.class, () -> scheduler.removeJob(TEST_JOB_ID));
  }
}
