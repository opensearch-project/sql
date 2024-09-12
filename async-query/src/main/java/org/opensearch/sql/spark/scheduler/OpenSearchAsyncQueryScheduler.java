/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteRequest;
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
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.sql.spark.scheduler.job.ScheduledAsyncQueryJobRunner;
import org.opensearch.sql.spark.scheduler.model.AsyncQuerySchedulerRequest;
import org.opensearch.sql.spark.scheduler.model.ScheduledAsyncQueryJobRequest;

/** Scheduler class for managing asynchronous query jobs. */
@RequiredArgsConstructor
public class OpenSearchAsyncQueryScheduler implements AsyncQueryScheduler {
  public static final String SCHEDULER_INDEX_NAME = ".async-query-scheduler";
  public static final String SCHEDULER_PLUGIN_JOB_TYPE = "async-query-scheduler";
  private static final String SCHEDULER_INDEX_MAPPING_FILE_NAME =
      "async-query-scheduler-index-mapping.yml";
  private static final String SCHEDULER_INDEX_SETTINGS_FILE_NAME =
      "async-query-scheduler-index-settings.yml";
  private static final Logger LOG = LogManager.getLogger();

  private final Client client;
  private final ClusterService clusterService;

  @Override
  /** Schedules a new job by indexing it into the job index. */
  public void scheduleJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest) {
    ScheduledAsyncQueryJobRequest request =
        ScheduledAsyncQueryJobRequest.fromAsyncQuerySchedulerRequest(asyncQuerySchedulerRequest);
    if (!this.clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)) {
      createAsyncQuerySchedulerIndex();
    }
    IndexRequest indexRequest = new IndexRequest(SCHEDULER_INDEX_NAME);
    indexRequest.id(request.getName());
    indexRequest.opType(DocWriteRequest.OpType.CREATE);
    indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
    IndexResponse indexResponse;
    try {
      indexRequest.source(request.toXContent(JsonXContent.contentBuilder(), EMPTY_PARAMS));
      ActionFuture<IndexResponse> indexResponseActionFuture = client.index(indexRequest);
      indexResponse = indexResponseActionFuture.actionGet();
    } catch (VersionConflictEngineException exception) {
      throw new IllegalArgumentException("A job already exists with name: " + request.getName());
    } catch (Throwable e) {
      LOG.error("Failed to schedule job : {}", request.getName(), e);
      throw new RuntimeException(e);
    }

    if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
      LOG.debug("Job : {}  successfully created", request.getName());
    } else {
      throw new RuntimeException(
          "Schedule job failed with result : " + indexResponse.getResult().getLowercase());
    }
  }

  /** Unschedules a job by marking it as disabled and updating its last update time. */
  @Override
  public void unscheduleJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest) {
    String jobId = asyncQuerySchedulerRequest.getJobId();
    if (Strings.isNullOrEmpty(jobId)) {
      throw new IllegalArgumentException("JobId cannot be null or empty");
    }
    try {
      asyncQuerySchedulerRequest.setEnabled(false);
      asyncQuerySchedulerRequest.setLastUpdateTime(Instant.now());
      updateJob(asyncQuerySchedulerRequest);
      LOG.info("Unscheduled job for jobId: {}", asyncQuerySchedulerRequest);
    } catch (IllegalStateException | DocumentMissingException e) {
      LOG.error("Failed to unschedule job: {}", asyncQuerySchedulerRequest, e);
    }
  }

  /** Updates an existing job with new parameters. */
  @Override
  @SneakyThrows
  public void updateJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest) {
    ScheduledAsyncQueryJobRequest request =
        ScheduledAsyncQueryJobRequest.fromAsyncQuerySchedulerRequest(asyncQuerySchedulerRequest);
    assertIndexExists();
    UpdateRequest updateRequest = new UpdateRequest(SCHEDULER_INDEX_NAME, request.getName());
    updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
    updateRequest.doc(request.toXContent(JsonXContent.contentBuilder(), EMPTY_PARAMS));
    UpdateResponse updateResponse;
    try {
      ActionFuture<UpdateResponse> updateResponseActionFuture = client.update(updateRequest);
      updateResponse = updateResponseActionFuture.actionGet();
    } catch (DocumentMissingException exception) {
      throw new IllegalArgumentException("Job: " + request.getName() + " doesn't exist");
    } catch (Throwable e) {
      LOG.error("Failed to update job : {}", request.getName(), e);
      throw new RuntimeException(e);
    }

    if (updateResponse.getResult().equals(DocWriteResponse.Result.UPDATED)
        || updateResponse.getResult().equals(DocWriteResponse.Result.NOOP)) {
      LOG.debug("Job : {} successfully updated", request.getName());
    } else {
      throw new RuntimeException(
          "Update job failed with result : " + updateResponse.getResult().getLowercase());
    }
  }

  /** Removes a job by deleting its document from the index. */
  @Override
  public void removeJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest) {
    assertIndexExists();
    String jobId = asyncQuerySchedulerRequest.getJobId();
    if (Strings.isNullOrEmpty(jobId)) {
      throw new IllegalArgumentException("JobId cannot be null or empty");
    }
    DeleteRequest deleteRequest = new DeleteRequest(SCHEDULER_INDEX_NAME, jobId);
    deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
    ActionFuture<DeleteResponse> deleteResponseActionFuture = client.delete(deleteRequest);
    DeleteResponse deleteResponse = deleteResponseActionFuture.actionGet();

    if (deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED)) {
      LOG.debug("Job : {} successfully deleted", jobId);
    } else if (deleteResponse.getResult().equals(DocWriteResponse.Result.NOT_FOUND)) {
      throw new IllegalArgumentException("Job : " + jobId + " doesn't exist");
    } else {
      throw new RuntimeException(
          "Remove job failed with result : " + deleteResponse.getResult().getLowercase());
    }
  }

  /** Creates the async query scheduler index with specified mappings and settings. */
  @VisibleForTesting
  void createAsyncQuerySchedulerIndex() {
    try {
      InputStream mappingFileStream =
          OpenSearchAsyncQueryScheduler.class
              .getClassLoader()
              .getResourceAsStream(SCHEDULER_INDEX_MAPPING_FILE_NAME);
      InputStream settingsFileStream =
          OpenSearchAsyncQueryScheduler.class
              .getClassLoader()
              .getResourceAsStream(SCHEDULER_INDEX_SETTINGS_FILE_NAME);
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(SCHEDULER_INDEX_NAME);
      createIndexRequest.mapping(
          IOUtils.toString(mappingFileStream, StandardCharsets.UTF_8), XContentType.YAML);
      createIndexRequest.settings(
          IOUtils.toString(settingsFileStream, StandardCharsets.UTF_8), XContentType.YAML);
      ActionFuture<CreateIndexResponse> createIndexResponseActionFuture =
          client.admin().indices().create(createIndexRequest);
      CreateIndexResponse createIndexResponse = createIndexResponseActionFuture.actionGet();

      if (createIndexResponse.isAcknowledged()) {
        LOG.debug("Index: {} creation Acknowledged", SCHEDULER_INDEX_NAME);
      } else {
        throw new RuntimeException("Index creation is not acknowledged.");
      }
    } catch (Throwable e) {
      LOG.error("Error creating index: {}", SCHEDULER_INDEX_NAME, e);
      throw new RuntimeException(
          "Internal server error while creating "
              + SCHEDULER_INDEX_NAME
              + " index: "
              + e.getMessage(),
          e);
    }
  }

  private void assertIndexExists() {
    if (!this.clusterService.state().routingTable().hasIndex(SCHEDULER_INDEX_NAME)) {
      throw new IllegalStateException("Job index does not exist.");
    }
  }

  /** Returns the job runner instance for the scheduler. */
  public static ScheduledJobRunner getJobRunner() {
    return ScheduledAsyncQueryJobRunner.getJobRunnerInstance();
  }
}
