/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.plugins.Plugin;
import org.opensearch.sql.legacy.executor.AsyncRestExecutor;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.scheduler.model.ScheduledAsyncQueryJobRequest;
import org.opensearch.threadpool.ThreadPool;

/**
 * The job runner class for scheduling async query.
 *
 * <p>The job runner should be a singleton class if it uses OpenSearch client or other objects
 * passed from OpenSearch. Because when registering the job runner to JobScheduler plugin,
 * OpenSearch has not invoked plugins' createComponents() method. That is saying the plugin is not
 * completely initialized, and the OpenSearch {@link org.opensearch.client.Client}, {@link
 * ClusterService} and other objects are not available to plugin and this job runner.
 *
 * <p>So we have to move this job runner initialization to {@link Plugin} createComponents() method,
 * and using singleton job runner to ensure we register a usable job runner instance to JobScheduler
 * plugin.
 */
public class ScheduledAsyncQueryJobRunner implements ScheduledJobRunner {
  // Share SQL plugin thread pool
  private static final String ASYNC_QUERY_THREAD_POOL_NAME =
      AsyncRestExecutor.SQL_WORKER_THREAD_POOL_NAME;
  private static final Logger LOGGER = LogManager.getLogger(ScheduledAsyncQueryJobRunner.class);

  private static final ScheduledAsyncQueryJobRunner INSTANCE = new ScheduledAsyncQueryJobRunner();

  public static ScheduledAsyncQueryJobRunner getJobRunnerInstance() {
    return INSTANCE;
  }

  private ClusterService clusterService;
  private ThreadPool threadPool;
  private Client client;
  private AsyncQueryExecutorService asyncQueryExecutorService;

  private ScheduledAsyncQueryJobRunner() {
    // Singleton class, use getJobRunnerInstance method instead of constructor
  }

  /** Loads job resources, setting up required services and job runner instance. */
  public void loadJobResource(
      Client client,
      ClusterService clusterService,
      ThreadPool threadPool,
      AsyncQueryExecutorService asyncQueryExecutorService) {
    this.client = client;
    this.clusterService = clusterService;
    this.threadPool = threadPool;
    this.asyncQueryExecutorService = asyncQueryExecutorService;
  }

  @Override
  public void runJob(ScheduledJobParameter jobParameter, JobExecutionContext context) {
    // Parser will convert jobParameter to ScheduledAsyncQueryJobRequest
    if (!(jobParameter instanceof ScheduledAsyncQueryJobRequest)) {
      throw new IllegalStateException(
          "Job parameter is not instance of ScheduledAsyncQueryJobRequest, type: "
              + jobParameter.getClass().getCanonicalName());
    }

    if (this.clusterService == null) {
      throw new IllegalStateException("ClusterService is not initialized.");
    }

    if (this.threadPool == null) {
      throw new IllegalStateException("ThreadPool is not initialized.");
    }

    if (this.client == null) {
      throw new IllegalStateException("Client is not initialized.");
    }

    if (this.asyncQueryExecutorService == null) {
      throw new IllegalStateException("AsyncQueryExecutorService is not initialized.");
    }

    Runnable runnable =
        () -> {
          try {
            doRefresh((ScheduledAsyncQueryJobRequest) jobParameter);
          } catch (Throwable throwable) {
            LOGGER.error(throwable);
          }
        };
    threadPool.executor(ASYNC_QUERY_THREAD_POOL_NAME).submit(runnable);
  }

  void doRefresh(ScheduledAsyncQueryJobRequest request) {
    LOGGER.info("Scheduled refresh index job on job: " + request.getName());
    CreateAsyncQueryRequest createAsyncQueryRequest =
        new CreateAsyncQueryRequest(
            request.getScheduledQuery(), request.getDataSource(), request.getQueryLang());
    CreateAsyncQueryResponse createAsyncQueryResponse =
        asyncQueryExecutorService.createAsyncQuery(
            createAsyncQueryRequest, new NullAsyncQueryRequestContext());
    LOGGER.info("Created async query with queryId: " + createAsyncQueryResponse.getQueryId());
  }
}
