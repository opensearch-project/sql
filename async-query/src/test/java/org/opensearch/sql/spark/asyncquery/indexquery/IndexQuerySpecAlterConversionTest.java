/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.indexquery;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.amazonaws.services.emrserverless.model.ValidationException;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceSpec;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.MockFlintIndex;
import org.opensearch.sql.spark.asyncquery.model.MockFlintSparkJob;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexType;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

import java.util.HashMap;
import java.util.Map;

public class IndexQuerySpecAlterConversionTest extends AsyncQueryExecutorServiceSpec {
  @Test
  public void testAlterIndexQueryConvertingToManualRefresh() {
    MockFlintIndex ALTER_SKIPPING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_skipping_index",
            FlintIndexType.SKIPPING,
            "ALTER SKIPPING INDEX ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=false)");
    MockFlintIndex ALTER_COVERING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_covering_index",
            FlintIndexType.COVERING,
            "ALTER INDEX covering ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=false)");
    MockFlintIndex ALTER_MV =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_mv",
            FlintIndexType.MATERIALIZED_VIEW,
            "ALTER MATERIALIZED VIEW my_glue.mydb.mv WITH (auto_refresh=false,"
                + " incremental_refresh=false) ");
    ImmutableList.of(ALTER_SKIPPING, ALTER_COVERING, ALTER_MV)
        .forEach(
            mockDS -> {
              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                      super.getJobRunResult(applicationId, jobId);
                      JobRun jobRun = new JobRun();
                      jobRun.setState("cancelled");
                      return new GetJobRunResult().withJobRun(jobRun);
                    }
                  };
              EMRServerlessClientFactory emrServerlessClientFactory = (accountId) -> emrsClient;
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrServerlessClientFactory);
              // Mock flint index
              mockDS.createIndex();
              HashMap<String, Object> existingOptions = new HashMap<>();
              existingOptions.put("auto_refresh", "true");
              mockDS.updateIndexOptions(existingOptions, false);
              // Mock index state
              MockFlintSparkJob flintIndexJob =
                  new MockFlintSparkJob(
                      flintIndexStateModelService, mockDS.getLatestId(), MYS3_DATASOURCE);
              flintIndexJob.active();

              // 1. alter index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(
                          mockDS.getQuery(), MYS3_DATASOURCE, LangType.SQL, null),
                      asyncQueryRequestContext);

              // 2. fetch result
              AsyncQueryExecutionResponse asyncQueryExecutionResponse =
                  asyncQueryExecutorService.getAsyncQueryResults(
                      response.getQueryId(), asyncQueryRequestContext);
              assertEquals("SUCCESS", asyncQueryExecutionResponse.getStatus());
              emrsClient.startJobRunCalled(0);
              emrsClient.cancelJobRunCalled(1);
              flintIndexJob.assertState(FlintIndexState.ACTIVE);
              Map<String, Object> mappings = mockDS.getIndexMappings();
              Map<String, Object> meta = (HashMap<String, Object>) mappings.get("_meta");
              Map<String, Object> options = (Map<String, Object>) meta.get("options");
              Assertions.assertEquals("false", options.get("auto_refresh"));
            });
  }

  @Test
  public void testAlterIndexQueryConvertingToManualRefreshWithNoIncrementalRefresh() {
    MockFlintIndex ALTER_SKIPPING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_skipping_index",
            FlintIndexType.SKIPPING,
            "ALTER SKIPPING INDEX ON my_glue.mydb.http_logs WITH (auto_refresh=false)");
    MockFlintIndex ALTER_COVERING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_covering_index",
            FlintIndexType.COVERING,
            "ALTER INDEX covering ON my_glue.mydb.http_logs WITH (auto_refresh=false)");
    MockFlintIndex ALTER_MV =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_mv",
            FlintIndexType.MATERIALIZED_VIEW,
            "ALTER MATERIALIZED VIEW my_glue.mydb.mv WITH (auto_refresh=false)");
    ImmutableList.of(ALTER_SKIPPING, ALTER_COVERING, ALTER_MV)
        .forEach(
            mockDS -> {
              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                      super.getJobRunResult(applicationId, jobId);
                      JobRun jobRun = new JobRun();
                      jobRun.setState("cancelled");
                      return new GetJobRunResult().withJobRun(jobRun);
                    }
                  };
              EMRServerlessClientFactory emrServerlessClientFactory = (accountId) -> emrsClient;
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrServerlessClientFactory);
              // Mock flint index
              mockDS.createIndex();
              HashMap<String, Object> existingOptions = new HashMap<>();
              existingOptions.put("auto_refresh", "true");
              existingOptions.put("checkpoint_location", "s3://checkpoint/location");
              mockDS.updateIndexOptions(existingOptions, true);
              // Mock index state
              MockFlintSparkJob flintIndexJob =
                  new MockFlintSparkJob(
                      flintIndexStateModelService, mockDS.getLatestId(), MYS3_DATASOURCE);
              flintIndexJob.active();

              // 1. alter index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(
                          mockDS.getQuery(), MYS3_DATASOURCE, LangType.SQL, null),
                      asyncQueryRequestContext);

              // 2. fetch result
              AsyncQueryExecutionResponse asyncQueryExecutionResponse =
                  asyncQueryExecutorService.getAsyncQueryResults(
                      response.getQueryId(), asyncQueryRequestContext);
              assertEquals("SUCCESS", asyncQueryExecutionResponse.getStatus());
              emrsClient.startJobRunCalled(0);
              emrsClient.cancelJobRunCalled(1);
              flintIndexJob.assertState(FlintIndexState.ACTIVE);
              Map<String, Object> mappings = mockDS.getIndexMappings();
              Map<String, Object> meta = (HashMap<String, Object>) mappings.get("_meta");
              Map<String, Object> options = (Map<String, Object>) meta.get("options");
              Assertions.assertEquals("false", options.get("auto_refresh"));
            });
  }

  @Test
  public void testAlterIndexQueryConvertingToAutoRefresh() {
    MockFlintIndex ALTER_SKIPPING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_skipping_index",
            FlintIndexType.SKIPPING,
            "ALTER SKIPPING INDEX ON my_glue.mydb.http_logs WITH (auto_refresh=true,"
                + " incremental_refresh=false)");
    MockFlintIndex ALTER_COVERING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_covering_index",
            FlintIndexType.COVERING,
            "ALTER INDEX covering ON my_glue.mydb.http_logs WITH (auto_refresh=true,"
                + " incremental_refresh=false)");
    MockFlintIndex ALTER_MV =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_mv",
            FlintIndexType.MATERIALIZED_VIEW,
            "ALTER MATERIALIZED VIEW my_glue.mydb.mv WITH (auto_refresh=true,"
                + " incremental_refresh=false) ");
    ImmutableList.of(ALTER_SKIPPING, ALTER_COVERING, ALTER_MV)
        .forEach(
            mockDS -> {
              LocalEMRSClient emrsClient = new LocalEMRSClient();
              EMRServerlessClientFactory clientFactory = (accountId) -> emrsClient;
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(clientFactory);

              // Mock flint index
              mockDS.createIndex();
              HashMap<String, Object> existingOptions = new HashMap<>();
              existingOptions.put("auto_refresh", "false");
              mockDS.updateIndexOptions(existingOptions, false);
              // Mock index state
              MockFlintSparkJob flintIndexJob =
                  new MockFlintSparkJob(
                      flintIndexStateModelService, mockDS.getLatestId(), MYS3_DATASOURCE);
              flintIndexJob.active();

              // 1. alter index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(
                          mockDS.getQuery(), MYS3_DATASOURCE, LangType.SQL, null),
                      asyncQueryRequestContext);

              // 2. fetch result
              assertEquals(
                  "RUNNING",
                  asyncQueryExecutorService
                      .getAsyncQueryResults(response.getQueryId(), asyncQueryRequestContext)
                      .getStatus());

              flintIndexJob.assertState(FlintIndexState.ACTIVE);
              emrsClient.startJobRunCalled(1);
              emrsClient.getJobRunResultCalled(1);
              emrsClient.cancelJobRunCalled(0);
              Map<String, Object> mappings = mockDS.getIndexMappings();
              Map<String, Object> meta = (HashMap<String, Object>) mappings.get("_meta");
              Map<String, Object> options = (Map<String, Object>) meta.get("options");
              Assertions.assertEquals("false", options.get("auto_refresh"));
            });
  }
}
