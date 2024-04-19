/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.cluster;

import static org.opensearch.sql.datasource.model.DataSourceStatus.DISABLED;

import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.opensearch.sql.datasource.model.DataSourceStatus;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceSpec;
import org.opensearch.sql.spark.asyncquery.model.MockFlintIndex;
import org.opensearch.sql.spark.asyncquery.model.MockFlintSparkJob;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.flint.FlintIndexMetadataServiceImpl;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexType;

public class FlintStreamingJobHouseKeeperTaskTest extends AsyncQueryExecutorServiceSpec {

  @Test
  @SneakyThrows
  public void testStreamingJobHouseKeeperWhenDataSourceDisabled() {
    MockFlintIndex SKIPPING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_skipping_index",
            FlintIndexType.SKIPPING,
            "ALTER SKIPPING INDEX ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\")");
    MockFlintIndex COVERING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_covering_index",
            FlintIndexType.COVERING,
            "ALTER INDEX covering ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\")");
    MockFlintIndex MV =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_mv",
            FlintIndexType.MATERIALIZED_VIEW,
            "ALTER MATERIALIZED VIEW my_glue.mydb.mv WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\") ");
    Map<MockFlintIndex, MockFlintSparkJob> indexJobMapping = new HashMap<>();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              INDEX.createIndex();
              MockFlintSparkJob flintIndexJob =
                  new MockFlintSparkJob(stateStore, INDEX.getLatestId(), MYGLUE_DATASOURCE);
              indexJobMapping.put(INDEX, flintIndexJob);
              HashMap<String, Object> existingOptions = new HashMap<>();
              existingOptions.put("auto_refresh", "true");
              // Making Index Auto Refresh
              INDEX.updateIndexOptions(existingOptions, false);
              flintIndexJob.refreshing();
            });
    changeDataSourceStatus(MYGLUE_DATASOURCE, DISABLED);
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
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    FlintStreamingJobHouseKeeperTask flintStreamingJobHouseKeeperTask =
        new FlintStreamingJobHouseKeeperTask(
            dataSourceService, flintIndexMetadataService, stateStore, emrServerlessClientFactory);
    Thread thread = new Thread(flintStreamingJobHouseKeeperTask);
    thread.start();
    thread.join();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              MockFlintSparkJob flintIndexJob = indexJobMapping.get(INDEX);
              flintIndexJob.assertState(FlintIndexState.ACTIVE);
              Map<String, Object> mappings = INDEX.getIndexMappings();
              Map<String, Object> meta = (HashMap<String, Object>) mappings.get("_meta");
              Map<String, Object> options = (Map<String, Object>) meta.get("options");
              Assertions.assertEquals("false", options.get("auto_refresh"));
            });
    emrsClient.cancelJobRunCalled(3);
    emrsClient.getJobRunResultCalled(3);
    emrsClient.startJobRunCalled(0);
    Assertions.assertEquals(
        0L,
        Metrics.getInstance()
            .getNumericalMetric(MetricName.STREAMING_JOB_HOUSEKEEPER_TASK_FAILURE_COUNT)
            .getValue());
  }

  @Test
  @SneakyThrows
  public void testStreamingJobHouseKeeperWhenCancelJobGivesTimeout() {
    MockFlintIndex SKIPPING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_skipping_index",
            FlintIndexType.SKIPPING,
            "ALTER SKIPPING INDEX ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\")");
    MockFlintIndex COVERING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_covering_index",
            FlintIndexType.COVERING,
            "ALTER INDEX covering ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\")");
    MockFlintIndex MV =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_mv",
            FlintIndexType.MATERIALIZED_VIEW,
            "ALTER MATERIALIZED VIEW my_glue.mydb.mv WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\") ");
    Map<MockFlintIndex, MockFlintSparkJob> indexJobMapping = new HashMap<>();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              INDEX.createIndex();
              MockFlintSparkJob flintIndexJob =
                  new MockFlintSparkJob(stateStore, INDEX.getLatestId(), MYGLUE_DATASOURCE);
              indexJobMapping.put(INDEX, flintIndexJob);
              HashMap<String, Object> existingOptions = new HashMap<>();
              existingOptions.put("auto_refresh", "true");
              // Making Index Auto Refresh
              INDEX.updateIndexOptions(existingOptions, false);
              flintIndexJob.refreshing();
            });
    changeDataSourceStatus(MYGLUE_DATASOURCE, DISABLED);
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    FlintStreamingJobHouseKeeperTask flintStreamingJobHouseKeeperTask =
        new FlintStreamingJobHouseKeeperTask(
            dataSourceService, flintIndexMetadataService, stateStore, emrServerlessClientFactory);
    Thread thread = new Thread(flintStreamingJobHouseKeeperTask);
    thread.start();
    thread.join();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              MockFlintSparkJob flintIndexJob = indexJobMapping.get(INDEX);
              flintIndexJob.assertState(FlintIndexState.REFRESHING);
              Map<String, Object> mappings = INDEX.getIndexMappings();
              Map<String, Object> meta = (HashMap<String, Object>) mappings.get("_meta");
              Map<String, Object> options = (Map<String, Object>) meta.get("options");
              Assertions.assertEquals("false", options.get("auto_refresh"));
            });
    emrsClient.cancelJobRunCalled(3);
    emrsClient.getJobRunResultCalled(9);
    emrsClient.startJobRunCalled(0);
    Assertions.assertEquals(
        3L,
        Metrics.getInstance()
            .getNumericalMetric(MetricName.STREAMING_JOB_HOUSEKEEPER_TASK_FAILURE_COUNT)
            .getValue());
  }

  @Test
  @SneakyThrows
  public void testSimulateConcurrentJobHouseKeeperExecution() {
    MockFlintIndex SKIPPING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_skipping_index",
            FlintIndexType.SKIPPING,
            "ALTER SKIPPING INDEX ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\")");
    MockFlintIndex COVERING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_covering_index",
            FlintIndexType.COVERING,
            "ALTER INDEX covering ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\")");
    MockFlintIndex MV =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_mv",
            FlintIndexType.MATERIALIZED_VIEW,
            "ALTER MATERIALIZED VIEW my_glue.mydb.mv WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\") ");
    Map<MockFlintIndex, MockFlintSparkJob> indexJobMapping = new HashMap<>();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              INDEX.createIndex();
              MockFlintSparkJob flintIndexJob =
                  new MockFlintSparkJob(stateStore, INDEX.getLatestId(), MYGLUE_DATASOURCE);
              indexJobMapping.put(INDEX, flintIndexJob);
              HashMap<String, Object> existingOptions = new HashMap<>();
              existingOptions.put("auto_refresh", "true");
              // Making Index Auto Refresh
              INDEX.updateIndexOptions(existingOptions, false);
              flintIndexJob.refreshing();
            });
    changeDataSourceStatus(MYGLUE_DATASOURCE, DISABLED);
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    FlintStreamingJobHouseKeeperTask flintStreamingJobHouseKeeperTask =
        new FlintStreamingJobHouseKeeperTask(
            dataSourceService, flintIndexMetadataService, stateStore, emrServerlessClientFactory);
    FlintStreamingJobHouseKeeperTask.isRunning.compareAndSet(false, true);
    Thread thread = new Thread(flintStreamingJobHouseKeeperTask);
    thread.start();
    thread.join();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              MockFlintSparkJob flintIndexJob = indexJobMapping.get(INDEX);
              flintIndexJob.assertState(FlintIndexState.REFRESHING);
              Map<String, Object> mappings = INDEX.getIndexMappings();
              Map<String, Object> meta = (HashMap<String, Object>) mappings.get("_meta");
              Map<String, Object> options = (Map<String, Object>) meta.get("options");
              Assertions.assertEquals("true", options.get("auto_refresh"));
            });
    emrsClient.cancelJobRunCalled(0);
    emrsClient.getJobRunResultCalled(0);
    emrsClient.startJobRunCalled(0);
    Assertions.assertEquals(
        0L,
        Metrics.getInstance()
            .getNumericalMetric(MetricName.STREAMING_JOB_HOUSEKEEPER_TASK_FAILURE_COUNT)
            .getValue());
    FlintStreamingJobHouseKeeperTask.isRunning.compareAndSet(true, false);
  }

  @SneakyThrows
  @Test
  public void testStreamingJobClearnerWhenDataSourceIsDeleted() {
    MockFlintIndex SKIPPING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_skipping_index",
            FlintIndexType.SKIPPING,
            "ALTER SKIPPING INDEX ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\")");
    MockFlintIndex COVERING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_covering_index",
            FlintIndexType.COVERING,
            "ALTER INDEX covering ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\")");
    MockFlintIndex MV =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_mv",
            FlintIndexType.MATERIALIZED_VIEW,
            "ALTER MATERIALIZED VIEW my_glue.mydb.mv WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\") ");
    Map<MockFlintIndex, MockFlintSparkJob> indexJobMapping = new HashMap<>();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              INDEX.createIndex();
              MockFlintSparkJob flintIndexJob =
                  new MockFlintSparkJob(stateStore, INDEX.getLatestId(), MYGLUE_DATASOURCE);
              indexJobMapping.put(INDEX, flintIndexJob);
              HashMap<String, Object> existingOptions = new HashMap<>();
              existingOptions.put("auto_refresh", "true");
              // Making Index Auto Refresh
              INDEX.updateIndexOptions(existingOptions, false);
              flintIndexJob.refreshing();
            });
    this.dataSourceService.deleteDataSource(MYGLUE_DATASOURCE);
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
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    FlintStreamingJobHouseKeeperTask flintStreamingJobHouseKeeperTask =
        new FlintStreamingJobHouseKeeperTask(
            dataSourceService, flintIndexMetadataService, stateStore, emrServerlessClientFactory);
    Thread thread = new Thread(flintStreamingJobHouseKeeperTask);
    thread.start();
    thread.join();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              MockFlintSparkJob flintIndexJob = indexJobMapping.get(INDEX);
              flintIndexJob.assertState(FlintIndexState.DELETED);
              Map<String, Object> mappings = INDEX.getIndexMappings();
              Map<String, Object> meta = (HashMap<String, Object>) mappings.get("_meta");
              Map<String, Object> options = (Map<String, Object>) meta.get("options");
              Assertions.assertEquals("true", options.get("auto_refresh"));
            });
    emrsClient.cancelJobRunCalled(3);
    emrsClient.getJobRunResultCalled(3);
    emrsClient.startJobRunCalled(0);
    Assertions.assertEquals(
        0L,
        Metrics.getInstance()
            .getNumericalMetric(MetricName.STREAMING_JOB_HOUSEKEEPER_TASK_FAILURE_COUNT)
            .getValue());
  }

  @Test
  @SneakyThrows
  public void testStreamingJobHouseKeeperWhenDataSourceIsNeitherDisabledNorDeleted() {
    MockFlintIndex SKIPPING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_skipping_index",
            FlintIndexType.SKIPPING,
            "ALTER SKIPPING INDEX ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\")");
    MockFlintIndex COVERING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_covering_index",
            FlintIndexType.COVERING,
            "ALTER INDEX covering ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\")");
    MockFlintIndex MV =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_mv",
            FlintIndexType.MATERIALIZED_VIEW,
            "ALTER MATERIALIZED VIEW my_glue.mydb.mv WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\") ");
    Map<MockFlintIndex, MockFlintSparkJob> indexJobMapping = new HashMap<>();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              INDEX.createIndex();
              MockFlintSparkJob flintIndexJob =
                  new MockFlintSparkJob(stateStore, INDEX.getLatestId(), MYGLUE_DATASOURCE);
              indexJobMapping.put(INDEX, flintIndexJob);
              HashMap<String, Object> existingOptions = new HashMap<>();
              existingOptions.put("auto_refresh", "true");
              // Making Index Auto Refresh
              INDEX.updateIndexOptions(existingOptions, false);
              flintIndexJob.refreshing();
            });
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
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    FlintStreamingJobHouseKeeperTask flintStreamingJobHouseKeeperTask =
        new FlintStreamingJobHouseKeeperTask(
            dataSourceService, flintIndexMetadataService, stateStore, emrServerlessClientFactory);
    Thread thread = new Thread(flintStreamingJobHouseKeeperTask);
    thread.start();
    thread.join();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              MockFlintSparkJob flintIndexJob = indexJobMapping.get(INDEX);
              flintIndexJob.assertState(FlintIndexState.REFRESHING);
              Map<String, Object> mappings = INDEX.getIndexMappings();
              Map<String, Object> meta = (HashMap<String, Object>) mappings.get("_meta");
              Map<String, Object> options = (Map<String, Object>) meta.get("options");
              Assertions.assertEquals("true", options.get("auto_refresh"));
            });
    emrsClient.cancelJobRunCalled(0);
    emrsClient.getJobRunResultCalled(0);
    emrsClient.startJobRunCalled(0);
    Assertions.assertEquals(
        0L,
        Metrics.getInstance()
            .getNumericalMetric(MetricName.STREAMING_JOB_HOUSEKEEPER_TASK_FAILURE_COUNT)
            .getValue());
  }

  @Test
  public void testStreamingJobHouseKeeperWhenS3GlueIsDisabledButNotStreamingJobQueries()
      throws InterruptedException {
    changeDataSourceStatus(MYGLUE_DATASOURCE, DISABLED);
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
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    FlintStreamingJobHouseKeeperTask flintStreamingJobHouseKeeperTask =
        new FlintStreamingJobHouseKeeperTask(
            dataSourceService, flintIndexMetadataService, stateStore, emrServerlessClientFactory);
    Thread thread = new Thread(flintStreamingJobHouseKeeperTask);
    thread.start();
    thread.join();
    emrsClient.getJobRunResultCalled(0);
    emrsClient.startJobRunCalled(0);
    emrsClient.cancelJobRunCalled(0);
    Assertions.assertEquals(
        0L,
        Metrics.getInstance()
            .getNumericalMetric(MetricName.STREAMING_JOB_HOUSEKEEPER_TASK_FAILURE_COUNT)
            .getValue());
  }

  @Test
  public void testStreamingJobHouseKeeperWhenFlintIndexIsCorrupted() throws InterruptedException {
    String indexName = "flint_my_glue_mydb_http_logs_covering_error_index";
    MockFlintIndex mockFlintIndex =
        new MockFlintIndex(client(), indexName, FlintIndexType.COVERING, null);
    mockFlintIndex.createIndex();
    changeDataSourceStatus(MYGLUE_DATASOURCE, DISABLED);
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
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    FlintStreamingJobHouseKeeperTask flintStreamingJobHouseKeeperTask =
        new FlintStreamingJobHouseKeeperTask(
            dataSourceService, flintIndexMetadataService, stateStore, emrServerlessClientFactory);
    Thread thread = new Thread(flintStreamingJobHouseKeeperTask);
    thread.start();
    thread.join();
    emrsClient.getJobRunResultCalled(0);
    emrsClient.startJobRunCalled(0);
    emrsClient.cancelJobRunCalled(0);
    Assertions.assertEquals(
        1L,
        Metrics.getInstance()
            .getNumericalMetric(MetricName.STREAMING_JOB_HOUSEKEEPER_TASK_FAILURE_COUNT)
            .getValue());
  }

  @SneakyThrows
  @Test
  public void testErrorScenario() {
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
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    FlintIndexMetadataService flintIndexMetadataService =
        new FlintIndexMetadataService() {
          @Override
          public Map<String, FlintIndexMetadata> getFlintIndexMetadata(String indexPattern) {
            throw new RuntimeException("Couldn't fetch details from ElasticSearch");
          }

          @Override
          public void updateIndexToManualRefresh(
              String indexName, FlintIndexOptions flintIndexOptions) {}
        };
    FlintStreamingJobHouseKeeperTask flintStreamingJobHouseKeeperTask =
        new FlintStreamingJobHouseKeeperTask(
            dataSourceService, flintIndexMetadataService, stateStore, emrServerlessClientFactory);
    Thread thread = new Thread(flintStreamingJobHouseKeeperTask);
    thread.start();
    thread.join();
    Assertions.assertFalse(FlintStreamingJobHouseKeeperTask.isRunning.get());
    emrsClient.getJobRunResultCalled(0);
    emrsClient.startJobRunCalled(0);
    emrsClient.cancelJobRunCalled(0);
    Assertions.assertEquals(
        1L,
        Metrics.getInstance()
            .getNumericalMetric(MetricName.STREAMING_JOB_HOUSEKEEPER_TASK_FAILURE_COUNT)
            .getValue());
  }

  @Test
  @SneakyThrows
  public void testStreamingJobHouseKeeperMultipleTimesWhenDataSourceDisabled() {
    MockFlintIndex SKIPPING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_skipping_index",
            FlintIndexType.SKIPPING,
            "ALTER SKIPPING INDEX ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\")");
    MockFlintIndex COVERING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_covering_index",
            FlintIndexType.COVERING,
            "ALTER INDEX covering ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\")");
    MockFlintIndex MV =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_mv",
            FlintIndexType.MATERIALIZED_VIEW,
            "ALTER MATERIALIZED VIEW my_glue.mydb.mv WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\") ");
    Map<MockFlintIndex, MockFlintSparkJob> indexJobMapping = new HashMap<>();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              INDEX.createIndex();
              MockFlintSparkJob flintIndexJob =
                  new MockFlintSparkJob(stateStore, INDEX.getLatestId(), MYGLUE_DATASOURCE);
              indexJobMapping.put(INDEX, flintIndexJob);
              HashMap<String, Object> existingOptions = new HashMap<>();
              existingOptions.put("auto_refresh", "true");
              // Making Index Auto Refresh
              INDEX.updateIndexOptions(existingOptions, false);
              flintIndexJob.refreshing();
            });
    changeDataSourceStatus(MYGLUE_DATASOURCE, DISABLED);
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
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    FlintStreamingJobHouseKeeperTask flintStreamingJobHouseKeeperTask =
        new FlintStreamingJobHouseKeeperTask(
            dataSourceService, flintIndexMetadataService, stateStore, emrServerlessClientFactory);
    Thread thread = new Thread(flintStreamingJobHouseKeeperTask);
    thread.start();
    thread.join();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              MockFlintSparkJob flintIndexJob = indexJobMapping.get(INDEX);
              flintIndexJob.assertState(FlintIndexState.ACTIVE);
              Map<String, Object> mappings = INDEX.getIndexMappings();
              Map<String, Object> meta = (HashMap<String, Object>) mappings.get("_meta");
              Map<String, Object> options = (Map<String, Object>) meta.get("options");
              Assertions.assertEquals("false", options.get("auto_refresh"));
            });
    emrsClient.cancelJobRunCalled(3);
    emrsClient.getJobRunResultCalled(3);
    emrsClient.startJobRunCalled(0);
    Assertions.assertEquals(
        0L,
        Metrics.getInstance()
            .getNumericalMetric(MetricName.STREAMING_JOB_HOUSEKEEPER_TASK_FAILURE_COUNT)
            .getValue());

    // Second Run
    Thread thread2 = new Thread(flintStreamingJobHouseKeeperTask);
    thread2.start();
    thread2.join();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              MockFlintSparkJob flintIndexJob = indexJobMapping.get(INDEX);
              flintIndexJob.assertState(FlintIndexState.ACTIVE);
              Map<String, Object> mappings = INDEX.getIndexMappings();
              Map<String, Object> meta = (HashMap<String, Object>) mappings.get("_meta");
              Map<String, Object> options = (Map<String, Object>) meta.get("options");
              Assertions.assertEquals("false", options.get("auto_refresh"));
            });

    // No New Calls and Errors
    emrsClient.cancelJobRunCalled(3);
    emrsClient.getJobRunResultCalled(3);
    emrsClient.startJobRunCalled(0);
    Assertions.assertEquals(
        0L,
        Metrics.getInstance()
            .getNumericalMetric(MetricName.STREAMING_JOB_HOUSEKEEPER_TASK_FAILURE_COUNT)
            .getValue());
  }

  @SneakyThrows
  @Test
  public void testRunStreamingJobHouseKeeperWhenDataSourceIsDeleted() {
    MockFlintIndex SKIPPING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_skipping_index",
            FlintIndexType.SKIPPING,
            "ALTER SKIPPING INDEX ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\")");
    MockFlintIndex COVERING =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_http_logs_covering_index",
            FlintIndexType.COVERING,
            "ALTER INDEX covering ON my_glue.mydb.http_logs WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\")");
    MockFlintIndex MV =
        new MockFlintIndex(
            client,
            "flint_my_glue_mydb_mv",
            FlintIndexType.MATERIALIZED_VIEW,
            "ALTER MATERIALIZED VIEW my_glue.mydb.mv WITH (auto_refresh=false,"
                + " incremental_refresh=true, output_mode=\"complete\") ");
    Map<MockFlintIndex, MockFlintSparkJob> indexJobMapping = new HashMap<>();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              INDEX.createIndex();
              MockFlintSparkJob flintIndexJob =
                  new MockFlintSparkJob(stateStore, INDEX.getLatestId(), MYGLUE_DATASOURCE);
              indexJobMapping.put(INDEX, flintIndexJob);
              HashMap<String, Object> existingOptions = new HashMap<>();
              existingOptions.put("auto_refresh", "true");
              // Making Index Auto Refresh
              INDEX.updateIndexOptions(existingOptions, false);
              flintIndexJob.refreshing();
            });
    this.dataSourceService.deleteDataSource(MYGLUE_DATASOURCE);
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
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    FlintStreamingJobHouseKeeperTask flintStreamingJobHouseKeeperTask =
        new FlintStreamingJobHouseKeeperTask(
            dataSourceService, flintIndexMetadataService, stateStore, emrServerlessClientFactory);
    Thread thread = new Thread(flintStreamingJobHouseKeeperTask);
    thread.start();
    thread.join();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              MockFlintSparkJob flintIndexJob = indexJobMapping.get(INDEX);
              flintIndexJob.assertState(FlintIndexState.DELETED);
              Map<String, Object> mappings = INDEX.getIndexMappings();
              Map<String, Object> meta = (HashMap<String, Object>) mappings.get("_meta");
              Map<String, Object> options = (Map<String, Object>) meta.get("options");
              Assertions.assertEquals("true", options.get("auto_refresh"));
            });
    emrsClient.cancelJobRunCalled(3);
    emrsClient.getJobRunResultCalled(3);
    emrsClient.startJobRunCalled(0);
    Assertions.assertEquals(
        0L,
        Metrics.getInstance()
            .getNumericalMetric(MetricName.STREAMING_JOB_HOUSEKEEPER_TASK_FAILURE_COUNT)
            .getValue());

    // Second Run
    Thread thread2 = new Thread(flintStreamingJobHouseKeeperTask);
    thread2.start();
    thread2.join();
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            INDEX -> {
              MockFlintSparkJob flintIndexJob = indexJobMapping.get(INDEX);
              flintIndexJob.assertState(FlintIndexState.DELETED);
              Map<String, Object> mappings = INDEX.getIndexMappings();
              Map<String, Object> meta = (HashMap<String, Object>) mappings.get("_meta");
              Map<String, Object> options = (Map<String, Object>) meta.get("options");
              Assertions.assertEquals("true", options.get("auto_refresh"));
            });
    // No New Calls and Errors
    emrsClient.cancelJobRunCalled(3);
    emrsClient.getJobRunResultCalled(3);
    emrsClient.startJobRunCalled(0);
    Assertions.assertEquals(
        0L,
        Metrics.getInstance()
            .getNumericalMetric(MetricName.STREAMING_JOB_HOUSEKEEPER_TASK_FAILURE_COUNT)
            .getValue());
  }

  private void changeDataSourceStatus(String dataSourceName, DataSourceStatus dataSourceStatus) {
    HashMap<String, Object> datasourceMap = new HashMap<>();
    datasourceMap.put("name", dataSourceName);
    datasourceMap.put("status", dataSourceStatus);
    this.dataSourceService.patchDataSource(datasourceMap);
  }
}
