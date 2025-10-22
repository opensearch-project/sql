/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.cluster;

import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.opensearch.sql.datasource.model.DataSourceStatus;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceSpec;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.asyncquery.model.MockFlintIndex;
import org.opensearch.sql.spark.asyncquery.model.MockFlintSparkJob;
import org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.flint.FlintIndexMetadataServiceImpl;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexType;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.sql.datasource.model.DataSourceStatus.DISABLED;

public class FlintStreamingJobHouseKeeperTaskBatch2Test extends FlintStreamingJobHouseKeeperBase {
  @Test
  @SneakyThrows
  public void testStreamingJobHouseKeeperWhenDataSourceDisabled() {
    ImmutableList<MockFlintIndex> mockFlintIndices = getMockFlintIndices();
    Map<MockFlintIndex, MockFlintSparkJob> indexJobMapping = new HashMap<>();
    mockFlintIndices.forEach(
        INDEX -> {
          INDEX.createIndex();
          MockFlintSparkJob flintIndexJob =
              new MockFlintSparkJob(
                  flintIndexStateModelService, INDEX.getLatestId(), MYGLUE_DATASOURCE);
          indexJobMapping.put(INDEX, flintIndexJob);
          HashMap<String, Object> existingOptions = new HashMap<>();
          existingOptions.put("auto_refresh", "true");
          // Making Index Auto Refresh
          INDEX.updateIndexOptions(existingOptions, false);
          flintIndexJob.refreshing();
        });
    changeDataSourceStatus(MYGLUE_DATASOURCE, DISABLED);
    LocalEMRSClient emrsClient = getCancelledLocalEmrsClient();
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    FlintStreamingJobHouseKeeperTask flintStreamingJobHouseKeeperTask =
        new FlintStreamingJobHouseKeeperTask(
            dataSourceService,
            flintIndexMetadataService,
            getFlintIndexOpFactory((accountId) -> emrsClient));

    Thread thread = new Thread(flintStreamingJobHouseKeeperTask);
    thread.start();
    thread.join();

    mockFlintIndices.forEach(
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
    ImmutableList<MockFlintIndex> mockFlintIndices = getMockFlintIndices();
    Map<MockFlintIndex, MockFlintSparkJob> indexJobMapping = new HashMap<>();
    mockFlintIndices.forEach(
        INDEX -> {
          INDEX.createIndex();
          MockFlintSparkJob flintIndexJob =
              new MockFlintSparkJob(
                  flintIndexStateModelService, INDEX.getLatestId(), MYGLUE_DATASOURCE);
          indexJobMapping.put(INDEX, flintIndexJob);
          HashMap<String, Object> existingOptions = new HashMap<>();
          existingOptions.put("auto_refresh", "true");
          // Making Index Auto Refresh
          INDEX.updateIndexOptions(existingOptions, false);
          flintIndexJob.refreshing();
        });
    changeDataSourceStatus(MYGLUE_DATASOURCE, DISABLED);
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    FlintStreamingJobHouseKeeperTask flintStreamingJobHouseKeeperTask =
        new FlintStreamingJobHouseKeeperTask(
            dataSourceService,
            flintIndexMetadataService,
            getFlintIndexOpFactory((accountId) -> emrsClient));

    Thread thread = new Thread(flintStreamingJobHouseKeeperTask);
    thread.start();
    thread.join();

    mockFlintIndices.forEach(
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
    ImmutableList<MockFlintIndex> mockFlintIndices = getMockFlintIndices();
    Map<MockFlintIndex, MockFlintSparkJob> indexJobMapping = new HashMap<>();
    mockFlintIndices.forEach(
        INDEX -> {
          INDEX.createIndex();
          MockFlintSparkJob flintIndexJob =
              new MockFlintSparkJob(
                  flintIndexStateModelService, INDEX.getLatestId(), MYGLUE_DATASOURCE);
          indexJobMapping.put(INDEX, flintIndexJob);
          HashMap<String, Object> existingOptions = new HashMap<>();
          existingOptions.put("auto_refresh", "true");
          // Making Index Auto Refresh
          INDEX.updateIndexOptions(existingOptions, false);
          flintIndexJob.refreshing();
        });
    changeDataSourceStatus(MYGLUE_DATASOURCE, DISABLED);
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    FlintStreamingJobHouseKeeperTask flintStreamingJobHouseKeeperTask =
        new FlintStreamingJobHouseKeeperTask(
            dataSourceService,
            flintIndexMetadataService,
            getFlintIndexOpFactory((accountId) -> emrsClient));
    FlintStreamingJobHouseKeeperTask.isRunning.compareAndSet(false, true);

    Thread thread = new Thread(flintStreamingJobHouseKeeperTask);
    thread.start();
    thread.join();

    mockFlintIndices.forEach(
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
    ImmutableList<MockFlintIndex> mockFlintIndices = getMockFlintIndices();
    Map<MockFlintIndex, MockFlintSparkJob> indexJobMapping = new HashMap<>();
    mockFlintIndices.forEach(
        INDEX -> {
          INDEX.createIndex();
          MockFlintSparkJob flintIndexJob =
              new MockFlintSparkJob(
                  flintIndexStateModelService, INDEX.getLatestId(), MYGLUE_DATASOURCE);
          indexJobMapping.put(INDEX, flintIndexJob);
          HashMap<String, Object> existingOptions = new HashMap<>();
          existingOptions.put("auto_refresh", "true");
          // Making Index Auto Refresh
          INDEX.updateIndexOptions(existingOptions, false);
          flintIndexJob.refreshing();
        });
    this.dataSourceService.deleteDataSource(MYGLUE_DATASOURCE);
    LocalEMRSClient emrsClient = getCancelledLocalEmrsClient();
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    FlintStreamingJobHouseKeeperTask flintStreamingJobHouseKeeperTask =
        new FlintStreamingJobHouseKeeperTask(
            dataSourceService,
            flintIndexMetadataService,
            getFlintIndexOpFactory((accountId) -> emrsClient));

    Thread thread = new Thread(flintStreamingJobHouseKeeperTask);
    thread.start();
    thread.join();

    mockFlintIndices.forEach(
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
    ImmutableList<MockFlintIndex> mockFlintIndices = getMockFlintIndices();
    Map<MockFlintIndex, MockFlintSparkJob> indexJobMapping = new HashMap<>();
    mockFlintIndices.forEach(
        INDEX -> {
          INDEX.createIndex();
          MockFlintSparkJob flintIndexJob =
              new MockFlintSparkJob(
                  flintIndexStateModelService, INDEX.getLatestId(), MYGLUE_DATASOURCE);
          indexJobMapping.put(INDEX, flintIndexJob);
          HashMap<String, Object> existingOptions = new HashMap<>();
          existingOptions.put("auto_refresh", "true");
          // Making Index Auto Refresh
          INDEX.updateIndexOptions(existingOptions, false);
          flintIndexJob.refreshing();
        });
    LocalEMRSClient emrsClient = getCancelledLocalEmrsClient();
    FlintIndexMetadataService flintIndexMetadataService = new FlintIndexMetadataServiceImpl(client);
    FlintStreamingJobHouseKeeperTask flintStreamingJobHouseKeeperTask =
        new FlintStreamingJobHouseKeeperTask(
            dataSourceService,
            flintIndexMetadataService,
            getFlintIndexOpFactory((accountId) -> emrsClient));

    Thread thread = new Thread(flintStreamingJobHouseKeeperTask);
    thread.start();
    thread.join();

    mockFlintIndices.forEach(
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
}
