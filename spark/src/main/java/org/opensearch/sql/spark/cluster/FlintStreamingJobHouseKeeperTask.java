/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.cluster;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceStatus;
import org.opensearch.sql.datasources.exceptions.DataSourceNotFoundException;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.flint.FlintIndexStateModelService;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpAlter;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpDrop;

/** Cleaner task which alters the active streaming jobs of a disabled datasource. */
@RequiredArgsConstructor
public class FlintStreamingJobHouseKeeperTask implements Runnable {

  private final DataSourceService dataSourceService;
  private final FlintIndexMetadataService flintIndexMetadataService;
  private final FlintIndexStateModelService flintIndexStateModelService;
  private final EMRServerlessClientFactory emrServerlessClientFactory;

  private static final Logger LOGGER = LogManager.getLogger(FlintStreamingJobHouseKeeperTask.class);
  protected static final AtomicBoolean isRunning = new AtomicBoolean(false);

  @Override
  public void run() {
    if (!isRunning.compareAndSet(false, true)) {
      LOGGER.info("Previous task is still running. Skipping this execution.");
      return;
    }
    try {
      LOGGER.info("Starting housekeeping task for auto refresh streaming jobs.");
      Map<String, FlintIndexMetadata> autoRefreshFlintIndicesMap = getAllAutoRefreshIndices();
      autoRefreshFlintIndicesMap.forEach(
          (autoRefreshIndex, flintIndexMetadata) -> {
            try {
              String datasourceName = getDataSourceName(flintIndexMetadata);
              try {
                DataSourceMetadata dataSourceMetadata =
                    this.dataSourceService.getDataSourceMetadata(datasourceName);
                if (dataSourceMetadata.getStatus() == DataSourceStatus.DISABLED) {
                  LOGGER.info("Datasource is disabled for autoRefreshIndex: {}", autoRefreshIndex);
                  alterAutoRefreshIndex(autoRefreshIndex, flintIndexMetadata, datasourceName);
                } else {
                  LOGGER.debug("Datasource is enabled for autoRefreshIndex : {}", autoRefreshIndex);
                }
              } catch (DataSourceNotFoundException exception) {
                LOGGER.info("Datasource is deleted for autoRefreshIndex: {}", autoRefreshIndex);
                try {
                  dropAutoRefreshIndex(autoRefreshIndex, flintIndexMetadata, datasourceName);
                } catch (IllegalStateException illegalStateException) {
                  LOGGER.debug(
                      "AutoRefresh index: {} is not in valid state for deletion.",
                      autoRefreshIndex);
                }
              }
            } catch (Exception exception) {
              LOGGER.error(
                  "Failed to alter/cancel index {}: {}",
                  autoRefreshIndex,
                  exception.getMessage(),
                  exception);
              Metrics.getInstance()
                  .getNumericalMetric(MetricName.STREAMING_JOB_HOUSEKEEPER_TASK_FAILURE_COUNT)
                  .increment();
            }
          });
      LOGGER.info("Finished housekeeping task for auto refresh streaming jobs.");
    } catch (Throwable error) {
      LOGGER.error("Error while running the streaming job cleaner task: {}", error.getMessage());
      Metrics.getInstance()
          .getNumericalMetric(MetricName.STREAMING_JOB_HOUSEKEEPER_TASK_FAILURE_COUNT)
          .increment();
    } finally {
      isRunning.set(false);
    }
  }

  private void dropAutoRefreshIndex(
      String autoRefreshIndex, FlintIndexMetadata flintIndexMetadata, String datasourceName) {
    // When the datasource is deleted. Possibly Replace with VACUUM Operation.
    LOGGER.info("Attempting to drop auto refresh index: {}", autoRefreshIndex);
    FlintIndexOpDrop flintIndexOpDrop =
        new FlintIndexOpDrop(
            flintIndexStateModelService, datasourceName, emrServerlessClientFactory.getClient());
    flintIndexOpDrop.apply(flintIndexMetadata);
    LOGGER.info("Successfully dropped index: {}", autoRefreshIndex);
  }

  private void alterAutoRefreshIndex(
      String autoRefreshIndex, FlintIndexMetadata flintIndexMetadata, String datasourceName) {
    LOGGER.info("Attempting to alter index: {}", autoRefreshIndex);
    FlintIndexOptions flintIndexOptions = new FlintIndexOptions();
    flintIndexOptions.setOption(FlintIndexOptions.AUTO_REFRESH, "false");
    FlintIndexOpAlter flintIndexOpAlter =
        new FlintIndexOpAlter(
            flintIndexOptions,
            flintIndexStateModelService,
            datasourceName,
            emrServerlessClientFactory.getClient(),
            flintIndexMetadataService);
    flintIndexOpAlter.apply(flintIndexMetadata);
    LOGGER.info("Successfully altered index: {}", autoRefreshIndex);
  }

  private String getDataSourceName(FlintIndexMetadata flintIndexMetadata) {
    String kind = flintIndexMetadata.getKind();
    switch (kind) {
      case "mv":
        return flintIndexMetadata.getName().split("\\.")[0];
      case "skipping":
      case "covering":
        return flintIndexMetadata.getSource().split("\\.")[0];
      default:
        throw new IllegalArgumentException(String.format("Unknown flint index kind: %s", kind));
    }
  }

  private Map<String, FlintIndexMetadata> getAllAutoRefreshIndices() {
    Map<String, FlintIndexMetadata> flintIndexMetadataHashMap =
        flintIndexMetadataService.getFlintIndexMetadata("flint_*");
    return flintIndexMetadataHashMap.entrySet().stream()
        .filter(entry -> entry.getValue().getFlintIndexOptions().autoRefresh())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
