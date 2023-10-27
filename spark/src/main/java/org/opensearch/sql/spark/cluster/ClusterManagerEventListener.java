/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.cluster;

import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_REQUEST_BUFFER_INDEX_NAME;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.opensearch.client.Client;
import org.opensearch.cluster.LocalNodeClusterManagerListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;

public class ClusterManagerEventListener implements LocalNodeClusterManagerListener {

  private Cancellable flintIndexRetentionCron;
  private ClusterService clusterService;
  private ThreadPool threadPool;
  private Client client;
  private Clock clock;
  private Duration sessionTtlDuration;
  private Duration resultTtlDuration;
  private boolean isAutoIndexManagementEnabled;

  public ClusterManagerEventListener(
      ClusterService clusterService,
      ThreadPool threadPool,
      Client client,
      Clock clock,
      Setting<TimeValue> sessionTtl,
      Setting<TimeValue> resultTtl,
      Setting<Boolean> isAutoIndexManagementEnabledSetting,
      Settings settings) {
    this.clusterService = clusterService;
    this.threadPool = threadPool;
    this.client = client;
    this.clusterService.addLocalNodeClusterManagerListener(this);
    this.clock = clock;

    this.sessionTtlDuration = toDuration(sessionTtl.get(settings));
    this.resultTtlDuration = toDuration(resultTtl.get(settings));

    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            sessionTtl,
            it -> {
              this.sessionTtlDuration = toDuration(it);
              cancel(flintIndexRetentionCron);
              reInitializeFlintIndexRetention();
            });

    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            resultTtl,
            it -> {
              this.resultTtlDuration = toDuration(it);
              cancel(flintIndexRetentionCron);
              reInitializeFlintIndexRetention();
            });

    isAutoIndexManagementEnabled = isAutoIndexManagementEnabledSetting.get(settings);
    clusterService
        .getClusterSettings()
        .addSettingsUpdateConsumer(
            isAutoIndexManagementEnabledSetting,
            it -> {
              if (isAutoIndexManagementEnabled != it) {
                this.isAutoIndexManagementEnabled = it;
                if (it) {
                  onClusterManager();
                } else {
                  offClusterManager();
                }
              }
            });
  }

  @Override
  public void onClusterManager() {

    if (isAutoIndexManagementEnabled && flintIndexRetentionCron == null) {
      reInitializeFlintIndexRetention();

      clusterService.addLifecycleListener(
          new LifecycleListener() {
            @Override
            public void beforeStop() {
              cancel(flintIndexRetentionCron);
              flintIndexRetentionCron = null;
            }
          });
    }
  }

  private void reInitializeFlintIndexRetention() {
    IndexCleanup indexCleanup = new IndexCleanup(client, clusterService);
    flintIndexRetentionCron =
        threadPool.scheduleWithFixedDelay(
            new FlintIndexRetention(
                sessionTtlDuration,
                resultTtlDuration,
                clock,
                indexCleanup,
                SPARK_REQUEST_BUFFER_INDEX_NAME + "*",
                DataSourceMetadata.DEFAULT_RESULT_INDEX + "*"),
            TimeValue.timeValueHours(24),
            executorName());
  }

  @Override
  public void offClusterManager() {
    cancel(flintIndexRetentionCron);
    flintIndexRetentionCron = null;
  }

  private void cancel(Cancellable cron) {
    if (cron != null) {
      cron.cancel();
    }
  }

  @VisibleForTesting
  public List<Cancellable> getFlintIndexRetentionCron() {
    return Arrays.asList(flintIndexRetentionCron);
  }

  private String executorName() {
    return ThreadPool.Names.GENERIC;
  }

  public static Duration toDuration(TimeValue timeValue) {
    return Duration.ofMillis(timeValue.millis());
  }
}
