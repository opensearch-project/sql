/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.opensearch.client.PluginClient;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.flint.operation.FlintIndexOpFactory;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;

class ClusterManagerEventListenerTest {

  private static final String INTERVAL =
      OpenSearchSettings.AUTO_INDEX_MANAGEMENT_INTERVAL_SETTING.getKey();

  private final ThreadPool threadPool = mock(ThreadPool.class);
  private final List<Cancellable> retentionCrons = new ArrayList<>();
  private final List<TimeValue> retentionIntervals = new ArrayList<>();
  private ClusterSettings clusterSettings;
  private ClusterManagerEventListener listener;

  @BeforeEach
  void setUp() {
    Settings settings = Settings.builder().put(INTERVAL, "2h").build();
    clusterSettings =
        new ClusterSettings(
            settings,
            Set.of(
                OpenSearchSettings.SESSION_INDEX_TTL_SETTING,
                OpenSearchSettings.RESULT_INDEX_TTL_SETTING,
                OpenSearchSettings.STREAMING_JOB_HOUSEKEEPER_INTERVAL_SETTING,
                OpenSearchSettings.AUTO_INDEX_MANAGEMENT_ENABLED_SETTING,
                OpenSearchSettings.AUTO_INDEX_MANAGEMENT_INTERVAL_SETTING));
    ClusterService clusterService = mock(ClusterService.class);
    when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    when(threadPool.scheduleWithFixedDelay(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Cancellable cron = mock(Cancellable.class);
              if (invocation.getArgument(0) instanceof FlintIndexRetention) {
                retentionCrons.add(cron);
                retentionIntervals.add(invocation.getArgument(1));
              }
              return cron;
            });

    listener =
        new ClusterManagerEventListener(
            clusterService,
            threadPool,
            mock(PluginClient.class),
            Clock.systemUTC(),
            OpenSearchSettings.SESSION_INDEX_TTL_SETTING,
            OpenSearchSettings.RESULT_INDEX_TTL_SETTING,
            OpenSearchSettings.STREAMING_JOB_HOUSEKEEPER_INTERVAL_SETTING,
            OpenSearchSettings.AUTO_INDEX_MANAGEMENT_ENABLED_SETTING,
            OpenSearchSettings.AUTO_INDEX_MANAGEMENT_INTERVAL_SETTING,
            settings,
            mock(DataSourceService.class),
            mock(FlintIndexMetadataService.class),
            mock(FlintIndexOpFactory.class));
  }

  @Test
  void schedulesTheRetentionAtTheConfiguredInterval() {
    listener.onClusterManager();

    assertEquals(List.of(TimeValue.timeValueHours(2)), retentionIntervals);
  }

  @Test
  void reschedulesTheRetentionWhenTheIntervalChanges() {
    listener.onClusterManager();

    clusterSettings.applySettings(Settings.builder().put(INTERVAL, "5s").build());

    assertEquals(
        List.of(TimeValue.timeValueHours(2), TimeValue.timeValueSeconds(5)), retentionIntervals);
    verify(retentionCrons.get(0)).cancel();
    assertEquals(List.of(retentionCrons.get(1)), listener.getFlintIndexRetentionCron());
  }

  @Test
  void doesNotStartTheRetentionOnANodeThatIsNotClusterManager() {
    clusterSettings.applySettings(Settings.builder().put(INTERVAL, "5s").build());

    assertEquals(List.of(), retentionIntervals);
  }

  @Test
  void rejectsAnIntervalBelowOneSecond() {
    Settings tooShort = Settings.builder().put(INTERVAL, "500ms").build();

    assertThrows(
        IllegalArgumentException.class,
        () -> OpenSearchSettings.AUTO_INDEX_MANAGEMENT_INTERVAL_SETTING.get(tooShort));
  }
}
