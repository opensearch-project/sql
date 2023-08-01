/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.monitor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.sql.common.setting.Settings;

@ExtendWith(MockitoExtension.class)
class OpenSearchResourceMonitorTest {

  @Mock
  private Settings settings;

  @Mock
  private OpenSearchMemoryHealthy memoryMonitor;

  @BeforeEach
  public void setup() {
    when(settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT))
        .thenReturn(new ByteSizeValue(10L));
  }

  @Test
  void isHealthy() {
    when(memoryMonitor.isMemoryHealthy(anyLong())).thenReturn(true);

    OpenSearchResourceMonitor resourceMonitor =
        new OpenSearchResourceMonitor(settings, memoryMonitor);
    assertTrue(resourceMonitor.isHealthy());
  }

  @Test
  void notHealthyFastFailure() {
    when(memoryMonitor.isMemoryHealthy(anyLong())).thenThrow(
        OpenSearchMemoryHealthy.MemoryUsageExceedFastFailureException.class);

    OpenSearchResourceMonitor resourceMonitor =
        new OpenSearchResourceMonitor(settings, memoryMonitor);
    assertFalse(resourceMonitor.isHealthy());
    verify(memoryMonitor, times(1)).isMemoryHealthy(anyLong());
  }

  @Test
  void notHealthyWithRetry() {
    when(memoryMonitor.isMemoryHealthy(anyLong())).thenThrow(
        OpenSearchMemoryHealthy.MemoryUsageExceedException.class);

    OpenSearchResourceMonitor resourceMonitor =
        new OpenSearchResourceMonitor(settings, memoryMonitor);
    assertFalse(resourceMonitor.isHealthy());
    verify(memoryMonitor, times(3)).isMemoryHealthy(anyLong());
  }

  @Test
  void healthyWithRetry() {

    when(memoryMonitor.isMemoryHealthy(anyLong())).thenThrow(
        OpenSearchMemoryHealthy.MemoryUsageExceedException.class).thenReturn(true);

    OpenSearchResourceMonitor resourceMonitor =
        new OpenSearchResourceMonitor(settings, memoryMonitor);
    assertTrue(resourceMonitor.isHealthy());
    verify(memoryMonitor, times(2)).isMemoryHealthy(anyLong());
  }
}
