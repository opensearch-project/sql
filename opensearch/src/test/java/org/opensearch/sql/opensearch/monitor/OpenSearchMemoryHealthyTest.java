/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.monitor;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OpenSearchMemoryHealthyTest {

  @Mock private OpenSearchMemoryHealthy.RandomFail randomFail;

  @Mock private MemoryUsage memoryUsage;

  private OpenSearchMemoryHealthy monitor;

  @BeforeEach
  public void setup() {
    monitor = new OpenSearchMemoryHealthy(randomFail, memoryUsage);
  }

  @Test
  void isMemoryHealthy() {
    when(memoryUsage.usage()).thenReturn(10L);

    assertTrue(monitor.isMemoryHealthy(11L));
  }

  @Test
  void memoryUsageExceedLimitFastFailure() {
    when(memoryUsage.usage()).thenReturn(10L);
    when(randomFail.shouldFail()).thenReturn(true);

    assertThrows(
        OpenSearchMemoryHealthy.MemoryUsageExceedFastFailureException.class,
        () -> monitor.isMemoryHealthy(9L));
  }

  @Test
  void memoryUsageExceedLimitWithoutFastFailure() {
    when(memoryUsage.usage()).thenReturn(10L);
    when(randomFail.shouldFail()).thenReturn(false);

    assertThrows(
        OpenSearchMemoryHealthy.MemoryUsageExceedException.class,
        () -> monitor.isMemoryHealthy(9L));
  }

  @Test
  void constructOpenSearchMemoryMonitorWithoutArguments() {
    OpenSearchMemoryHealthy monitor = new OpenSearchMemoryHealthy(null);
    assertNotNull(monitor);
  }

  @Test
  void randomFail() {
    OpenSearchMemoryHealthy.RandomFail randomFail = new OpenSearchMemoryHealthy.RandomFail();
    assertNotNull(randomFail.shouldFail());
  }

  @Test
  void getMemoryUsage() {
    MemoryUsage usage = RuntimeMemoryUsage.getInstance();
    assertTrue(usage.usage() > 0);
  }

  @Test
  void setMemoryUsage() {
    MemoryUsage usage = RuntimeMemoryUsage.getInstance();
    assertThrows(UnsupportedOperationException.class, () -> usage.setUsage(10L));
  }
}
