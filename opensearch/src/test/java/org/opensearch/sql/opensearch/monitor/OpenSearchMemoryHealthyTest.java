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

  @Mock private OpenSearchMemoryHealthy.FastFail fastFail;

  @Mock private OpenSearchMemoryHealthy.MemoryUsage memoryUsage;

  private OpenSearchMemoryHealthy monitor;

  @BeforeEach
  public void setup() {
    monitor = new OpenSearchMemoryHealthy(fastFail, memoryUsage);
  }

  @Test
  void isMemoryHealthy() {
    when(memoryUsage.usage()).thenReturn(10L);

    assertTrue(monitor.isMemoryHealthy(11L));
  }

  @Test
  void memoryUsageExceedLimitFastFailure() {
    when(memoryUsage.usage()).thenReturn(10L);
    when(fastFail.shouldFail()).thenReturn(true);

    assertThrows(
        OpenSearchMemoryHealthy.MemoryUsageExceedFastFailureException.class,
        () -> monitor.isMemoryHealthy(9L));
  }

  @Test
  void memoryUsageExceedLimitWithoutFastFailure() {
    when(memoryUsage.usage()).thenReturn(10L);
    when(fastFail.shouldFail()).thenReturn(false);

    assertThrows(
        OpenSearchMemoryHealthy.MemoryUsageExceedException.class,
        () -> monitor.isMemoryHealthy(9L));
  }

  @Test
  void constructOpenSearchMemoryMonitorWithoutArguments() {
    OpenSearchMemoryHealthy monitor = new OpenSearchMemoryHealthy();
    assertNotNull(monitor);
  }

  @Test
  void randomFail() {
    OpenSearchMemoryHealthy.FastFail fastFail = new OpenSearchMemoryHealthy.FastFail();
    assertNotNull(fastFail.shouldFail());
  }

  @Test
  void setMemoryUsage() {
    OpenSearchMemoryHealthy.MemoryUsage usage = new OpenSearchMemoryHealthy.MemoryUsage();
    assertTrue(usage.usage() > 0);
  }
}
