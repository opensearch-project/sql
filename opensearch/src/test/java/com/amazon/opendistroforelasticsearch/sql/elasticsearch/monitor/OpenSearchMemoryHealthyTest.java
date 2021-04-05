/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.monitor;

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

  @Mock
  private OpenSearchMemoryHealthy.RandomFail randomFail;

  @Mock
  private OpenSearchMemoryHealthy.MemoryUsage memoryUsage;

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

    assertThrows(OpenSearchMemoryHealthy.MemoryUsageExceedFastFailureException.class,
        () -> monitor.isMemoryHealthy(9L));
  }

  @Test
  void memoryUsageExceedLimitWithoutFastFailure() {
    when(memoryUsage.usage()).thenReturn(10L);
    when(randomFail.shouldFail()).thenReturn(false);

    assertThrows(OpenSearchMemoryHealthy.MemoryUsageExceedException.class,
        () -> monitor.isMemoryHealthy(9L));
  }

  @Test
  void constructElasticsearchMemoryMonitorWithoutArguments() {
    OpenSearchMemoryHealthy monitor = new OpenSearchMemoryHealthy();
    assertNotNull(monitor);
  }

  @Test
  void randomFail() {
    OpenSearchMemoryHealthy.RandomFail randomFail = new OpenSearchMemoryHealthy.RandomFail();
    assertNotNull(randomFail.shouldFail());
  }

  @Test
  void setMemoryUsage() {
    OpenSearchMemoryHealthy.MemoryUsage usage =
        new OpenSearchMemoryHealthy.MemoryUsage();
    assertTrue(usage.usage() > 0);
  }
}