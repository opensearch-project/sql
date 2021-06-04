/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.executor.protector.ResourceMonitorPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

@ExtendWith(MockitoExtension.class)
class ResourceMonitorPlanTest {
  @Mock
  private PhysicalPlan plan;

  @Mock
  private ResourceMonitor resourceMonitor;

  @Mock
  private PhysicalPlanNodeVisitor visitor;

  @Mock
  private Object context;

  private ResourceMonitorPlan monitorPlan;

  @BeforeEach
  public void setup() {
    monitorPlan = new ResourceMonitorPlan(plan, resourceMonitor);
  }

  @Test
  void openExceedResourceLimit() {
    when(resourceMonitor.isHealthy()).thenReturn(false);

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> monitorPlan.open());
    assertEquals("resource is not enough to run the query, quit.", exception.getMessage());
  }

  @Test
  void openSuccess() {
    when(resourceMonitor.isHealthy()).thenReturn(true);

    monitorPlan.open();
    verify(plan, times(1)).open();
  }

  @Test
  void nextSuccess() {
    when(resourceMonitor.isHealthy()).thenReturn(true);

    for (int i = 1; i <= 1000; i++) {
      monitorPlan.next();
    }
    verify(resourceMonitor, times(1)).isHealthy();
    verify(plan, times(1000)).next();
  }

  @Test
  void nextExceedResourceLimit() {
    when(resourceMonitor.isHealthy()).thenReturn(false);

    for (int i = 1; i < 1000; i++) {
      monitorPlan.next();
    }

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> monitorPlan.next());
    assertEquals("resource is not enough to load next row, quit.", exception.getMessage());
  }

  @Test
  void hasNextSuccess() {
    monitorPlan.hasNext();
    verify(plan, times(1)).hasNext();
  }

  @Test
  void closeSuccess() {
    monitorPlan.close();
    verify(plan, times(1)).close();
  }

  @Test
  void getChildSuccess() {
    monitorPlan.getChild();
    verify(plan, times(1)).getChild();
  }

  @Test
  void acceptSuccess() {
    monitorPlan.accept(visitor, context);
    verify(plan, times(1)).accept(visitor, context);
  }
}
