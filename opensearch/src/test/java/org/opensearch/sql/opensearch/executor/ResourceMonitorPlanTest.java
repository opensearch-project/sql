/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
