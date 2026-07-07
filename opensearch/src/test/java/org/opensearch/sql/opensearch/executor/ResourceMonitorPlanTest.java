/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.opensearch.executor.protector.ResourceMonitorPlan;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

@ExtendWith(MockitoExtension.class)
class ResourceMonitorPlanTest {
  @Mock private PhysicalPlan plan;

  @Mock private ResourceMonitor resourceMonitor;

  @Mock private PhysicalPlanNodeVisitor visitor;

  @Mock private Object context;

  private ResourceMonitorPlan monitorPlan;

  @BeforeEach
  public void setup() {
    monitorPlan = new ResourceMonitorPlan(plan, resourceMonitor);
  }

  @Test
  void openExceedResourceLimit() {
    when(resourceMonitor.getStatus())
        .thenReturn(
            org.opensearch.sql.monitor.ResourceStatus.builder()
                .healthy(false)
                .type(org.opensearch.sql.monitor.ResourceStatus.ResourceType.MEMORY)
                .currentUsage(900L * 1024 * 1024) // 900MB
                .maxLimit(850L * 1024 * 1024) // 850MB
                .description("Memory usage exceeds limit")
                .build());

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> monitorPlan.open());
    assertTrue(
        exception.getMessage().contains("Insufficient resources to start query"),
        "Expected enriched error message, got: " + exception.getMessage());
    assertTrue(
        exception.getMessage().contains("plugins.query.memory_limit"),
        "Expected config suggestion in message");
  }

  @Test
  void openSuccess() {
    when(resourceMonitor.getStatus())
        .thenReturn(
            org.opensearch.sql.monitor.ResourceStatus.healthy(
                org.opensearch.sql.monitor.ResourceStatus.ResourceType.MEMORY));

    monitorPlan.open();
    verify(plan, times(1)).open();
  }

  @Test
  void nextSuccess() {
    when(resourceMonitor.getStatus())
        .thenReturn(
            org.opensearch.sql.monitor.ResourceStatus.healthy(
                org.opensearch.sql.monitor.ResourceStatus.ResourceType.MEMORY));

    for (int i = 1; i <= 1000; i++) {
      monitorPlan.next();
    }
    verify(resourceMonitor, times(1)).getStatus();
    verify(plan, times(1000)).next();
  }

  @Test
  void nextExceedResourceLimit() {
    when(resourceMonitor.getStatus())
        .thenReturn(
            org.opensearch.sql.monitor.ResourceStatus.builder()
                .healthy(false)
                .type(org.opensearch.sql.monitor.ResourceStatus.ResourceType.MEMORY)
                .currentUsage(900L * 1024 * 1024) // 900MB
                .maxLimit(850L * 1024 * 1024) // 850MB
                .description("Memory usage exceeds limit")
                .build());

    for (int i = 1; i < 1000; i++) {
      monitorPlan.next();
    }

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> monitorPlan.next());
    assertTrue(
        exception.getMessage().contains("Insufficient resources to continue processing"),
        "Expected enriched error message, got: " + exception.getMessage());
    assertTrue(
        exception.getMessage().contains("Rows processed: 1000"), "Expected row count in message");
    assertTrue(
        exception.getMessage().contains("plugins.query.memory_limit"),
        "Expected config suggestion in message");
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

  @Test
  void getPlanForSerialization() {
    plan = mock(PhysicalPlan.class, withSettings().extraInterfaces(SerializablePlan.class));
    monitorPlan = new ResourceMonitorPlan(plan, resourceMonitor);
    assertEquals(plan, monitorPlan.getPlanForSerialization());
  }

  @Test
  void notSerializable() {
    // ResourceMonitorPlan shouldn't be serialized, attempt should throw an exception
    assertThrows(UnsupportedOperationException.class, () -> monitorPlan.writeExternal(null));
    assertThrows(UnsupportedOperationException.class, () -> monitorPlan.readExternal(null));
  }
}
