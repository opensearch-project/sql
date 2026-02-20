/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.monitor.ResourceStatus.ResourceType;

class ResourceStatusTest {

  @Test
  void testHealthyFactoryMethod() {
    // Test healthy() for each ResourceType
    ResourceStatus memoryStatus = ResourceStatus.healthy(ResourceType.MEMORY);
    assertTrue(memoryStatus.isHealthy());
    assertEquals(ResourceType.MEMORY, memoryStatus.getType());
    assertEquals("MEMORY resources are healthy", memoryStatus.getDescription());

    ResourceStatus cpuStatus = ResourceStatus.healthy(ResourceType.CPU);
    assertTrue(cpuStatus.isHealthy());
    assertEquals(ResourceType.CPU, cpuStatus.getType());
    assertEquals("CPU resources are healthy", cpuStatus.getDescription());

    ResourceStatus diskStatus = ResourceStatus.healthy(ResourceType.DISK);
    assertTrue(diskStatus.isHealthy());
    assertEquals(ResourceType.DISK, diskStatus.getType());
    assertEquals("DISK resources are healthy", diskStatus.getDescription());

    ResourceStatus otherStatus = ResourceStatus.healthy(ResourceType.OTHER);
    assertTrue(otherStatus.isHealthy());
    assertEquals(ResourceType.OTHER, otherStatus.getType());
    assertEquals("OTHER resources are healthy", otherStatus.getDescription());
  }

  @Test
  void testUnhealthyFactoryMethod() {
    // Test unhealthy() for each ResourceType
    ResourceStatus memoryStatus =
        ResourceStatus.unhealthy(ResourceType.MEMORY, 1024, 2048, "Memory limit exceeded");
    assertFalse(memoryStatus.isHealthy());
    assertEquals(ResourceType.MEMORY, memoryStatus.getType());
    assertEquals("Memory limit exceeded", memoryStatus.getDescription());
    assertEquals(1024L, memoryStatus.getCurrentUsage());
    assertEquals(2048L, memoryStatus.getMaxLimit());

    ResourceStatus cpuStatus =
        ResourceStatus.unhealthy(ResourceType.CPU, 95, 100, "CPU usage too high");
    assertFalse(cpuStatus.isHealthy());
    assertEquals(ResourceType.CPU, cpuStatus.getType());
    assertEquals("CPU usage too high", cpuStatus.getDescription());

    ResourceStatus diskStatus =
        ResourceStatus.unhealthy(ResourceType.DISK, 900, 1000, "Disk space low");
    assertFalse(diskStatus.isHealthy());
    assertEquals(ResourceType.DISK, diskStatus.getType());
    assertEquals("Disk space low", diskStatus.getDescription());

    ResourceStatus otherStatus =
        ResourceStatus.unhealthy(ResourceType.OTHER, 50, 100, "Other resource issue");
    assertFalse(otherStatus.isHealthy());
    assertEquals(ResourceType.OTHER, otherStatus.getType());
    assertEquals("Other resource issue", otherStatus.getDescription());
  }

  @Test
  void testGetFormattedDescriptionWithMetrics() {
    // Test with metrics set (using values that will format as GB)
    ResourceStatus status =
        ResourceStatus.unhealthy(
            ResourceType.MEMORY, 1536L * 1024 * 1024, 2048L * 1024 * 1024, "Memory usage high");

    String formatted = status.getFormattedDescription();
    assertNotNull(formatted);
    assertTrue(formatted.contains("Memory usage high"));
    assertTrue(formatted.contains("current:"));
    assertTrue(formatted.contains("limit:"));
    assertTrue(formatted.contains("usage:"));
    assertTrue(formatted.contains("75.0%")); // 1536/2048 = 0.75
    assertTrue(formatted.contains("1.5GB")); // 1536 MB = 1.5 GB
    assertTrue(formatted.contains("2.0GB")); // 2048 MB = 2.0 GB
  }

  @Test
  void testGetFormattedDescriptionWithoutMetrics() {
    // Test with null metrics
    ResourceStatus status =
        ResourceStatus.builder()
            .healthy(false)
            .type(ResourceType.MEMORY)
            .description("Memory issue detected")
            .build();

    String formatted = status.getFormattedDescription();
    assertEquals("Memory issue detected", formatted);
  }

  @Test
  void testByteFormattingLessThan1KB() {
    // Test < 1KB
    ResourceStatus status = ResourceStatus.unhealthy(ResourceType.MEMORY, 512, 1024, "Low memory");

    String formatted = status.getFormattedDescription();
    assertTrue(formatted.contains("512B"));
    assertTrue(formatted.contains("1.0KB"));
  }

  @Test
  void testByteFormattingExactly1KB() {
    // Test exactly 1KB
    ResourceStatus status =
        ResourceStatus.unhealthy(ResourceType.MEMORY, 1024, 2048, "Memory warning");

    String formatted = status.getFormattedDescription();
    assertTrue(formatted.contains("1.0KB"));
    assertTrue(formatted.contains("2.0KB"));
  }

  @Test
  void testByteFormattingExactly1MB() {
    // Test exactly 1MB
    long oneMB = 1024 * 1024;
    ResourceStatus status =
        ResourceStatus.unhealthy(ResourceType.MEMORY, oneMB, oneMB * 2, "Memory warning");

    String formatted = status.getFormattedDescription();
    assertTrue(formatted.contains("1.0MB"));
    assertTrue(formatted.contains("2.0MB"));
  }

  @Test
  void testByteFormattingExactly1GB() {
    // Test exactly 1GB
    long oneGB = 1024L * 1024 * 1024;
    ResourceStatus status =
        ResourceStatus.unhealthy(ResourceType.MEMORY, oneGB, oneGB * 2, "Memory warning");

    String formatted = status.getFormattedDescription();
    assertTrue(formatted.contains("1.0GB"));
    assertTrue(formatted.contains("2.0GB"));
  }

  @Test
  void testByteFormattingZeroUsage() {
    // Test with 0 usage
    ResourceStatus status = ResourceStatus.unhealthy(ResourceType.MEMORY, 0, 1024, "Zero usage");

    String formatted = status.getFormattedDescription();
    assertTrue(formatted.contains("0B"));
    assertTrue(formatted.contains("0.0%"));
  }

  @Test
  void testByteFormattingUsageEqualsLimit() {
    // Test when currentUsage == maxLimit
    ResourceStatus status = ResourceStatus.unhealthy(ResourceType.MEMORY, 2048, 2048, "At limit");

    String formatted = status.getFormattedDescription();
    assertTrue(formatted.contains("2.0KB"));
    assertTrue(formatted.contains("100.0%"));
  }

  @Test
  void testByteFormattingMultipleSizes() {
    // Test various sizes to ensure proper formatting
    // Small bytes
    ResourceStatus status1 = ResourceStatus.unhealthy(ResourceType.MEMORY, 100, 1000, "Test");
    assertTrue(status1.getFormattedDescription().contains("100B"));

    // KB range
    ResourceStatus status2 =
        ResourceStatus.unhealthy(ResourceType.MEMORY, 500 * 1024, 1000 * 1024, "Test");
    assertTrue(status2.getFormattedDescription().contains("500.0KB"));

    // MB range
    ResourceStatus status3 =
        ResourceStatus.unhealthy(
            ResourceType.MEMORY, 500L * 1024 * 1024, 1000L * 1024 * 1024, "Test");
    assertTrue(status3.getFormattedDescription().contains("500.0MB"));

    // GB range
    ResourceStatus status4 =
        ResourceStatus.unhealthy(
            ResourceType.MEMORY, 5L * 1024 * 1024 * 1024, 10L * 1024 * 1024 * 1024, "Test");
    assertTrue(status4.getFormattedDescription().contains("5.0GB"));
  }

  @Test
  void testMaxLimitZeroOrNegative() {
    // Test with maxLimit = 0 (should not compute percentage)
    ResourceStatus status1 =
        ResourceStatus.unhealthy(ResourceType.MEMORY, 1024, 0, "Invalid limit");
    String formatted1 = status1.getFormattedDescription();
    assertTrue(formatted1.contains("Invalid limit"));
    assertTrue(formatted1.contains("current:"));
    assertTrue(formatted1.contains("limit:"));
    assertFalse(formatted1.contains("%")); // Should not contain percentage

    // Test with maxLimit < 0 (should not compute percentage)
    ResourceStatus status2 =
        ResourceStatus.unhealthy(ResourceType.MEMORY, 1024, -100, "Negative limit");
    String formatted2 = status2.getFormattedDescription();
    assertTrue(formatted2.contains("Negative limit"));
    assertTrue(formatted2.contains("current:"));
    assertTrue(formatted2.contains("limit:"));
    assertFalse(formatted2.contains("%")); // Should not contain percentage
  }

  @Test
  void testHealthyWithNullResourceType() {
    // Test healthy() with null ResourceType
    ResourceStatus status = ResourceStatus.healthy(null);
    assertTrue(status.isHealthy());
    assertNull(status.getType());
    assertEquals("null resources are healthy", status.getDescription());
  }

  @Test
  void testUnhealthyWithNullResourceType() {
    // Test unhealthy() with null ResourceType
    ResourceStatus status = ResourceStatus.unhealthy(null, 1024, 2048, "Resource limit exceeded");
    assertFalse(status.isHealthy());
    assertNull(status.getType());
    assertEquals("Resource limit exceeded", status.getDescription());
    assertEquals(1024L, status.getCurrentUsage());
    assertEquals(2048L, status.getMaxLimit());
  }

  @Test
  void testUnhealthyWithNullDescription() {
    // Test unhealthy() with null description
    ResourceStatus status = ResourceStatus.unhealthy(ResourceType.MEMORY, 1024, 2048, null);
    assertFalse(status.isHealthy());
    assertEquals(ResourceType.MEMORY, status.getType());
    assertNull(status.getDescription());
    assertEquals(1024L, status.getCurrentUsage());
    assertEquals(2048L, status.getMaxLimit());

    // Formatted description should handle null description gracefully
    String formatted = status.getFormattedDescription();
    assertNotNull(formatted);
    assertTrue(formatted.contains("current:"));
    assertTrue(formatted.contains("limit:"));
  }
}
