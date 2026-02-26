/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.api.dialect.DialectPlugin;
import org.opensearch.sql.api.dialect.DialectRegistry;

/**
 * Unit tests verifying that {@link ClickHouseDialectPlugin} is properly registered in the {@link
 * DialectRegistry} at startup. Simulates the startup registration performed by
 * OpenSearchPluginModule.dialectRegistry().
 */
class ClickHouseDialectPluginTest {

  private DialectRegistry registry;

  @BeforeEach
  void setUp() {
    // Simulate startup registration as done in OpenSearchPluginModule.dialectRegistry()
    registry = new DialectRegistry();
    registry.register(ClickHouseDialectPlugin.INSTANCE);
  }

  @Test
  void resolveClickhouseReturnsPresent() {
    Optional<DialectPlugin> resolved = registry.resolve("clickhouse");
    assertTrue(resolved.isPresent(), "Expected 'clickhouse' dialect to be registered");
  }

  @Test
  void resolveClickhouseReturnsSamePluginInstance() {
    Optional<DialectPlugin> resolved = registry.resolve("clickhouse");
    assertTrue(resolved.isPresent());
    assertEquals(ClickHouseDialectPlugin.INSTANCE, resolved.get());
  }

  @Test
  void availableDialectsContainsClickhouse() {
    assertTrue(
        registry.availableDialects().contains("clickhouse"),
        "Available dialects should contain 'clickhouse'");
  }

  @Test
  void resolvedPluginDialectNameIsClickhouse() {
    DialectPlugin plugin = registry.resolve("clickhouse").orElseThrow();
    assertEquals("clickhouse", plugin.dialectName());
  }

  @Test
  void resolvedPluginProvidesAllComponents() {
    DialectPlugin plugin = registry.resolve("clickhouse").orElseThrow();
    assertNotNull(plugin.preprocessor(), "Preprocessor should not be null");
    assertNotNull(plugin.parserConfig(), "Parser config should not be null");
    assertNotNull(plugin.operatorTable(), "Operator table should not be null");
    assertNotNull(plugin.sqlDialect(), "SQL dialect should not be null");
  }
}
