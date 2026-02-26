/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import net.jqwik.api.*;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.StringLength;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ListSqlOperatorTable;

/**
 * Property-based tests for {@link DialectRegistry}. Validates: Requirements 2.1, 2.2, 2.3
 *
 * <p>Uses jqwik for property-based testing with a minimum of 100 iterations per property.
 */
class DialectRegistryPropertyTest {

  /**
   * Property 1: Dialect registry round-trip — For any dialect name and valid DialectPlugin
   * implementation, registering the plugin and then resolving by that name SHALL return the same
   * plugin instance.
   *
   * <p>Validates: Requirements 2.1, 2.2
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 1: Dialect registry round-trip")
  void registeredDialectCanBeResolvedByName(
      @ForAll @AlphaChars @StringLength(min = 1, max = 50) String dialectName) {
    DialectRegistry registry = new DialectRegistry();
    DialectPlugin plugin = stubPlugin(dialectName);

    registry.register(plugin);

    Optional<DialectPlugin> resolved = registry.resolve(dialectName);
    assertTrue(resolved.isPresent(), "Registered dialect should be resolvable");
    assertSame(plugin, resolved.get(), "Resolved plugin should be the same instance");
  }

  /**
   * Property 2: Duplicate registration rejection — For any dialect name that is already registered,
   * attempting to register another plugin with the same name SHALL raise an error, and the original
   * plugin SHALL remain unchanged.
   *
   * <p>Validates: Requirements 2.3
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 2: Duplicate registration rejection")
  void duplicateRegistrationThrowsAndPreservesOriginal(
      @ForAll @AlphaChars @StringLength(min = 1, max = 50) String dialectName) {
    DialectRegistry registry = new DialectRegistry();
    DialectPlugin original = stubPlugin(dialectName);
    DialectPlugin duplicate = stubPlugin(dialectName);

    registry.register(original);

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> registry.register(duplicate));
    assertTrue(
        ex.getMessage().contains(dialectName),
        "Error message should contain the dialect name: " + ex.getMessage());

    Optional<DialectPlugin> resolved = registry.resolve(dialectName);
    assertTrue(resolved.isPresent(), "Original dialect should still be resolvable");
    assertSame(
        original, resolved.get(), "Original plugin should remain unchanged after failed register");
  }

  /** Creates a minimal stub DialectPlugin with the given dialect name. */
  private static DialectPlugin stubPlugin(String name) {
    return new DialectPlugin() {
      @Override
      public String dialectName() {
        return name;
      }

      @Override
      public QueryPreprocessor preprocessor() {
        return query -> query;
      }

      @Override
      public SqlParser.Config parserConfig() {
        return SqlParser.config();
      }

      @Override
      public SqlOperatorTable operatorTable() {
        return new ListSqlOperatorTable();
      }

      @Override
      public SqlDialect sqlDialect() {
        return SqlDialect.DatabaseProduct.UNKNOWN.getDialect();
      }
    };
  }
}
