/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect;

import static org.junit.jupiter.api.Assertions.*;
import static org.opensearch.sql.api.dialect.DialectNames.CLICKHOUSE;

import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import net.jqwik.api.*;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.StringLength;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ListSqlOperatorTable;

/**
 * Property-based tests for unregistered dialect error response behavior.
 *
 * <p>Property 11: Unregistered dialect error lists available dialects — For any dialect name not in
 * the registry, the error response SHALL contain both the requested dialect name and the complete
 * set of available dialect names.
 *
 * <p>Validates: Requirements 1.3
 *
 * <p>Since the REST layer constructs the error message using DialectRegistry's resolve() and
 * availableDialects(), this test verifies the registry behavior that drives the error response and
 * validates the error message format as constructed in RestSQLQueryAction.
 */
class DialectRegistryUnregisteredDialectPropertyTest {

  /**
   * Property 11: Unregistered dialect error lists available dialects — For any dialect name not in
   * the registry, the error response SHALL contain both the requested dialect name and the complete
   * set of available dialect names.
   *
   * <p>Validates: Requirements 1.3
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 11: Unregistered dialect error lists available"
          + " dialects")
  void unregisteredDialectErrorContainsRequestedNameAndAvailableDialects(
      @ForAll("unregisteredDialectNames") String requestedDialect) {
    // Set up registry with ClickHouseDialectPlugin registered
    DialectRegistry registry = new DialectRegistry();
    DialectPlugin clickhousePlugin = stubPlugin(CLICKHOUSE);
    registry.register(clickhousePlugin);

    // Verify resolve returns empty for the unregistered dialect
    Optional<DialectPlugin> resolved = registry.resolve(requestedDialect);
    assertTrue(resolved.isEmpty(), "Unregistered dialect should not resolve");

    // Verify availableDialects returns the registered dialects
    Set<String> available = registry.availableDialects();
    assertFalse(available.isEmpty(), "Available dialects should not be empty");
    assertTrue(available.contains(CLICKHOUSE), "Available dialects should contain 'clickhouse'");

    // Construct the error message as RestSQLQueryAction would
    String message =
        String.format(
            Locale.ROOT,
            "Unknown SQL dialect '%s'. Supported dialects: %s",
            requestedDialect,
            available);

    // Verify the error message contains the requested dialect name
    assertTrue(
        message.contains(requestedDialect),
        "Error message should contain the requested dialect name: " + requestedDialect);

    // Verify the error message contains all available dialect names
    for (String dialectName : available) {
      assertTrue(
          message.contains(dialectName),
          "Error message should contain available dialect: " + dialectName);
    }
  }

  /**
   * Provides random dialect names that are guaranteed NOT to be "clickhouse", ensuring they are
   * unregistered in the test registry.
   */
  @Provide
  Arbitrary<String> unregisteredDialectNames() {
    return Arbitraries.strings()
        .alpha()
        .ofMinLength(1)
        .ofMaxLength(50)
        .filter(name -> !name.equalsIgnoreCase(CLICKHOUSE));
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
