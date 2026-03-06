/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect;

/**
 * Central constants for dialect names. Avoids scattered string literals across the codebase. All
 * dialect name strings used in registration, routing, and error messages should reference constants
 * from this class.
 */
public final class DialectNames {

  /** The ClickHouse SQL dialect name used in the {@code ?dialect=clickhouse} query parameter. */
  public static final String CLICKHOUSE = "clickhouse";

  private DialectNames() {}
}
