/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

import java.util.Map;

/**
 * The single redaction hook for the {@code rest} command: given one raw response row (column name
 * to value) for an endpoint, it returns the row to emit with sensitive values masked in one pass.
 * Not a {@code loadExtensions} SPI (one central owner, not an open registration surface); the
 * default is {@link #NONE} (no-op), applied once per row at the choke point before values are
 * coerced to the schema types.
 */
@FunctionalInterface
public interface Redactor {

  /** A no-op that returns the row unchanged (the OSS default). */
  Redactor NONE = (endpoint, row) -> row;

  /**
   * Redact one raw response row.
   *
   * @param endpoint the endpoint name the row came from, a scope for endpoint-aware masking
   * @param row column name to raw value for one row (never null)
   * @return the row to emit, with sensitive values masked
   */
  Map<String, Object> redact(String endpoint, Map<String, Object> row);
}
