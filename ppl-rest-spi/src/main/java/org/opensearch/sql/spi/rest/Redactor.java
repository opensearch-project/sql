/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

import java.util.Map;

/**
 * The single central redaction hook for the {@code rest} command. It receives one raw
 * response row (column name to value) for a given endpoint and returns the row to emit, with any
 * sensitive values masked in one coordinated pass.
 *
 * <p>This is deliberately NOT a {@code loadExtensions} SPI and NOT a per-field classification:
 * masking has one central owner rather than an open registration surface. The default is {@link
 * #NONE} (a no-op). Because one implementation owns the whole row, it decides its own masking order
 * and never has independent maskers interfering on the same value. The endpoint name is passed as a
 * scope so the implementation can vary masking per endpoint. Applied once per row at the {@code
 * rest} row-shaping choke point, before values are coerced to the fixed schema types.
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
