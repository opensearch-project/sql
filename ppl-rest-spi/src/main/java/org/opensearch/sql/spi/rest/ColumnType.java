/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

/**
 * The output type of a {@link Column}. A small, dependency-free enum so the SPI does not leak the
 * sql engine's {@code ExprType} across the module boundary.
 */
public enum ColumnType {
  STRING,
  INTEGER,
  LONG,
  DOUBLE,
  BOOLEAN
}
