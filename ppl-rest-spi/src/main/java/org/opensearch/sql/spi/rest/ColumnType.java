/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

/**
 * The output type of a {@link Column} in a {@code rest} endpoint schema.
 *
 * <p>A deliberately small, dependency-free enum so the SPI does not leak the sql engine's {@code
 * ExprType} across the module/classloader boundary. The sql side maps each value onto its own type
 * system when it adapts a {@link RestEndpointDefinition} into an internal endpoint.
 */
public enum ColumnType {
  STRING,
  INTEGER,
  LONG,
  DOUBLE,
  BOOLEAN
}
