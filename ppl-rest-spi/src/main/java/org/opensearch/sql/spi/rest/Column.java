/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

/**
 * One column of a {@code rest} endpoint's fixed output schema: a name and a {@link ColumnType}.
 * Column order is output-field order.
 */
public record Column(String name, ColumnType type) {

  public static Column of(String name, ColumnType type) {
    return new Column(name, type);
  }
}
