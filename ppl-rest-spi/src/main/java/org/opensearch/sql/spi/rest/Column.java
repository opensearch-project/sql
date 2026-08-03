/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

/**
 * One column of a {@code rest} endpoint's output schema, identified by name. Column order is
 * output-field order. Every column is surfaced as a string; a query extracts and casts fields as
 * needed (for example with {@code spath} or {@code json_extract}).
 */
public record Column(String name) {

  public static Column of(String name) {
    return new Column(name);
  }
}
