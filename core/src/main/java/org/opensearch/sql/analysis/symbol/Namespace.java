/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis.symbol;

/** Namespace of symbol to avoid naming conflict. */
public enum Namespace {
  INDEX_NAME("Index"),
  FIELD_NAME("Field"),
  FUNCTION_NAME("Function");

  private final String name;

  Namespace(String name) {
    this.name = name;
  }
}
