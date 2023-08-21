/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.scope;

/** Namespace of symbol to avoid naming conflict */
public enum Namespace {
  FIELD_NAME("Field"),
  FUNCTION_NAME("Function"),
  OPERATOR_NAME("Operator");

  private final String name;

  Namespace(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return name;
  }
}
