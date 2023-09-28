/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.rest.model;

/** Language type accepted in async query apis. */
public enum LangType {
  SQL("sql"),
  PPL("ppl");
  private final String text;

  LangType(String text) {
    this.text = text;
  }

  public String getText() {
    return this.text;
  }

  /**
   * Get LangType from text.
   *
   * @param text text.
   * @return LangType {@link LangType}.
   */
  public static LangType fromString(String text) {
    for (LangType langType : LangType.values()) {
      if (langType.text.equalsIgnoreCase(text)) {
        return langType;
      }
    }
    throw new IllegalArgumentException("No LangType with text " + text + " found");
  }
}
