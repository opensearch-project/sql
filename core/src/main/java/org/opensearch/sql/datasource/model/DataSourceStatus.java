/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.model;

/** Enum for capturing the current datasource status. */
public enum DataSourceStatus {
  ACTIVE("active"),
  DISABLED("disabled");

  private final String text;

  DataSourceStatus(String text) {
    this.text = text;
  }

  public String getText() {
    return this.text;
  }

  /**
   * Get DataSourceStatus from text.
   *
   * @param text text.
   * @return DataSourceStatus {@link DataSourceStatus}.
   */
  public static DataSourceStatus fromString(String text) {
    for (DataSourceStatus dataSourceStatus : DataSourceStatus.values()) {
      if (dataSourceStatus.text.equalsIgnoreCase(text)) {
        return dataSourceStatus;
      }
    }
    throw new IllegalArgumentException("No DataSourceStatus with text " + text + " found");
  }
}
