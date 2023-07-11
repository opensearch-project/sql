/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.model;

public enum DataSourceType {
  PROMETHEUS("prometheus"),
  OPENSEARCH("opensearch"),
  SPARK("spark");
  private String text;

  DataSourceType(String text) {
    this.text = text;
  }

  public String getText() {
    return this.text;
  }

  /**
   * Get DataSourceType from text.
   *
   * @param text text.
   * @return DataSourceType {@link DataSourceType}.
   */
  public static DataSourceType fromString(String text) {
    for (DataSourceType dataSourceType : DataSourceType.values()) {
      if (dataSourceType.text.equalsIgnoreCase(text)) {
        return dataSourceType;
      }
    }
    throw new IllegalArgumentException("No DataSourceType with text " + text + " found");
  }
}
