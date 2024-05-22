/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.model;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DataSourceType {
  public static DataSourceType PROMETHEUS = new DataSourceType("PROMETHEUS");
  public static DataSourceType OPENSEARCH = new DataSourceType("OPENSEARCH");
  public static DataSourceType SPARK = new DataSourceType("SPARK");
  public static DataSourceType S3GLUE = new DataSourceType("S3GLUE");

  private static DataSourceType[] KNOWN_VALUES = {PROMETHEUS, OPENSEARCH, SPARK, S3GLUE};

  private final String name;

  public String name() {
    return name;
  }

  public static DataSourceType fromString(String name) {
    for (DataSourceType dataSourceType : KNOWN_VALUES) {
      if (dataSourceType.name.equalsIgnoreCase(name)) {
        return dataSourceType;
      }
    }
    throw new IllegalArgumentException("No DataSourceType with name " + name + " found");
  }

  @Override
  public String toString() {
    return name;
  }
}
