/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DataSourceType {
  public static DataSourceType PROMETHEUS = new DataSourceType("PROMETHEUS");
  public static DataSourceType OPENSEARCH = new DataSourceType("OPENSEARCH");
  public static DataSourceType SPARK = new DataSourceType("SPARK");
  public static DataSourceType S3GLUE = new DataSourceType("S3GLUE");
  public static DataSourceType SECURITY_LAKE = new DataSourceType("SECURITY_LAKE");

  // Map from uppercase DataSourceType name to DataSourceType object
  private static Map<String, DataSourceType> knownValues = new HashMap<>();

  static {
    register(PROMETHEUS, OPENSEARCH, SPARK, S3GLUE, SECURITY_LAKE);
  }

  private final String name;

  public String name() {
    return name;
  }

  @Override
  public String toString() {
    return name;
  }

  /** Register DataSourceType to be used in fromString method */
  public static void register(DataSourceType... dataSourceTypes) {
    for (DataSourceType type : dataSourceTypes) {
      String upperCaseName = type.name().toUpperCase();
      if (!knownValues.containsKey(upperCaseName)) {
        knownValues.put(type.name().toUpperCase(), type);
      } else {
        throw new IllegalArgumentException(
            "DataSourceType with name " + type.name() + " already exists");
      }
    }
  }

  public static DataSourceType fromString(String name) {
    String upperCaseName = name.toUpperCase();
    if (knownValues.containsKey(upperCaseName)) {
      return knownValues.get(upperCaseName);
    } else {
      throw new IllegalArgumentException("No DataSourceType with name " + name + " found");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DataSourceType that = (DataSourceType) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
