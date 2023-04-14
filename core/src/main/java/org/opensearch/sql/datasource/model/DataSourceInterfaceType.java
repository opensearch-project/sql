/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.datasource.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum DataSourceInterfaceType {

  API("api"), KEYSTORE("keystore");

  private String name;

  private static final Map<String, DataSourceInterfaceType> ENUM_MAP;

  DataSourceInterfaceType(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  static {
    Map<String, DataSourceInterfaceType> map = new HashMap<>();
    for (DataSourceInterfaceType instance : DataSourceInterfaceType.values()) {
      map.put(instance.getName().toLowerCase(), instance);
    }
    ENUM_MAP = Collections.unmodifiableMap(map);
  }

  public static DataSourceInterfaceType get(String name) {
    return ENUM_MAP.get(name.toLowerCase());
  }

}