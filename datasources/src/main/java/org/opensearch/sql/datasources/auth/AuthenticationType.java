/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.datasources.auth;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum AuthenticationType {

  BASICAUTH("basicauth"), AWSSIGV4AUTH("awssigv4");

  private String name;

  private static final Map<String, AuthenticationType> ENUM_MAP;

  AuthenticationType(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  static {
    Map<String, AuthenticationType> map = new HashMap<>();
    for (AuthenticationType instance : AuthenticationType.values()) {
      map.put(instance.getName().toLowerCase(), instance);
    }
    ENUM_MAP = Collections.unmodifiableMap(map);
  }

  public static AuthenticationType get(String name) {
    return ENUM_MAP.get(name.toLowerCase());
  }
}
