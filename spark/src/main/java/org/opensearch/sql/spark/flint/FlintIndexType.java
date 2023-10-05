/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Enum for FlintIndex Type. */
public enum FlintIndexType {
  SKIPPING("skipping_index"),
  COVERING("covering_index"),
  MATERIALIZED("materialized_view");

  private final String name;
  private static final Map<String, FlintIndexType> ENUM_MAP;

  FlintIndexType(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  static {
    Map<String, FlintIndexType> map = new HashMap<>();
    for (FlintIndexType instance : FlintIndexType.values()) {
      map.put(instance.getName().toLowerCase(), instance);
    }
    ENUM_MAP = Collections.unmodifiableMap(map);
  }

  public static FlintIndexType get(String name) {
    return ENUM_MAP.get(name.toLowerCase());
  }
}
