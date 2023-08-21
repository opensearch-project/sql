/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.cursor;

import java.util.HashMap;
import java.util.Map;

/**
 * Different types queries for which cursor is supported. The result execution, and cursor
 * generation/parsing will depend on the cursor type. NullCursor is the placeholder implementation
 * in case of non-cursor query.
 */
public enum CursorType {
  NULL(null),
  DEFAULT("d"),
  AGGREGATION("a"),
  JOIN("j");

  public String id;

  CursorType(String id) {
    this.id = id;
  }

  public String getId() {
    return this.id;
  }

  public static final Map<String, CursorType> LOOKUP = new HashMap<>();

  static {
    for (CursorType type : CursorType.values()) {
      LOOKUP.put(type.getId(), type);
    }
  }

  public static CursorType getById(String id) {
    return LOOKUP.getOrDefault(id, NULL);
  }
}
