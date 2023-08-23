/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.types.base;

import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchIndex.IndexType.NESTED_FIELD;
import static org.opensearch.sql.legacy.utils.StringUtils.toUpper;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;

/** Base type hierarchy based on OpenSearch data type */
public enum OpenSearchDataType implements BaseType {
  TYPE_ERROR,
  UNKNOWN,

  SHORT,
  LONG,
  INTEGER(SHORT, LONG),
  FLOAT(INTEGER),
  DOUBLE(FLOAT),
  NUMBER(DOUBLE),

  KEYWORD,
  TEXT(KEYWORD),
  STRING(TEXT),

  DATE_NANOS,
  DATE(DATE_NANOS, STRING),

  BOOLEAN,

  OBJECT,
  NESTED,
  COMPLEX(OBJECT, NESTED),

  GEO_POINT,

  OPENSEARCH_TYPE(
      NUMBER,
      // STRING, move to under DATE because DATE is compatible
      DATE,
      BOOLEAN,
      COMPLEX,
      GEO_POINT);

  /**
   * Java Enum's valueOf() may thrown "enum constant not found" exception. And Java doesn't provide
   * a contains method. So this static map is necessary for check and efficiency.
   */
  private static final Map<String, OpenSearchDataType> ALL_BASE_TYPES;

  static {
    ImmutableMap.Builder<String, OpenSearchDataType> builder = new ImmutableMap.Builder<>();
    for (OpenSearchDataType type : OpenSearchDataType.values()) {
      builder.put(type.name(), type);
    }
    ALL_BASE_TYPES = builder.build();
  }

  public static OpenSearchDataType typeOf(String str) {
    return ALL_BASE_TYPES.getOrDefault(toUpper(str), UNKNOWN);
  }

  /** Parent of current base type */
  private OpenSearchDataType parent;

  OpenSearchDataType(OpenSearchDataType... compatibleTypes) {
    for (OpenSearchDataType subType : compatibleTypes) {
      subType.parent = this;
    }
  }

  @Override
  public String getName() {
    return name();
  }

  /**
   * For base type, compatibility means this (current type) is ancestor of other in the base type
   * hierarchy.
   */
  @Override
  public boolean isCompatible(Type other) {
    // Skip compatibility check if type is unknown
    if (this == UNKNOWN || other == UNKNOWN) {
      return true;
    }

    if (!(other instanceof OpenSearchDataType)) {
      // Nested data type is compatible with nested index type for type expression use
      if (other instanceof OpenSearchIndex && ((OpenSearchIndex) other).type() == NESTED_FIELD) {
        return isCompatible(NESTED);
      }
      return false;
    }

    // One way compatibility: parent base type is compatible with children
    OpenSearchDataType cur = (OpenSearchDataType) other;
    while (cur != null && cur != this) {
      cur = cur.parent;
    }
    return cur != null;
  }

  @Override
  public String toString() {
    return "OpenSearch Data Type [" + getName() + "]";
  }
}
