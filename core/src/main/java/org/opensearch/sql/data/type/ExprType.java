/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import java.util.List;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;

/**
 * The Type of {@link Expression} and {@link ExprValue}.
 */
public interface ExprType {
  /**
   * Is compatible with other types.
   */
  default boolean isCompatible(ExprType other) {
    other = other.getExprType();
    if (getExprType().equals(other)) {
      return true;
    } else {
      if (other.equals(UNKNOWN)) {
        return false;
      }
      for (ExprType parentTypeOfOther : other.getParent()) {
        if (isCompatible(parentTypeOfOther)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Should cast this type to other type or not. By default, cast is always required
   * if the given type is different from this type.
   * @param other other data type
   * @return      true if cast is required, otherwise false
   */
  default boolean shouldCast(ExprType other) {
    return !this.equals(other);
  }

  /**
   * Get the parent type.
   */
  default List<ExprType> getParent() {
    return List.of(UNKNOWN);
  }

  /**
   * Get the type name.
   */
  String typeName();

  /**
   * Get the legacy type name for old engine.
   */
  default String legacyTypeName() {
    return typeName();
  }

  /**
   * Perform field name conversion if needed before inserting it into a search query.
   */
  default String convertFieldForSearchQuery(String fieldName) {
    return fieldName;
  }

  /**
   * Perform value conversion if needed before inserting it into a search query.
   */
  default Object convertValueForSearchQuery(ExprValue value) {
    return value.value();
  }

  /**
   * Get a simplified type {@link ExprCoreType} if possible. Used in {@link #isCompatible}.
   */
  default ExprType getExprType() {
    return this;
  }
}
