/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;

/** The Type of {@link Expression} and {@link ExprValue}. */
public interface ExprType {
  /** Is compatible with other types. */
  default boolean isCompatible(ExprType other) {
    if (this.equals(other)) {
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
   * Should cast this type to other type or not. By default, cast is always required if the given
   * type is different from this type.
   *
   * @param other other data type
   * @return true if cast is required, otherwise false
   */
  default boolean shouldCast(ExprType other) {
    return !this.equals(other);
  }

  /** Get the parent type. */
  default List<ExprType> getParent() {
    return Arrays.asList(UNKNOWN);
  }

  /** Get the type name. */
  String typeName();

  /** Get the legacy type name for old engine. */
  default String legacyTypeName() {
    return typeName();
  }

  /** Get the original path. Types like alias type will set the actual path in field property. */
  default Optional<String> getOriginalPath() {
    return Optional.empty();
  }

  /**
   * Get the original path. Types like alias type should be derived from the type of the original
   * field.
   */
  default ExprType getOriginalExprType() {
    return this;
  }
}
