/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.model;

/** The definition of the Expression Value. */
public interface ExprValue {
  default Object value() {
    throw new IllegalStateException("invalid value operation on " + kind());
  }

  default ExprValueKind kind() {
    throw new IllegalStateException("invalid kind operation");
  }

  enum ExprValueKind {
    TUPLE_VALUE,
    COLLECTION_VALUE,
    MISSING_VALUE,

    BOOLEAN_VALUE,
    INTEGER_VALUE,
    DOUBLE_VALUE,
    LONG_VALUE,
    FLOAT_VALUE,
    STRING_VALUE
  }
}
