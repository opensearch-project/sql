/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import java.util.Objects;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/** Expression Null Value. */
public class ExprNullValue extends AbstractExprValue {
  private static final ExprNullValue instance = new ExprNullValue();

  private ExprNullValue() {}

  @Override
  public int hashCode() {
    return Objects.hashCode("NULL");
  }

  @Override
  public String toString() {
    return "NULL";
  }

  public static ExprNullValue of() {
    return instance;
  }

  @Override
  public Object value() {
    return null;
  }

  @Override
  public ExprType type() {
    return ExprCoreType.UNDEFINED;
  }

  @Override
  public boolean isNull() {
    return true;
  }

  @Override
  public int compare(ExprValue other) {
    throw new IllegalStateException(
        String.format("[BUG] Unreachable, Comparing with NULL is undefined"));
  }

  /**
   * NULL value is equal to NULL value. Notes, this function should only used for Java Object
   * Compare.
   */
  @Override
  public boolean equal(ExprValue other) {
    return other.isNull();
  }
}
