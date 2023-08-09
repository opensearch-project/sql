/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import java.util.Objects;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/** Expression Missing Value. */
public class ExprMissingValue extends AbstractExprValue {
  private static final ExprMissingValue instance = new ExprMissingValue();

  private ExprMissingValue() {}

  public static ExprMissingValue of() {
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
  public boolean isMissing() {
    return true;
  }

  @Override
  public int compare(ExprValue other) {
    throw new IllegalStateException(
        String.format("[BUG] Unreachable, Comparing with MISSING is " + "undefined"));
  }

  /**
   * Missing value is equal to Missing value. Notes, this function should only used for Java Object
   * Compare.
   */
  @Override
  public boolean equal(ExprValue other) {
    return other.isMissing();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode("MISSING");
  }

  @Override
  public String toString() {
    return "MISSING";
  }
}
