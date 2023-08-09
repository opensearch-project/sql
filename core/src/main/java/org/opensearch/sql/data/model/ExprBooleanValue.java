/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import com.google.common.base.Objects;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/** Expression Boolean Value. */
public class ExprBooleanValue extends AbstractExprValue {
  private static final ExprBooleanValue TRUE = new ExprBooleanValue(true);
  private static final ExprBooleanValue FALSE = new ExprBooleanValue(false);

  private final Boolean value;

  private ExprBooleanValue(Boolean value) {
    this.value = value;
  }

  public static ExprBooleanValue of(Boolean value) {
    return value ? TRUE : FALSE;
  }

  @Override
  public Object value() {
    return value;
  }

  @Override
  public ExprType type() {
    return ExprCoreType.BOOLEAN;
  }

  @Override
  public Boolean booleanValue() {
    return value;
  }

  @Override
  public String toString() {
    return value.toString();
  }

  @Override
  public int compare(ExprValue other) {
    return Boolean.compare(value, other.booleanValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return value.equals(other.booleanValue());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }
}
