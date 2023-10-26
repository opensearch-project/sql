/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

import java.util.List;
import java.util.Objects;

/** Expression array Value. */
@RequiredArgsConstructor
public class ExprArrayValue extends AbstractExprValue {
  private final List<ExprValue> value;

  @Override
  public boolean isArray() {
    return true;
  }

  @Override
  public Object value() {
    return value;
  }

  @Override
  public ExprType type() {
    return ExprCoreType.ARRAY;
  }

  @Override
  public String stringValue() {
    return value.stream().map(Object::toString).reduce("", String::concat);
  }

  @Override
  public List<ExprValue> arrayValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.format("\"%s\"", stringValue());
  }

  @Override
  public int compare(ExprValue other) {
    return stringValue().compareTo(other.stringValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return stringValue().equals(other.stringValue());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(stringValue());
  }
}
