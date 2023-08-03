/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/** Expression Integer Value. */
public class ExprIntegerValue extends AbstractExprNumberValue {

  public ExprIntegerValue(Number value) {
    super(value);
  }

  @Override
  public Object value() {
    return integerValue();
  }

  @Override
  public ExprType type() {
    return ExprCoreType.INTEGER;
  }

  @Override
  public String toString() {
    return integerValue().toString();
  }

  @Override
  public int compare(ExprValue other) {
    return Integer.compare(integerValue(), other.integerValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return integerValue().equals(other.integerValue());
  }
}
