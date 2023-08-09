/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/** Expression Float Value. */
public class ExprFloatValue extends AbstractExprNumberValue {

  public ExprFloatValue(Number value) {
    super(value);
  }

  @Override
  public Object value() {
    return floatValue();
  }

  @Override
  public ExprType type() {
    return ExprCoreType.FLOAT;
  }

  @Override
  public String toString() {
    return floatValue().toString();
  }

  @Override
  public int compare(ExprValue other) {
    return Float.compare(floatValue(), other.floatValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return floatValue().equals(other.floatValue());
  }
}
