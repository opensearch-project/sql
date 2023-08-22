/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/** Expression Short Value. */
public class ExprShortValue extends AbstractExprNumberValue {

  public ExprShortValue(Number value) {
    super(value);
  }

  @Override
  public Object value() {
    return shortValue();
  }

  @Override
  public ExprType type() {
    return ExprCoreType.SHORT;
  }

  @Override
  public String toString() {
    return shortValue().toString();
  }

  @Override
  public int compare(ExprValue other) {
    return Short.compare(shortValue(), other.shortValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return shortValue().equals(other.shortValue());
  }
}
