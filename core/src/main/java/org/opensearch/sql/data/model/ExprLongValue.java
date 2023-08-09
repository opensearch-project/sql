/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/** Expression Long Value. */
public class ExprLongValue extends AbstractExprNumberValue {

  public ExprLongValue(Number value) {
    super(value);
  }

  @Override
  public Object value() {
    return longValue();
  }

  @Override
  public ExprType type() {
    return ExprCoreType.LONG;
  }

  @Override
  public String toString() {
    return longValue().toString();
  }

  @Override
  public int compare(ExprValue other) {
    return Long.compare(longValue(), other.longValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return longValue().equals(other.longValue());
  }
}
