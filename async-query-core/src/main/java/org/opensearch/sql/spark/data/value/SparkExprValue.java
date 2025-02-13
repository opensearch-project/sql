/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.data.value;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.AbstractExprValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.spark.data.type.SparkDataType;

/** SparkExprValue hold spark query response value. */
@RequiredArgsConstructor
public class SparkExprValue extends AbstractExprValue {

  private final SparkDataType type;
  private final Object value;

  @Override
  public Object value() {
    return value;
  }

  @Override
  public ExprType type() {
    return type;
  }

  @Override
  public int compare(ExprValue other) {
    throw new UnsupportedOperationException("SparkExprValue is not comparable");
  }

  @Override
  public boolean equal(ExprValue other) {
    return value.equals(other.value());
  }
}
