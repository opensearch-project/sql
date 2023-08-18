/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import lombok.EqualsAndHashCode;
import org.opensearch.sql.data.model.AbstractExprValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchBinaryType;

/**
 * OpenSearch BinaryValue.<br>
 * Todo, add this to avoid the unknown value type exception, the implementation will be changed.
 */
@EqualsAndHashCode(callSuper = false)
public class OpenSearchExprBinaryValue extends AbstractExprValue {
  private final String encodedString;

  public OpenSearchExprBinaryValue(String encodedString) {
    this.encodedString = encodedString;
  }

  @Override
  public int compare(ExprValue other) {
    return encodedString.compareTo((String) other.value());
  }

  @Override
  public boolean equal(ExprValue other) {
    return encodedString.equals(other.value());
  }

  @Override
  public Object value() {
    return encodedString;
  }

  @Override
  public ExprType type() {
    return OpenSearchBinaryType.of();
  }
}
