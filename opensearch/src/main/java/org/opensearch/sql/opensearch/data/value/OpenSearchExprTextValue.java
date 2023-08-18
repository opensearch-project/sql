/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;

/** Expression Text Value, it is a extension of the ExprValue by OpenSearch. */
public class OpenSearchExprTextValue extends ExprStringValue {
  public OpenSearchExprTextValue(String value) {
    super(value);
  }

  @Override
  public ExprType type() {
    return OpenSearchTextType.of();
  }
}
