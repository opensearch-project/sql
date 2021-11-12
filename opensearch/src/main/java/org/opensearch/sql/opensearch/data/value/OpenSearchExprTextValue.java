/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT;

import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.type.ExprType;

/**
 * Expression Text Value, it is a extension of the ExprValue by Elasticsearch.
 */
public class OpenSearchExprTextValue extends ExprStringValue {
  public OpenSearchExprTextValue(String value) {
    super(value);
  }

  @Override
  public ExprType type() {
    return OPENSEARCH_TEXT;
  }
}
