/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD;

import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.type.ExprType;

/**
 * Expression Text Keyword Value, it is an extension of the ExprValue by Elasticsearch.
 * This mostly represents a multi-field in OpenSearch which has a text field and a
 * keyword field inside to preserve the original text.
 */
public class OpenSearchExprTextKeywordValue extends ExprStringValue {

  public OpenSearchExprTextKeywordValue(String value) {
    super(value);
  }

  @Override
  public ExprType type() {
    return OPENSEARCH_TEXT_KEYWORD;
  }

}
