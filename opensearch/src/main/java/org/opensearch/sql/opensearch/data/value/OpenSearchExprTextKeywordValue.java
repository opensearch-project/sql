/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
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
