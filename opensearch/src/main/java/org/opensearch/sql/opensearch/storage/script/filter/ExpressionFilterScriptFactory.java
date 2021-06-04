/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
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

package org.opensearch.sql.opensearch.storage.script.filter;

import java.util.Map;
import lombok.EqualsAndHashCode;
import org.opensearch.script.FilterScript;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.sql.expression.Expression;

/**
 * Expression script factory that generates leaf factory.
 */
@EqualsAndHashCode
public class ExpressionFilterScriptFactory implements FilterScript.Factory {

  /**
   * Expression to execute.
   */
  private final Expression expression;

  public ExpressionFilterScriptFactory(Expression expression) {
    this.expression = expression;
  }

  @Override
  public boolean isResultDeterministic() {
    // This implies the results are cacheable
    return true;
  }

  @Override
  public FilterScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
    return new ExpressionFilterScriptLeafFactory(expression, params, lookup);
  }

}
