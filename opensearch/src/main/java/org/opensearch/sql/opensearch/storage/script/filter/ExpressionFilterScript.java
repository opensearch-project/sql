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

package org.opensearch.sql.opensearch.storage.script.filter;

import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.script.FilterScript;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.opensearch.storage.script.core.ExpressionScript;

/**
 * Expression script executor that executes the expression on each document
 * and determine if the document is supposed to be filtered out or not.
 */
@EqualsAndHashCode(callSuper = false)
class ExpressionFilterScript extends FilterScript {

  /**
   * Expression Script.
   */
  private final ExpressionScript expressionScript;

  public ExpressionFilterScript(Expression expression,
                                SearchLookup lookup,
                                LeafReaderContext context,
                                Map<String, Object> params) {
    super(params, lookup, context);
    this.expressionScript = new ExpressionScript(expression);
  }

  @Override
  public boolean execute() {
    return expressionScript.execute(this::getDoc, this::evaluateExpression).booleanValue();
  }

  private ExprValue evaluateExpression(Expression expression,
                                       Environment<Expression, ExprValue> valueEnv) {
    ExprValue result = expression.valueOf(valueEnv);
    if (result.isNull()) {
      return ExprBooleanValue.of(false);
    }

    if (result.type() != ExprCoreType.BOOLEAN) {
      throw new IllegalStateException(String.format(
          "Expression has wrong result type instead of boolean: "
              + "expression [%s], result [%s]", expression, result));
    }
    return result;
  }

}
