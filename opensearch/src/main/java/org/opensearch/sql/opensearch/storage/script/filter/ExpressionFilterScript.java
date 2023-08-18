/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
 * Expression script executor that executes the expression on each document and determine if the
 * document is supposed to be filtered out or not.
 */
@EqualsAndHashCode(callSuper = false)
class ExpressionFilterScript extends FilterScript {

  /** Expression Script. */
  private final ExpressionScript expressionScript;

  public ExpressionFilterScript(
      Expression expression,
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

  private ExprValue evaluateExpression(
      Expression expression, Environment<Expression, ExprValue> valueEnv) {
    ExprValue result = expression.valueOf(valueEnv);
    if (result.isNull()) {
      return ExprBooleanValue.of(false);
    }

    if (result.type() != ExprCoreType.BOOLEAN) {
      throw new IllegalStateException(
          String.format(
              "Expression has wrong result type instead of boolean: "
                  + "expression [%s], result [%s]",
              expression, result));
    }
    return result;
  }
}
