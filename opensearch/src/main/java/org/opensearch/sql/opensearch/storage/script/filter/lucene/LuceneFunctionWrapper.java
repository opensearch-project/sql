/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import lombok.Getter;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * {@link FunctionExpression} wrapper for {@link LuceneQuery}. Helps build queries to Lucene.
 */
@Getter
public class LuceneFunctionWrapper {

  private final FunctionExpression func;
  private final ReferenceExpression reference;
  private final Expression literal;

  public LuceneFunctionWrapper(FunctionExpression func) {
    this.func = func;
    ReferenceExpression reference = null;
    Expression literal = null;
    for (var arg : func.getArguments()) {
      if (arg instanceof ReferenceExpression) {
        reference = (ReferenceExpression) arg;
      } else if (isArgASupportedLiteral(arg)) {
        literal = arg;
      }
    }
    this.reference = reference;
    this.literal = literal;
  }

  /**
   * Check if the argument of the function is a supported literal.
   */
  private boolean isArgASupportedLiteral(Expression arg) {
    return arg instanceof LiteralExpression || isLiteralExpressionWrappedByCast(arg);
  }

  /**
   * Check if the argument of the function is a literal expression wrapped by cast function.
   */
  private boolean isLiteralExpressionWrappedByCast(Expression arg) {
    if (arg instanceof FunctionExpression) {
      FunctionExpression expr = (FunctionExpression) arg;
       return LuceneQuery.castMap.containsKey(expr.getFunctionName())
          && isArgASupportedLiteral(expr.getArguments().get(0));
    }
    return false;
  }

  /**
   * Check if the function expression has multiple named argument expressions as the parameters.
   */
  public boolean isMultiParameterQuery() {
    for (Expression expr : func.getArguments()) {
      if (!(expr instanceof NamedArgumentExpression)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get the value of the {@link #literal}.
   * The caller should be aware that {@link #func} may have no literal arguments.
   */
  public ExprValue getLiteralValue() {
    return literal instanceof LiteralExpression
        ? literal.valueOf()
        : LuceneQuery.castMap.get(((FunctionExpression) literal).getFunctionName())
            .apply((LiteralExpression) ((FunctionExpression) literal).getArguments().get(0));
  }
}
