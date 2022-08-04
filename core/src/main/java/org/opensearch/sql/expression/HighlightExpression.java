/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import java.util.List;
import lombok.Getter;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/**
 * Highlight Expression.
 */
@Getter
public class HighlightExpression extends FunctionExpression {
  private final Expression highlightField;

  /**
   * HighlightExpression Constructor.
   * @param highlightField : Highlight field for expression.
   */
  public HighlightExpression(Expression highlightField) {
    super(BuiltinFunctionName.HIGHLIGHT.getName(), List.of(highlightField));
    this.highlightField = highlightField;
  }

  /**
   * Return collection value matching highlight field.
   * @param valueEnv : Dataset to parse value from.
   * @return : collection value of highlight fields.
   */
  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    String refName = "_highlight" + "." + StringUtils.unquoteText(getHighlightField().toString());
    return valueEnv.resolve(DSL.ref(refName, ExprCoreType.STRING));
  }

  /**
   * Get type for HighlightExpression.
   * @return : String type.
   */
  @Override
  public ExprType type() {
    return ExprCoreType.ARRAY;
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitHighlight(this, context);
  }
}
