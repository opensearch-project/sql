package org.opensearch.sql.analysis;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.env.Environment;

@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class HighlightExpression implements Expression {
  String highlightField;

  public HighlightExpression(String field) {
    highlightField = field;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    throw new SemanticCheckException("valeOf highlight is not supported");
  }

  @Override
  public ExprType type() {
    return ExprCoreType.STRING;
  }

  @Override
  public <T, C> T accept(org.opensearch.sql.expression.ExpressionNodeVisitor<T, C> visitor,
                         C context) {
    return visitor.visitHighlight(this, context);
  }
}
