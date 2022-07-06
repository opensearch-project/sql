package org.opensearch.sql.analysis;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class HighlightExpression extends FunctionExpression {
  Expression highlightField;

  public HighlightExpression(Expression highlightField) {
    super(BuiltinFunctionName.HIGHLIGHT.getName(), List.of(highlightField));
    this.highlightField = highlightField;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    throw new SemanticCheckException("valeOf highlight is not supported");
  }

  @Override
  public ExprType type() {
    return ExprCoreType.STRING;
  }

  //  @Override
  //  public <T, C> T accept(org.opensearch.sql.expression.ExpressionNodeVisitor<T, C> visitor,
  //                         C context) {
  //    return visitor.visitHighlight(this, context);
  //  }
}
