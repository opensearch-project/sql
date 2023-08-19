package org.opensearch.sql.opensearch.ast.expression;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.analysis.OpenSearchExpressionNodeVisitor;

import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;

public class OpenSearchFunctionExpression extends FunctionExpression {
  private final FunctionName functionName;
  private final List<Expression> arguments;

  @Getter
  @Setter
  private boolean isScoreTracked;

  /**
   * Required argument constructor.
   * @param functionName name of the function
   * @param arguments a list of expressions
   */
  public OpenSearchFunctionExpression(FunctionName functionName, List<Expression> arguments) {
    super(functionName, arguments);
    this.functionName = functionName;
    this.arguments = arguments;
    this.isScoreTracked = false;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    throw new UnsupportedOperationException(String.format(
        "OpenSearch defined function [%s] is only supported in WHERE and HAVING clause.",
        functionName));
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    if (visitor instanceof OpenSearchExpressionNodeVisitor) {
      return accept((OpenSearchExpressionNodeVisitor<T, C>) visitor, context);
    }
    return visitor.visitFunction(this, context);
  }

  public <T, C> T accept(OpenSearchExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitOpenSearchFunction(this, context);
  }

  @Override
  public ExprType type() {
    return BOOLEAN;
  }

  @Override
  public String toString() {
    List<String> args = arguments.stream()
        .map(arg -> String.format("%s=%s", ((NamedArgumentExpression) arg)
            .getArgName(), ((NamedArgumentExpression) arg).getValue().toString()))
        .collect(Collectors.toList());
    return String.format("%s(%s)", functionName, String.join(", ", args));
  }
}
