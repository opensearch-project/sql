/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.expression.window.patterns;

import static org.opensearch.sql.utils.ExpressionUtils.format;

import java.util.List;
import java.util.Locale;
import lombok.EqualsAndHashCode;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.parse.PatternsExpression;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.expression.window.WindowFunctionExpression;
import org.opensearch.sql.expression.window.frame.CurrentRowWindowFrame;
import org.opensearch.sql.expression.window.frame.WindowFrame;
import org.opensearch.sql.utils.FunctionUtils;

@EqualsAndHashCode(callSuper = true)
public class StreamPatternWindowFunction extends FunctionExpression
    implements WindowFunctionExpression {

  private final PatternsExpression patternsExpression;

  public StreamPatternWindowFunction(List<Expression> arguments) {
    super(BuiltinFunctionName.SIMPLE_PATTERN.getName(), arguments);
    String pattern =
        FunctionUtils.getNamedArgumentValue(getArguments(), "pattern")
            .map(ExprValue::stringValue)
            .orElse("");
    this.patternsExpression =
        new PatternsExpression(
            getArguments().get(0),
            new LiteralExpression(new ExprStringValue(pattern)),
            new LiteralExpression(new ExprStringValue("")));
  }

  @Override
  public WindowFrame createWindowFrame(WindowDefinition definition) {
    return new CurrentRowWindowFrame(definition);
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    CurrentRowWindowFrame frame = (CurrentRowWindowFrame) valueEnv;
    ExprValue sourceFieldValue =
        patternsExpression.getSourceField().valueOf(frame.current().bindingTuples());
    return patternsExpression.parseValue(sourceFieldValue);
  }

  @Override
  public ExprType type() {
    return ExprCoreType.STRING;
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "%s(%s)", getFunctionName(), format(getArguments()));
  }
}
