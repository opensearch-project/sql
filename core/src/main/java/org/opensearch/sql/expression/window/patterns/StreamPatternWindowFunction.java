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
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.parse.PatternsExpression;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.expression.window.WindowFunctionExpression;
import org.opensearch.sql.expression.window.frame.StreamPatternRowWindowFrame;
import org.opensearch.sql.expression.window.frame.WindowFrame;

@EqualsAndHashCode(callSuper = true)
public class StreamPatternWindowFunction extends FunctionExpression
    implements WindowFunctionExpression {

  public StreamPatternWindowFunction(List<Expression> arguments) {
    super(BuiltinFunctionName.SIMPLE_PATTERN.getName(), arguments);
  }

  @Override
  public WindowFrame createWindowFrame(WindowDefinition definition) {
    String pattern =
        getArguments().stream()
            .filter(
                expression ->
                    expression instanceof NamedArgumentExpression
                        && ((NamedArgumentExpression) expression)
                            .getArgName()
                            .equalsIgnoreCase("pattern"))
            .map(
                expression ->
                    ((NamedArgumentExpression) expression).getValue().valueOf().stringValue())
            .findFirst()
            .orElse("");
    return new StreamPatternRowWindowFrame(
        definition,
        new PatternsExpression(
            getArguments().get(0),
            new LiteralExpression(new ExprStringValue(pattern)),
            new LiteralExpression(new ExprStringValue(""))));
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    StreamPatternRowWindowFrame frame = (StreamPatternRowWindowFrame) valueEnv;
    ExprValue sourceFieldValue =
        frame.getPatternsExpression().getSourceField().valueOf(frame.current().bindingTuples());
    return frame.getPatternsExpression().parseValue(sourceFieldValue);
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
