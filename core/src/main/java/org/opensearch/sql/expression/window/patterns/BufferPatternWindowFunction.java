/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window.patterns;

import static org.opensearch.sql.utils.ExpressionUtils.format;

import java.util.List;
import java.util.Locale;
import lombok.EqualsAndHashCode;
import org.opensearch.sql.common.patterns.BrainLogParser;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.expression.window.WindowFunctionExpression;
import org.opensearch.sql.expression.window.frame.BufferPatternRowsWindowFrame;
import org.opensearch.sql.expression.window.frame.WindowFrame;

@EqualsAndHashCode(callSuper = true)
public class BufferPatternWindowFunction extends FunctionExpression
    implements WindowFunctionExpression {

  public BufferPatternWindowFunction(List<Expression> arguments) {
    super(BuiltinFunctionName.BRAIN.getName(), arguments);
  }

  @Override
  public WindowFrame createWindowFrame(WindowDefinition definition) {
    int variableCountThreshold =
        getArguments().stream()
            .filter(
                expression ->
                    expression instanceof NamedArgumentExpression
                        && ((NamedArgumentExpression) expression)
                            .getArgName()
                            .equalsIgnoreCase("variable_count_threshold"))
            .map(
                expression ->
                    ((NamedArgumentExpression) expression).getValue().valueOf().integerValue())
            .findFirst()
            .orElse(5);
    float thresholdPercentage =
        getArguments().stream()
            .filter(
                expression ->
                    expression instanceof NamedArgumentExpression
                        && ((NamedArgumentExpression) expression)
                            .getArgName()
                            .equalsIgnoreCase("frequency_threshold_percentage"))
            .map(
                expression ->
                    ((NamedArgumentExpression) expression).getValue().valueOf().floatValue())
            .findFirst()
            .orElse(0.3f);
    return new BufferPatternRowsWindowFrame(
        definition,
        new BrainLogParser(variableCountThreshold, thresholdPercentage),
        getArguments().get(0)); // actually only first argument is meaningful
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    BufferPatternRowsWindowFrame frame = (BufferPatternRowsWindowFrame) valueEnv;
    List<String> preprocessedMessage = frame.currentPreprocessedMessage();
    frame.next();
    List<String> logPattern = frame.getLogParser().parseLogPattern(preprocessedMessage);
    return new ExprStringValue(String.join(" ", logPattern));
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
