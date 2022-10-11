/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.span;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.env.Environment;

@RequiredArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
public class SpanExpression implements Expression {
  private final Expression field;
  private final Expression value;
  private final SpanUnit unit;

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    return value.valueOf(valueEnv);
  }

  public long windowSize() {
    long unitValue = convertUnitToMs();
    return value.valueOf(null).longValue() * unitValue;
  }

  private long convertUnitToMs() {
    switch (unit) {
      case S:
      case SECOND:
        return 1000;
      case m:
      case MINUTE:
        return 60 * 1000;
      case H:
      case HOUR: return 60 * 60 * 1000;
      case D:
      case DAY:
        return 24 * 60 * 60 * 1000;
      default:
        throw new UnsupportedOperationException("not implement");
    }
  }

  /**
   * Return type follows the following table.
   *  FIELD         VALUE     RETURN_TYPE
   *  int/long      integer   int/long (field type)
   *  int/long      double    double
   *  float/double  integer   float/double (field type)
   *  float/double  double    float/double (field type)
   *  other         any       field type
   */
  @Override
  public ExprType type() {
    if (field.type().isCompatible(value.type())) {
      return field.type();
    } else if (value.type().isCompatible(field.type())) {
      return value.type();
    } else {
      return field.type();
    }
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }
}
