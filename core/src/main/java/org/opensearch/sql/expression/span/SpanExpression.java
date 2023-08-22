/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.span;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.planner.physical.collector.Rounding;

@Getter
@ToString
@EqualsAndHashCode
public class SpanExpression implements Expression {
  private final Expression field;
  private final Expression value;
  private final SpanUnit unit;

  /** Construct a span expression by field and span interval expression. */
  public SpanExpression(Expression field, Expression value, SpanUnit unit) {
    this.field = field;
    this.value = value;
    this.unit = unit;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    Rounding<?> rounding =
        Rounding.createRounding(this); // TODO: will integrate with WindowAssigner
    return rounding.round(field.valueOf(valueEnv));
  }

  /**
   * Return type follows the following table.
   *
   * <table>
   *   <tr>
   *     <th>FIELD</th>
   *     <th>VALUE</th>
   *     <th>RETURN_TYPE</th>
   *   </tr>
   *   <tr>
   *       <th>int/long</th>
   *       <th>integer</th>
   *       <th>int/long (field type)</th>
   *   </tr>
   *   <tr>
   *       <th>int/long</th>
   *       <th>double</th>
   *       <th>double</th>
   *   </tr>
   *   <tr>
   *       <th>float/double</th>
   *       <th>integer</th>
   *       <th>float/double (field type)</th>
   *   </tr>
   *   <tr>
   *       <th>float/double</th>
   *       <th>double</th>
   *       <th>float/double (field type)</th>
   *   </tr>
   *   <tr>
   *       <th>other</th>
   *       <th>any</th>
   *       <th>field type</th>
   *   </tr>
   *  </table>
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
