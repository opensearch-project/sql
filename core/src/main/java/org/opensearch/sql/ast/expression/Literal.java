/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.utils.DecimalUtils;

/**
 * Expression node of literal type Params include literal value (@value) and literal data type
 * (@type) which can be selected from {@link DataType}.
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class Literal extends UnresolvedExpression {

  private final Object value;
  private final DataType type;

  public Literal(Object value, DataType dataType) {
    if (dataType == DataType.DECIMAL && value instanceof Double) {
      // For backward compatibility, we accept decimal literal by Literal(double, DataType.DECIMAL)
      // The double value will be converted by DecimalUtils.safeBigDecimal((Double) value),
      // some double values such as 0.0001 will be converted to string "1.0E-4" and finally
      // generate decimal 0.00010. So here we parse a decimal text to Double then convert it
      // to BigDecimal as well.
      // In v2, a decimal literal will be converted back to double in resolving expression
      // via ExprDoubleValue.
      // In v3, a decimal literal will be kept in Calcite RexNode and converted back to double
      // in runtime.
      this.value = DecimalUtils.safeBigDecimal((Double) value);
    } else {
      this.value = value;
    }
    this.type = dataType;
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return ImmutableList.of();
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitLiteral(this, context);
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }

  public static Literal TRUE = new Literal(true, DataType.BOOLEAN);
  public static Literal FALSE = new Literal(false, DataType.BOOLEAN);
  public static Literal ZERO = new Literal(0, DataType.INTEGER);
  public static Literal ONE = new Literal(1, DataType.INTEGER);
}
